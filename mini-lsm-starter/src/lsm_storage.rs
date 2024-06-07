#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use core::fmt;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

impl fmt::Debug for LsmStorageState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "The current memtable {:?}", self.memtable)?;
        writeln!(f, "Immutable memtables: {:?}", self.imm_memtables)?;
        writeln!(f, "L0 SSTs: {:?}", self.l0_sstables)?;
        writeln!(f, "Levels: {:?}", self.levels)?;
        writeln!(f, "SST objects: {:?}", self.sstables)
    }
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            match compaction_thread.join() {
                std::result::Result::Ok(_) => (),
                Err(e) => eprintln!("compaction thread panicked: {:?}", e),
            }
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            match flush_thread.join() {
                std::result::Result::Ok(_) => (),
                Err(e) => eprintln!("flush thread panicked: {:?}", e),
            }
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            let state_lock = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&state_lock)?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        println!("the path {:?}", path);
        let mut state = LsmStorageState::create(&options);
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let mut next_sst_id = 1;
        let manifest_path = path.join("MANIFEST");
        let manifest = {
            if !manifest_path.exists() {
                if options.enable_wal {
                    state.memtable = Arc::new(MemTable::create_with_wal(
                        state.memtable.id(),
                        Self::path_of_wal_static(path, state.memtable.id()),
                    )?);
                }
                let manifest = Manifest::create(manifest_path)?;
                manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
                manifest
            } else {
                let (manifest, existing_records) = Manifest::recover(manifest_path)?;
                let mut recovered_memtable_ids = BTreeSet::new();
                for record in existing_records {
                    match record {
                        ManifestRecord::Flush(sst_id) => {
                            assert!(
                                recovered_memtable_ids.remove(&sst_id),
                                "memtable {} in manifest does not exist",
                                sst_id
                            );
                            if CompactionController::flush_to_l0(&compaction_controller) {
                                state.l0_sstables.insert(0, sst_id);
                            } else {
                                state.levels.insert(0, (sst_id, vec![sst_id]));
                            }
                            next_sst_id = next_sst_id.max(sst_id);
                        }
                        ManifestRecord::NewMemtable(memtable_id) => {
                            recovered_memtable_ids.insert(memtable_id);
                            next_sst_id = next_sst_id.max(memtable_id);
                        }
                        ManifestRecord::Compaction(task, new_sst_ids) => {
                            let (new_snapshot, _) = compaction_controller.apply_compaction_result(
                                &state,
                                &task,
                                &new_sst_ids,
                            );
                            state = new_snapshot;
                            next_sst_id = next_sst_id
                                .max(new_sst_ids.iter().max().copied().unwrap_or_default());
                        }
                    }
                }
                //  read the l0 sstables from disk
                for sst_id in &state.l0_sstables {
                    let sst = SsTable::open(
                        *sst_id,
                        Some(Arc::clone(&block_cache)),
                        FileObject::open(&Self::path_of_sst_static(path, *sst_id))?,
                    )?;
                    state.sstables.insert(*sst_id, Arc::new(sst));
                }
                //  read the remaining sstables from disk
                for (_, sst_ids) in &state.levels {
                    for sst_id in sst_ids {
                        let sst = SsTable::open(
                            *sst_id,
                            Some(Arc::clone(&block_cache)),
                            FileObject::open(&Self::path_of_sst_static(path, *sst_id))?,
                        )?;
                        state.sstables.insert(*sst_id, Arc::new(sst));
                    }
                }

                //  read the recovered memtable ids
                if options.enable_wal {
                    for memtable_id in recovered_memtable_ids {
                        let wal_path = Self::path_of_wal_static(path, memtable_id);
                        if !wal_path.exists() {
                            eprintln!(
                                "wal file not found for memtable {} while recovering from disk",
                                memtable_id
                            );
                        }
                        let memtable = MemTable::recover_from_wal(memtable_id, wal_path)?;
                        state.imm_memtables.insert(0, Arc::new(memtable));
                    }
                }

                //  create a new memtable for the engine
                next_sst_id += 1;
                let memtable = {
                    if options.enable_wal {
                        Arc::new(MemTable::create_with_wal(
                            next_sst_id,
                            Self::path_of_wal_static(path, next_sst_id),
                        )?)
                    } else {
                        Arc::new(MemTable::create(next_sst_id))
                    }
                };
                state.memtable = memtable;
                next_sst_id += 1;
                manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

                manifest
            }
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn should_include_table(&self, sstable: &Arc<SsTable>, key: &KeyBytes) -> bool {
        if key < sstable.first_key() || key > sstable.last_key() {
            return false;
        }
        if let Some(bloom_filter) = &sstable.bloom {
            if !bloom_filter.may_contain(farmhash::fingerprint32(key.as_key_slice().into_inner())) {
                return false;
            }
        }
        true
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        //  probe the memtables
        let snapshot = {
            let state_guard = self.state.read();
            Arc::clone(&state_guard)
        };
        let mut memtables = Vec::new();
        memtables.push(Arc::clone(&snapshot.memtable));
        memtables.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|memtable| Arc::clone(memtable)),
        );
        for memtable in memtables {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        //  create iterator for l0
        let mut sstable_iterators: Vec<Box<SsTableIterator>> = Vec::new();
        for sstable_id in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables.get(&sstable_id).unwrap();
            if !self.should_include_table(sstable, &Key::from_vec(key.to_vec()).into_key_bytes()) {
                continue;
            }
            let sstable_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sstable),
                KeySlice::from_slice(key),
            )?;
            sstable_iterators.push(Box::new(sstable_iter));
        }
        let l0_iter = MergeIterator::create(sstable_iterators);

        //  create iterators for l1 onwards
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, sst_ids_at_level) in &snapshot.levels {
            let mut tables = Vec::with_capacity(sst_ids_at_level.len());
            for table_id in sst_ids_at_level {
                let table = snapshot
                    .sstables
                    .get(table_id)
                    .ok_or_else(|| anyhow::anyhow!("could not find sstable with id {}", table_id))?
                    .clone();
                if self.should_include_table(&table, &Key::from_vec(key.to_vec()).into_key_bytes())
                {
                    tables.push(table);
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(tables, Key::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }
        let sst_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(l0_iter, sst_iter)?;

        if iter.is_valid() && iter.key() == Key::from_slice(key) && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let memtable_size = {
            let state = self.state.read();
            state.memtable.put(key, value)?;
            state.memtable.approximate_size()
        };
        if memtable_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let state = self.state.read();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let state = self.state.read();
        let result = state.memtable.put(_key, &[]);
        if state.memtable.approximate_size() >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if state.memtable.approximate_size() >= self.options.target_sst_size {
                drop(state);
                self.force_freeze_memtable(&state_lock)?
            }
        }
        result
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let current_memtable = {
            let state_read = self.state.read();
            Arc::clone(&state_read.memtable)
        };

        let memtable_id = self.next_sst_id();
        let new_memtable = {
            if self.options.enable_wal {
                let wal_path = self.path_of_wal(memtable_id);
                Arc::new(MemTable::create_with_wal(memtable_id, wal_path)?)
            } else {
                Arc::new(MemTable::create(memtable_id))
            }
        };

        let mut state_guard = self.state.write();
        let state = Arc::make_mut(&mut state_guard);
        state.imm_memtables.insert(0, current_memtable);
        state.memtable = new_memtable;

        if self.options.enable_wal {
            self.manifest
                .as_ref()
                .map(|manifest| {
                    manifest.add_record(
                        &_state_lock_observer,
                        ManifestRecord::NewMemtable(memtable_id),
                    )
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("no manifest handle found while freezing memtable")
                })??;
        }
        self.sync_dir()?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();
        let oldest_memtable = {
            let state_guard = self.state.read();
            if state_guard.imm_memtables.len() == 0 {
                return Ok(());
            }
            let oldest_memtable = state_guard
                .imm_memtables
                .last()
                .expect("No memtable found")
                .clone();
            oldest_memtable
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        oldest_memtable.flush(&mut sst_builder)?;
        let sst = Arc::new(sst_builder.build(
            oldest_memtable.id(),
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(oldest_memtable.id()),
        )?);

        {
            let mut state_guard = self.state.write();
            let mut snapshot = state_guard.as_ref().clone();
            let oldest_memtable = snapshot.imm_memtables.pop().expect("No memtable found");
            snapshot.l0_sstables.insert(0, oldest_memtable.id());
            snapshot.sstables.insert(oldest_memtable.id(), sst);
            *state_guard = Arc::new(snapshot);
        }

        if let Some(manifest) = self.manifest.as_ref() {
            manifest.add_record(&_state_lock, ManifestRecord::Flush(oldest_memtable.id()))?;
        } else {
            eprintln!("no manifest handle found while flushing immutable memtable to disk");
        }
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        //  create the merge iterator for the memtables here
        let snapshot = {
            let state_guard = self.state.read();
            Arc::clone(&state_guard)
        };
        let mut memtables = Vec::new();
        memtables.push(Arc::clone(&snapshot.memtable));
        memtables.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|memtable| Arc::clone(memtable)),
        );
        let mut memtable_iterators = Vec::new();
        for memtable in memtables {
            //  create a memtable iterator for each memtable
            let iterator = memtable.scan(lower, upper);
            memtable_iterators.push(Box::new(iterator));
        }
        let memtable_merge_iterator = MergeIterator::create(memtable_iterators);

        //  create an iterator for sstables at l0
        let l0_ssts = &*snapshot.l0_sstables;
        let mut l0_iters = Vec::new();
        for sstable_id in l0_ssts {
            let sstable = snapshot.sstables.get(&sstable_id).unwrap();
            //  need to skip building any iterators that cannot contain the key
            if !self.range_overlap(lower, upper, sstable.first_key(), sstable.last_key()) {
                continue;
            }

            let sstable_iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    Arc::clone(sstable),
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iterator = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sstable),
                        KeySlice::from_slice(key),
                    )?;
                    if iterator.is_valid() && iterator.key().raw_ref() == key {
                        iterator.next()?;
                    }
                    iterator
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(Arc::clone(sstable))?,
            };
            l0_iters.push(Box::new(sstable_iter));
        }
        let l0_iter = MergeIterator::create(l0_iters);

        //  create iterators for l1 onwards
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, sst_ids_at_level) in &snapshot.levels {
            let tables = sst_ids_at_level
                .iter()
                .map(|sst_id_at_level| {
                    snapshot
                        .sstables
                        .get(sst_id_at_level)
                        .ok_or_else(|| {
                            anyhow::anyhow!("could not find sstable with id {}", sst_id_at_level)
                        })
                        .cloned()
                })
                .collect::<Result<Vec<_>, _>>()?;
            let level_iter = match lower {
                Bound::Included(key) => {
                    SstConcatIterator::create_and_seek_to_key(tables, Key::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SstConcatIterator::create_and_seek_to_key(tables, Key::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(tables)?,
            };
            level_iters.push(Box::new(level_iter));
        }
        let sst_iter = MergeIterator::create(level_iters);

        let iter = TwoMergeIterator::create(memtable_merge_iterator, l0_iter)?;
        let iter = TwoMergeIterator::create(iter, sst_iter)?;
        let lsm_iterator = LsmIterator::new(iter, map_bound(upper))?;
        Ok(FusedIterator::new(lsm_iterator))
    }

    fn range_overlap(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        table_first: &KeyBytes,
        table_last: &KeyBytes,
    ) -> bool {
        match lower {
            Bound::Excluded(key) if key >= table_last.as_key_slice().into_inner() => {
                return false;
            }
            Bound::Included(key) if key > table_last.as_key_slice().into_inner() => {
                return false;
            }
            _ => {}
        }
        match upper {
            Bound::Excluded(key) if key <= table_first.as_key_slice().into_inner() => {
                return false;
            }
            Bound::Included(key) if key < table_first.as_key_slice().into_inner() => {
                return false;
            }
            _ => {}
        }
        true
    }
}
