#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let mut sstables = Vec::new();
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                //  fetch all the required sstables first
                let mut tables = Vec::new();
                for table_id in l0_sstables {
                    let table = snapshot
                        .sstables
                        .get(table_id)
                        .ok_or_else(|| anyhow::anyhow!("sstable {} not found", table_id))?;
                    tables.push(Arc::clone(table));
                }
                for table_id in l1_sstables {
                    let table = snapshot
                        .sstables
                        .get(table_id)
                        .ok_or_else(|| anyhow::anyhow!("sstable {} not found", table_id))?;
                    tables.push(Arc::clone(table));
                }

                //  build an sstable iterator for each sstable and then build a merge iterator over all the individual iterators
                let iterators = tables
                    .into_iter()
                    .map(|table| SsTableIterator::create_and_seek_to_first(table).map(Box::new))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut merge_iterator = MergeIterator::create(iterators);

                //  iterate over merge iterator and create new SSTables
                let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
                let mut is_built = false;
                while merge_iterator.is_valid() {
                    is_built = false;
                    let key = merge_iterator.key();
                    let value = merge_iterator.value();
                    if value.is_empty() {
                        merge_iterator.next()?;
                        continue;
                    }
                    sstable_builder.add(key, value);

                    //  set this sstable and create a new builder
                    if sstable_builder.estimated_size() >= self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        let sstable = sstable_builder.build(
                            sst_id,
                            Some(Arc::clone(&self.block_cache)),
                            self.path_of_sst(sst_id),
                        )?;
                        is_built = true;
                        sstable_builder = SsTableBuilder::new(self.options.block_size);
                        sstables.push(Arc::new(sstable));
                    }

                    merge_iterator.next()?;
                }

                if !is_built {
                    let sst_id = self.next_sst_id();
                    let sstable = sstable_builder.build(
                        sst_id,
                        Some(Arc::clone(&self.block_cache)),
                        self.path_of_sst(sst_id),
                    )?;
                    sstables.push(Arc::new(sstable));
                }
                return Ok(sstables);
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                is_lower_level_bottom_level: _,
            }) => {
                match upper_level {
                    //  does not include L0
                    Some(_) => {
                        println!("creating a simple compaction task without L0");
                        let upper_sstables: Vec<_> = upper_level_sst_ids
                            .iter()
                            .map(|sst_id| {
                                snapshot
                                    .sstables
                                    .get(sst_id)
                                    .ok_or_else(|| anyhow::anyhow!("sstable {} not found", sst_id))
                                    .cloned()
                            })
                            .collect::<Result<_, _>>()?;
                        let upper_sstables_iter =
                            SstConcatIterator::create_and_seek_to_first(upper_sstables)?;
                        println!("created upper iterator");
                        let lower_sstables: Vec<_> = lower_level_sst_ids
                            .iter()
                            .map(|sst_id| {
                                snapshot
                                    .sstables
                                    .get(sst_id)
                                    .ok_or_else(|| anyhow::anyhow!("sstable {} not found", sst_id))
                                    .cloned()
                            })
                            .collect::<Result<_, _>>()?;
                        let lower_sstables_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?;
                        println!("created lower iterator");
                        let mut iter =
                            TwoMergeIterator::create(upper_sstables_iter, lower_sstables_iter)?;
                        println!("created two merge iter");

                        //  move over iterator and build out the new sstables with the resulting values
                        let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
                        let mut is_built = false;
                        while iter.is_valid() {
                            is_built = false;
                            let key = iter.key();
                            let value = iter.value();
                            if value.is_empty() {
                                iter.next()?;
                                continue;
                            }
                            sstable_builder.add(key, value);

                            if sstable_builder.estimated_size() >= self.options.target_sst_size {
                                let sst_id = self.next_sst_id();
                                let sstable = sstable_builder.build(
                                    sst_id,
                                    Some(Arc::clone(&self.block_cache)),
                                    self.path_of_sst(sst_id),
                                )?;
                                is_built = true;
                                sstable_builder = SsTableBuilder::new(self.options.block_size);
                                sstables.push(Arc::new(sstable));
                            }
                            iter.next()?;
                        }

                        if !is_built {
                            let sst_id = self.next_sst_id();
                            let sstable = sstable_builder.build(
                                sst_id,
                                Some(Arc::clone(&self.block_cache)),
                                self.path_of_sst(sst_id),
                            )?;
                            sstables.push(Arc::new(sstable));
                        }
                        return Ok(sstables);
                    }
                    //  includes L0 as the upper level
                    None => {
                        let upper_sstables: Vec<_> = upper_level_sst_ids
                            .iter()
                            .map(|sst_id| {
                                snapshot
                                    .sstables
                                    .get(sst_id)
                                    .ok_or_else(|| anyhow::anyhow!("sstable {} not found", sst_id))
                                    .cloned()
                            })
                            .collect::<Result<_, _>>()?;
                        let upper_sstables_iter: Vec<_> = upper_sstables
                            .iter()
                            .map(|sstable| {
                                SsTableIterator::create_and_seek_to_first(sstable.clone())
                                    .map(Box::new)
                            })
                            .collect::<Result<_, _>>()?;
                        let upper_sstables_iter = MergeIterator::create(upper_sstables_iter);

                        let lower_sstables: Vec<_> = lower_level_sst_ids
                            .iter()
                            .map(|sst_id| {
                                snapshot
                                    .sstables
                                    .get(sst_id)
                                    .ok_or_else(|| anyhow::anyhow!("sstable {} not found", sst_id))
                                    .cloned()
                            })
                            .collect::<Result<_, _>>()?;
                        let lower_sstables_iter =
                            SstConcatIterator::create_and_seek_to_first(lower_sstables)?;

                        let mut iter =
                            TwoMergeIterator::create(upper_sstables_iter, lower_sstables_iter)?;

                        //  move over iterator and build out the new sstables with the resulting values
                        let mut sstable_builder = SsTableBuilder::new(self.options.block_size);
                        let mut is_built = false;
                        while iter.is_valid() {
                            is_built = false;
                            let key = iter.key();
                            let value = iter.value();
                            if value.is_empty() {
                                iter.next()?;
                                continue;
                            }
                            sstable_builder.add(key, value);

                            if sstable_builder.estimated_size() >= self.options.target_sst_size {
                                let sst_id = self.next_sst_id();
                                let sstable = sstable_builder.build(
                                    sst_id,
                                    Some(Arc::clone(&self.block_cache)),
                                    self.path_of_sst(sst_id),
                                )?;
                                is_built = true;
                                sstable_builder = SsTableBuilder::new(self.options.block_size);
                                sstables.push(Arc::new(sstable));
                            }
                            iter.next()?;
                        }

                        if !is_built {
                            let sst_id = self.next_sst_id();
                            let sstable = sstable_builder.build(
                                sst_id,
                                Some(Arc::clone(&self.block_cache)),
                                self.path_of_sst(sst_id),
                            )?;
                            sstables.push(Arc::new(sstable));
                        }
                        return Ok(sstables);
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "compaction task variant apart from ForceFullCompaction passed in"
                ));
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            let l0_sstables = state.l0_sstables.clone();
            let l1_sstables = state.levels[0].1.clone();
            (l0_sstables, l1_sstables)
        };
        let new_sstables = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        /*
         Remove the old sstables from l0, l1 and the sstables hashmap
         Add the newly generated sstables to l1
        */
        //  First remove the old sstables from l0 and l1
        let _state_lock = self.state_lock.lock();
        let mut state_guard = self.state.write();
        let state = Arc::make_mut(&mut state_guard);
        state.l0_sstables.retain(|id| !l0_sstables.contains(id));
        for (level, sst_ids) in &mut state.levels {
            if *level == 1 {
                sst_ids.retain(|id| !l1_sstables.contains(id));
            }
        }

        //  remove the old sstables from the hashmap of id -> sstable
        for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            state.sstables.remove(sst_id);
        }

        //  add the new sstables to levels with an l1 key
        let (_, sst_ids_at_l1) = state
            .levels
            .iter_mut()
            .find(|(level, _)| *level == 1)
            .expect("No level found for L1 in levels");
        sst_ids_at_l1.extend(new_sstables.iter().map(|sstable| sstable.sst_id()));

        //  add the new sstables to the hashmap struct sstables
        for sst in new_sstables {
            state.sstables.insert(sst.sst_id(), sst);
        }

        //  remove the files for the old sstables
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        self.dump_structure();
        println!("created a compaction task {:?}", task);
        let sstables_to_add = self.compact(&task)?;
        let output: Vec<usize> = sstables_to_add
            .iter()
            .map(|sstable| sstable.sst_id())
            .collect();
        let (mut new_state, sstable_ids_to_remove) = self
            .compaction_controller
            .apply_compaction_result(&snapshot, &task, &output);

        //  remove old files from state and file system
        //  add new files to the state
        //  replace state
        println!("waiting to acquire lock");
        let _state_lock = self.state_lock.lock();
        for sstable_id in &sstable_ids_to_remove {
            let sst = new_state.sstables.remove(&sstable_id);
            assert!(sst.is_some());
            std::fs::remove_file(self.path_of_sst(*sstable_id))?;
        }
        for sstable in &sstables_to_add {
            new_state.sstables.insert(sstable.sst_id(), sstable.clone());
        }
        let mut state = self.state.write();
        *state = Arc::new(new_state);
        println!(
            "compaction finished -> files added {:?}, files removed {:?}",
            sstables_to_add
                .iter()
                .map(|sst| sst.sst_id())
                .collect::<Vec<_>>(),
            sstable_ids_to_remove
        );

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        {
            let state_guard = self.state.read();
            if state_guard.imm_memtables.len() < self.options.num_memtable_limit {
                return Ok(());
            }
        }

        self.force_flush_next_imm_memtable()?;
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
