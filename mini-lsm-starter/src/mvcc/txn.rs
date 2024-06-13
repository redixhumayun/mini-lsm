#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use core::fmt;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn map_bound(&self, bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
            Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(entry) = self.local_storage.get(key) {
            match entry.value().is_empty() {
                true => return Ok(None),
                false => return Ok(Some(entry.value().clone())),
            };
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let mut txn_loc_iter = TxnLocalIteratorBuilder {
            map: Arc::clone(&self.local_storage),
            iter_builder: |map| map.range((self.map_bound(lower), self.map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        txn_loc_iter.next()?;
        let fused_lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let iter = TwoMergeIterator::create(txn_loc_iter, fused_lsm_iter)?;
        Ok(TxnIterator {
            txn: Arc::clone(&self),
            iter,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::from(Vec::new()));
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Transaction already committed"));
        }
        self.committed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let batch = self
            .local_storage
            .iter()
            .map(|entry| match entry.value().is_empty() {
                true => WriteBatchRecord::Del(entry.key().clone()),
                false => WriteBatchRecord::Put(entry.key().clone(), entry.value().clone()),
            })
            .collect::<Vec<WriteBatchRecord<Bytes>>>();
        self.inner.write_batch(&batch)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner
            .mvcc()
            .expect("could not get access to mvcc object when dropping txn")
            .ts
            .lock()
            .1
            .remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl fmt::Debug for TxnLocalIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "debug for txn local iter not implemented")
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.with_item(|item| &item.1)
    }

    fn key(&self) -> &[u8] {
        self.with_item(|item| &item.0)
    }

    fn is_valid(&self) -> bool {
        !self.with_item(|item| item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let next_item = self.with_iter_mut(|iter| {
            iter.next()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
        });
        self.with_item_mut(|item| *item = next_item);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { txn, iter })
    }
}

impl fmt::Debug for TxnIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnIterator {{ {:?} }}", self.iter)
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.iter.next()?;
        }
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
