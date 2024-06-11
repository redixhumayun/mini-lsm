use core::fmt;
use std::{
    io::{self, ErrorKind},
    ops::Bound,
};

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>, //  memtable and l0
    MergeIterator<SstConcatIterator>, //  l1 onwards
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<Bytes>,
    is_valid: bool,
    prev_key: Option<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper_bound: Bound<Bytes>) -> Result<Self> {
        let mut lsm_iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            upper_bound,
            prev_key: None,
        };

        lsm_iter.skip_keys()?;

        Ok(lsm_iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.upper_bound.as_ref() {
            Bound::Included(key) => {
                if self.inner.key().key_ref() > key {
                    self.is_valid = false;
                }
            }
            Bound::Excluded(key) => {
                if self.inner.key().key_ref() >= key {
                    self.is_valid = false;
                }
            }
            Bound::Unbounded => {}
        };
        Ok(())
    }

    fn skip_keys(&mut self) -> Result<()> {
        loop {
            let key = self.prev_key.take().unwrap_or(Bytes::from_static(&[]));
            //  skip all identical keys
            while self.inner.is_valid() && self.inner.key().key_ref() == key {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                self.is_valid = false;
                break;
            }
            self.prev_key = Some(Bytes::copy_from_slice(self.inner.key().key_ref()));
            //  skip all deleted keys
            if !self.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for LsmIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LsmIterator {{ {:?} }}", self.inner)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        &self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.skip_keys()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> fmt::Debug for FusedIterator<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FusedIterator {{ {:?} }}", self.iter)
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(io::Error::new(ErrorKind::Other, "The iterator has errored").into());
        }
        if !self.is_valid() {
            return Ok(());
        }
        match self.iter.next() {
            Ok(_) => Ok(()),
            Err(e) => {
                self.has_errored = true;
                Err(e)
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::{
        compact::CompactionOptions,
        iterators::{
            concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
            two_merge_iterator::TwoMergeIterator, StorageIterator,
        },
        key::KeySlice,
        lsm_storage::{LsmStorageInner, LsmStorageOptions},
        mem_table::MemTable,
        tests::harness::generate_sst_with_ts,
    };

    use super::LsmIterator;

    /// This test asserts that an LSM iterator will only return a single version of a key
    /// and skip over all identical keys with different timestamps
    #[test]
    fn lsm_iter_skip_identical_keys() {
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction);
        let storage = LsmStorageInner::open(&dir, options).unwrap();
        let mut sst_data: Vec<((Bytes, u64), Bytes)> = Vec::new();
        for i in 0..=100 {
            let key = Bytes::from(format!("key{:03}", i));
            let ts = i;
            let value: Bytes = Bytes::from(format!("value{:03}", i));
            sst_data.push(((key, ts), value));
        }
        let sst = generate_sst_with_ts(
            0,
            &dir.path().join("0.sst"),
            sst_data,
            Some(storage.block_cache),
        );
        let sst = Arc::new(sst);
        let l1_iter = SstConcatIterator::create_and_seek_to_first(vec![Arc::clone(&sst)]).unwrap();
        let l1_merge_iter = MergeIterator::create(vec![Box::new(l1_iter)]);
        println!("the l1 merge iter {:?}\n", l1_merge_iter);

        let memtable = MemTable::create(1);
        for i in 0..=100 {
            let key = Bytes::from(format!("key{:03}", i));
            let ts = 100 + i;
            let value = Bytes::from(format!("value{:03}", i));
            memtable
                .put(KeySlice::for_testing_from_slice_with_ts(&key, ts), &value)
                .unwrap();
        }
        let memtable_iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        let memtable_merge_iter = MergeIterator::create(vec![Box::new(memtable_iter)]);

        let two_merge_iter_1 =
            TwoMergeIterator::create(memtable_merge_iter, MergeIterator::create(Vec::new()))
                .unwrap();
        let lsm_iter_inner = TwoMergeIterator::create(two_merge_iter_1, l1_merge_iter).unwrap();
        let mut lsm_iter = LsmIterator::new(lsm_iter_inner, Bound::Unbounded).unwrap();

        //  assert that each key is only seen once
        let expected_keys = (0..=100).map(|i| Bytes::from(format!("key{:03}", i)));
        for key in expected_keys {
            assert_eq!(lsm_iter.key(), key);
            lsm_iter.next().unwrap();
        }
    }
}
