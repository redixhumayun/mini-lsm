use std::{
    io::{self, ErrorKind},
    ops::Bound,
};

use anyhow::Result;
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper_bound: Bound<Bytes>) -> Result<Self> {
        let mut lsm_iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            upper_bound,
        };

        //  when an iterator is first created, there is the possibility that
        //  the very first key-value pair is a tombstone so need to account for that
        lsm_iter.skip_deleted_values()?;

        Ok(lsm_iter)
    }

    //  if the value associated with the key after calling next is an empty string
    //  this marks a tombstone. this key should be skipped so that the consumer
    //  of this iterator does not see it
    fn skip_deleted_values(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
            if !self.inner.is_valid() {
                self.is_valid = false;
                return Ok(());
            }
            match self.upper_bound.as_ref() {
                Bound::Included(key) => {
                    if self.inner.key().raw_ref() > key {
                        //invalidate the iterator
                        self.is_valid = false;
                    }
                }
                Bound::Excluded(key) => {
                    if self.inner.key().raw_ref() >= key {
                        //  invalidate the iterator
                        self.is_valid = false;
                    }
                }
                Bound::Unbounded => {}
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        println!("ENTER: lsm_iterator::next()");
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.upper_bound.as_ref() {
            Bound::Included(key) => {
                if self.inner.key().raw_ref() > key {
                    //invalidate the iterator
                    self.is_valid = false;
                }
            }
            Bound::Excluded(key) => {
                if self.inner.key().raw_ref() >= key {
                    //  invalidate the iterator
                    self.is_valid = false;
                }
            }
            Bound::Unbounded => {}
        }
        self.skip_deleted_values()?;
        println!("EXIT: lsm_iterator::next()");
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
