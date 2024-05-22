#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.len() == 0 {
            return Ok(Self {
                current: None,
                next_sst_idx: 1,
                sstables,
            });
        }
        let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sstables.first().expect(
            "At least one sstable should be present for an sstable concat iterator to be created",
        )))?;
        Ok(Self {
            current: Some(iter),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut index = 0;
        while index < sstables.len() {
            let iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(
                    sstables
                        .get(index)
                        .expect("Could not find the sstable while building the concat iterator"),
                ),
                key,
            )?;
            if iter.is_valid() {
                return Ok(Self {
                    current: Some(iter),
                    next_sst_idx: index + 1,
                    sstables,
                });
            }
            index += 1;
        }
        //  could not find a valid iterator
        Ok(Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(current) = self.current.as_ref() {
            current.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;

        //  check if iterator valid
        if self.current.as_ref().unwrap().is_valid() {
            return Ok(());
        }

        //  iterator is not valid, pick another valid iterator
        while let Some(current) = self.current.as_mut() {
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                return Ok(());
            }

            let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(
                &self.sstables[self.next_sst_idx],
            ))?;
            self.current = Some(iter);
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
