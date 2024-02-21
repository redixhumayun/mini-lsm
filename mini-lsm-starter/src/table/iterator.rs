use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        //  get the first block from the sstable and build an iterator on top of it
        let block = table.read_block_cached(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);
        let iter = SsTableIterator {
            table,
            blk_iter: block_iterator,
            blk_idx: 0,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        //  get metata for first block index
        let block = self.table.read_block_cached(0)?;
        let block_iterator = BlockIterator::create_and_seek_to_first(block);
        self.blk_idx = 0;
        self.blk_iter = block_iterator;
        Ok(())
    }

    fn seek_to(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut block_index = table.find_block_idx(key);
        let block = table.read_block_cached(block_index).unwrap();
        let mut block_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !block_iter.is_valid() {
            block_index += 1;
            if block_index < table.num_of_blocks() {
                block_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(block_index)?);
            }
        }
        Ok((block_index, block_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (block_index, block_iter) = Self::seek_to(&table, key)?;
        let iter = SsTableIterator {
            table,
            blk_iter: block_iter,
            blk_idx: block_index,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (block_index, block_iter) = Self::seek_to(&self.table, key)?;
        self.blk_iter = block_iter;
        self.blk_idx = block_index;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.blk_iter.next();
            if !self.blk_iter.is_valid() {
                self.blk_idx += 1;
                if self.blk_idx < self.table.num_of_blocks() {
                    let new_block = self.table.read_block_cached(self.blk_idx)?;
                    let new_block_iter = BlockIterator::create_and_seek_to_first(new_block);
                    self.blk_iter = new_block_iter;
                }
            }
        }
        Ok(())
    }
}
