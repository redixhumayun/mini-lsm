#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let value_range = self.value_range;
        let value_raw = &self.block.data[value_range.0..value_range.1];
        value_raw
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        if self.key.is_empty() {
            return false;
        }
        true
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
        self.idx = 0;
    }

    fn seek_to(&mut self, index: usize) {
        let start_next_kv = self.value_range.1;
        let data_to_consider = &self.block.data[start_next_kv..];

        let (key_length_raw, rest) = data_to_consider.split_at(2);
        let key_length = u16::from_le_bytes(key_length_raw.try_into().unwrap());

        let (key, rest) = rest.split_at(key_length as usize);
        self.key = KeyVec::from_vec(key.to_vec());

        let (value_length_raw, rest) = rest.split_at(2);
        let value_length = u16::from_le_bytes(value_length_raw.try_into().unwrap());

        let (value, _) = rest.split_at(value_length as usize);
        let new_value_start = start_next_kv + 2 + key_length as usize;
        self.value_range = (
            new_value_start + 2,
            new_value_start + 2 + value_length as usize,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;

        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        self.seek_to(self.idx);
    }

    fn get_key_at_index(&mut self, index: usize) -> KeySlice {
        self.seek_to(index);
        self.idx = index;
        self.key.as_key_slice()
    }

    fn update_iterator_to_current_index(&mut self) {
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_key = self.get_key_at_index(mid);

            match mid_key.cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid - 1,
                std::cmp::Ordering::Equal => break,
            }
        }
        self.idx = low;
        self.update_iterator_to_current_index();
    }
}
