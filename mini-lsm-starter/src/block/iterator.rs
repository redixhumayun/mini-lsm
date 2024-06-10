use core::fmt;
use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl fmt::Debug for BlockIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block Iterator {{ ")?;
        write!(
            f,
            "current key: {}, index: {}",
            String::from_utf8_lossy(self.key.raw_ref()),
            self.idx
        )?;
        write!(f, " }} ")
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: BlockIterator::decode_first_key(&block),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    fn decode_first_key(block: &Arc<Block>) -> KeyVec {
        let mut buf = &block.data[..];
        let overlap_length = buf.get_u16_le(); //  read the overlap length (should be 0)
        assert_eq!(overlap_length, 0);
        let key_length = buf.get_u16_le(); //  read the key length
        let key = &buf[..key_length as usize];
        KeyVec::from_vec(key.to_vec())
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
        let offset = self.block.offsets[index] as usize;
        let data_to_consider = &self.block.data[offset..];

        let (key_overlap_length_raw, rest) = data_to_consider.split_at(2);
        let key_overlap_length = u16::from_le_bytes(key_overlap_length_raw.try_into().unwrap());

        let (key_length_raw, rest) = rest.split_at(2);
        let key_length = u16::from_le_bytes(key_length_raw.try_into().unwrap());

        let (key, rest) = rest.split_at(key_length as usize);
        let key_overlap = &(self.first_key.raw_ref())[..key_overlap_length as usize];
        let mut full_key = Vec::new();
        full_key.extend_from_slice(&key_overlap);
        full_key.extend_from_slice(&key);
        self.key = KeyVec::from_vec(full_key);

        let (value_length_raw, rest) = rest.split_at(2);
        let value_length = u16::from_le_bytes(value_length_raw.try_into().unwrap());

        let (_, _) = rest.split_at(value_length as usize);
        let new_value_start = offset + 2 + 2 + key_length as usize;
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

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            self.idx = mid;
            let mid_key = self.key.as_key_slice();

            match mid_key.cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    high = mid - 1;
                }
                std::cmp::Ordering::Equal => return,
            }
        }

        if low >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        self.idx = low;
        self.seek_to(self.idx);
    }
}
