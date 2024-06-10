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
            String::from_utf8_lossy(self.key.key_ref()),
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
        let key_length = buf.get_u16_le() as usize; //  read the key length
        let key = &buf[..key_length];
        buf.advance(key_length);
        let ts = buf.get_u64_le();
        KeyVec::from_vec_with_ts(key.to_vec(), ts)
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
        let mut buf = &self.block.data[offset..];

        let key_overlap_length = buf.get_u16_le() as usize;
        let key_length = buf.get_u16_le() as usize;

        let key = &buf[..key_length];
        buf.advance(key_length);

        let ts = buf.get_u64_le();

        let key_overlap = &self.first_key.key_ref()[..key_overlap_length];
        let mut full_key = Vec::with_capacity(key_overlap_length + key_length);
        full_key.extend_from_slice(key_overlap);
        full_key.extend_from_slice(key);

        self.key = KeyVec::from_vec_with_ts(full_key, ts);

        let value_length = buf.get_u16_le() as usize;
        let value_start = offset + 2 + 2 + key_length + 8 + 2; //  offset + key overlap + key len + key + ts + val len
        self.value_range = (value_start, value_start + value_length);
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
