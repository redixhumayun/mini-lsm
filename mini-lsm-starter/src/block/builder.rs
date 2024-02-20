use crate::key::{Key, KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: Key::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_length = key.len();
        let key_length_bytes = (key_length as u16).to_le_bytes();
        let value_length = value.len();
        let value_length_bytes = (value_length as u16).to_le_bytes();
        let size_required = 2 + key_length + 2 + value_length + 2;

        if self.data.len() + self.offsets.len() + size_required > self.block_size
            && self.first_key.raw_ref().len() > 0
        {
            return false;
        }

        println!("Offset for this key-value pair is {}", self.data.len());
        self.offsets.push(self.data.len() as u16);

        self.data.extend_from_slice(&key_length_bytes);
        self.data.extend_from_slice(key.into_inner());
        self.data.extend_from_slice(&value_length_bytes);
        self.data.extend_from_slice(value);

        if self.first_key.raw_ref().len() == 0 {
            let mut new_key = Key::new();
            new_key.set_from_slice(key);
            self.first_key = new_key;
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn size(&self) -> usize {
        self.offsets.len()
    }
}
