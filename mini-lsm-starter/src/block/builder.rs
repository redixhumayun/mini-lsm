use crate::key::{KeySlice, KeyVec};

use super::Block;

const OVERLAP_KEY_LENGTH_BYTES: usize = 2;
const REMAINING_KEY_LENGTH_BYTES: usize = 2;
const VALUE_LENGTH_BYTES: usize = 2;

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
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            panic!("key cannot be empty while adding to block builder");
        }
        //  get the overlap of the key with the first key
        let key_overlap = key
            .key_ref()
            .iter()
            .zip(self.first_key.as_key_slice().key_ref().iter())
            .take_while(|(a, b)| a == b)
            .count() as u16;
        let key_overlap_bytes = key_overlap.to_le_bytes();
        let rest_of_key = &(key.key_ref())[key_overlap as usize..];
        let rest_of_key_len = (rest_of_key.len() as u16).to_le_bytes();
        let timestamp = key.ts().to_le_bytes();

        let value_length = value.len();
        let value_length_bytes = (value_length as u16).to_le_bytes();

        // let entry_size = 2 + rest_of_key.len() + 2 + value_length + 2;   //  older version, only here for comparison
        //  TODO: Should i include timestamp in entry size calculation?
        let entry_size = OVERLAP_KEY_LENGTH_BYTES
            + REMAINING_KEY_LENGTH_BYTES
            + rest_of_key.len()
            + VALUE_LENGTH_BYTES
            + value_length;

        if self.data.len() + self.offsets.len() + entry_size > self.block_size
            && self.first_key.key_ref().len() > 0
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        self.data.extend_from_slice(&key_overlap_bytes);
        self.data.extend_from_slice(&rest_of_key_len);
        self.data.extend_from_slice(rest_of_key);
        self.data.extend_from_slice(&timestamp);
        self.data.extend_from_slice(&value_length_bytes);
        self.data.extend_from_slice(value);

        if self.first_key.key_ref().len() == 0 {
            let mut new_key = KeyVec::new();
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
