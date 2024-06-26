#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::{Block, BlockBuilder},
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

pub const CHECKSUM_LENGTH: usize = 4;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes
            .push(farmhash::fingerprint32(&key.key_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            self.max_ts = self.max_ts.max(key.ts());
            return;
        }

        self.freeze_block();

        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
        self.max_ts = self.max_ts.max(key.ts());
    }

    /// This function will take current block builder, build it and replace it with a fresh block builder
    /// It will add the block to the SSTable data and then create and store the metadata for this block
    fn freeze_block(&mut self) {
        //  the block is full, split block and replace older builder
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let encoded_block = Block::encode(&block);
        let checksum = crc32fast::hash(&block.data);
        let encoded_checksum = checksum.to_be_bytes();

        //  get metadata for split block
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        };

        self.data.extend_from_slice(&encoded_block);
        self.data.extend_from_slice(&encoded_checksum);
        self.meta.push(block_meta);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.freeze_block();

        //  create the bloom filter
        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom_filter = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);

        let mut encoded_sst: Vec<u8> = Vec::new();
        encoded_sst.extend_from_slice(&self.data);

        //  encode meta section for each block
        let mut encoded_meta: Vec<u8> = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_meta, self.max_ts);

        //  calculate the checksum for the metadata
        let meta_checksum = crc32fast::hash(&encoded_meta);
        let meta_checksum_bytes = meta_checksum.to_le_bytes();

        //  add meta to encoding along with checksum
        encoded_sst.extend_from_slice(&encoded_meta);
        encoded_sst.extend_from_slice(&meta_checksum_bytes);

        //  encode the meta block offset in the next 4 bytes
        let data_len = (self.data.len() as u32).to_le_bytes();
        encoded_sst.extend_from_slice(&data_len);

        //  encode the bloom filter and add it to encoded table
        let bloom_filter_offset = encoded_sst.len() as u32;
        bloom_filter.encode(&mut encoded_sst);
        encoded_sst.extend_from_slice(&bloom_filter_offset.to_le_bytes());

        //  write the entire encoding to disk
        let file = FileObject::create(path.as_ref(), encoded_sst)?;
        Ok(SsTable {
            file,
            block_meta_offset: self.data.len(),
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom_filter),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
