#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{Block, BlockBuilder},
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().raw_ref().to_vec();
        }
        if !self.builder.add(key, value) {
            self.freeze_block();

            //  add key, value pair to new block
            assert!(self.builder.add(key, value)); //  this should not fail now since its a new block
            self.first_key = key.to_key_vec().raw_ref().to_vec();
        }

        self.last_key = key.to_key_vec().raw_ref().to_vec();
    }

    /// This function will take current block builder, build it and replace it with a fresh block builder
    /// It will add the block to the SSTable data and then create and store the metadata for this block
    fn freeze_block(&mut self) {
        //  the block is full, split block and replace older builder
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let encoded_block = Block::encode(&block);

        //  get metadata for split block
        let block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: Key::from_vec(self.last_key.clone()).into_key_bytes(),
        };

        self.data.extend_from_slice(&encoded_block);
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
        let mut encoded_sst: Vec<u8> = Vec::new();

        encoded_sst.extend_from_slice(&self.data);

        //  encode meta section for each block
        let mut encoded_meta: Vec<u8> = Vec::new();
        BlockMeta::encode_block_meta(&self.meta, &mut encoded_meta);
        encoded_sst.extend_from_slice(&encoded_meta);

        //  encode the meta block offset in the last 4 bytes
        let data_len = (self.data.len() as u32).to_le_bytes();
        encoded_sst.extend_from_slice(&data_len);

        //  write the entire encoding to disk
        let file = FileObject::create(path.as_ref(), encoded_sst)?;
        Ok(SsTable {
            file,
            block_meta_offset: self.data.len(),
            id,
            block_cache: None,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
