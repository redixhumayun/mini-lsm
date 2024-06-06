#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use core::fmt;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        for ind_block_meta in block_meta {
            let offset_bytes = (ind_block_meta.offset as u16).to_le_bytes();
            buf.extend_from_slice(&offset_bytes);

            let first_key_length = ind_block_meta.first_key.len() as u16;
            let first_key_length_bytes = first_key_length.to_le_bytes();
            buf.extend_from_slice(&first_key_length_bytes);
            buf.extend_from_slice(ind_block_meta.first_key.raw_ref());

            let last_key_length = ind_block_meta.last_key.len() as u16;
            let last_key_length_bytes = last_key_length.to_le_bytes();
            buf.extend_from_slice(&last_key_length_bytes);
            buf.extend_from_slice(ind_block_meta.last_key.raw_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_metas: Vec<BlockMeta> = Vec::new();

        while buf.remaining() > 0 {
            let offset = buf.get_u16_le() as usize;

            let first_key_length = buf.get_u16_le() as usize;
            let mut first_key = vec![0; first_key_length];
            buf.copy_to_slice(&mut first_key);

            let last_key_length = buf.get_u16_le() as usize;
            let mut last_key = vec![0; last_key_length];
            buf.copy_to_slice(&mut last_key);

            block_metas.push(BlockMeta {
                offset,
                first_key: Key::from_vec(first_key).into_key_bytes(),
                last_key: Key::from_vec(last_key).into_key_bytes(),
            });
        }

        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl fmt::Debug for SsTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Retrieve the first and last key-value pairs
        let first_block = self.read_block(0).expect("Unable to read first block");
        let last_block = self
            .read_block(self.num_of_blocks() - 1)
            .expect("Unable to read last block");

        // Extract the first key-value pair from the first block
        let first_key_value = extract_first_key_value(&first_block);
        let last_key_value = extract_last_key_value(&last_block);

        let first_key = String::from_utf8_lossy(&self.first_key.raw_ref());
        let first_value = String::from_utf8_lossy(&first_key_value.1);
        let last_key = String::from_utf8_lossy(&self.last_key.raw_ref());
        let last_value = String::from_utf8_lossy(&last_key_value.1);

        // Trim values to last 4 characters
        let trimmed_first_value = trim_to_last_4(&first_value);
        let trimmed_last_value = trim_to_last_4(&last_value);

        write!(
            f,
            "Key range: {} -> {} | Value range: {} -> {}",
            first_key, last_key, trimmed_first_value, trimmed_last_value
        )
    }
}

fn extract_first_key_value(block: &Block) -> (Vec<u8>, Vec<u8>) {
    let entry_start = *block.offsets.first().expect("Block is empty") as usize;
    let key_value = decode_key_value(&block.data, entry_start);
    key_value
}

fn extract_last_key_value(block: &Block) -> (Vec<u8>, Vec<u8>) {
    let entry_start = *block.offsets.last().expect("Block is empty") as usize;
    let key_value = decode_key_value(&block.data, entry_start);
    key_value
}

fn decode_key_value(data: &[u8], entry_start: usize) -> (Vec<u8>, Vec<u8>) {
    // Decode the key-value entry
    let key_overlap = u16::from_le_bytes([data[entry_start], data[entry_start + 1]]) as usize;
    let rest_of_key_len =
        u16::from_le_bytes([data[entry_start + 2], data[entry_start + 3]]) as usize;
    let rest_of_key = &data[entry_start + 4..entry_start + 4 + rest_of_key_len];

    let value_length_pos = entry_start + 4 + rest_of_key_len;
    let value_length =
        u16::from_le_bytes([data[value_length_pos], data[value_length_pos + 1]]) as usize;
    let value = &data[value_length_pos + 2..value_length_pos + 2 + value_length];

    (rest_of_key.to_vec(), value.to_vec())
}

fn trim_to_last_4(value: &str) -> String {
    if value.len() <= 4 {
        value.to_string()
    } else {
        value[value.len() - 4..].to_string()
    }
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        //  read the last 4 bytes to get the bloom filter offset
        let bloom_filter_offset_raw = file.read(len - 4, 4)?;
        let bloom_filter_offset = (&bloom_filter_offset_raw[..]).get_u32_le() as u64;

        //  use the bloom filter offset to read the data starting
        let raw_bloom_filter = file.read(bloom_filter_offset, len - bloom_filter_offset - 4)?;
        let bloom_filter = Bloom::decode(&raw_bloom_filter)?;

        //  read the 4 bytes before the bloom offset to get the meta offset
        let raw_meta_offset = file.read(bloom_filter_offset - 4, 4)?;
        let meta_offset = (&raw_meta_offset[..]).get_u32_le() as u64;

        //  use the meta offset to read the metadata from the file
        let raw_meta = file.read(meta_offset as u64, bloom_filter_offset - 4 - meta_offset)?;
        let meta = BlockMeta::decode_block_meta(raw_meta.as_slice());

        Ok(SsTable {
            file,
            block_meta_offset: meta_offset as usize,
            id,
            block_cache,
            first_key: meta.first().unwrap().first_key.clone(),
            last_key: meta.last().unwrap().last_key.clone(),
            block_meta: meta,
            bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset_start = self.block_meta[block_idx].offset;
        let block_offset_end = if block_idx + 1 < self.block_meta.len() {
            self.block_meta[block_idx + 1].offset
        } else {
            self.block_meta_offset
        };
        let block_len = block_offset_end - block_offset_start;
        let block_data_raw = self
            .file
            .read(block_offset_start as u64, block_len as u64)?;
        let block_data = Block::decode(&block_data_raw);
        Ok(Arc::new(block_data))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = &self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| {
                    println!("Error: {:?}", e);
                    anyhow::anyhow!(e)
                })?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key) //  parition_point does binary search
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
