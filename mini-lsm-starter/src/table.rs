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
use builder::CHECKSUM_LENGTH;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{Key, KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const BLOOM_FILTER_OFFSET_LEN: u64 = 4;
const META_BLOCK_OFFSET_LEN: u64 = 4;

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
        for meta in block_meta {
            buf.put_u16_le(meta.offset as u16);

            //  add the first key
            buf.put_u16_le(meta.first_key.key_len() as u16);
            buf.put_slice(meta.first_key.key_ref());
            buf.put_u64_le(meta.first_key.ts());

            //  add the second key
            buf.put_u16_le(meta.last_key.key_len() as u16);
            buf.put_slice(meta.last_key.key_ref());
            buf.put_u64_le(meta.last_key.ts());
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
            let first_key_ts = buf.get_u64_le();

            let last_key_length = buf.get_u16_le() as usize;
            let mut last_key = vec![0; last_key_length];
            buf.copy_to_slice(&mut last_key);
            let last_key_ts = buf.get_u64_le();

            block_metas.push(BlockMeta {
                offset,
                first_key: Key::from_vec_with_ts(first_key, first_key_ts).into_key_bytes(),
                last_key: Key::from_vec_with_ts(last_key, last_key_ts).into_key_bytes(),
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
        write!(f, "Key range: {:?} -> {:?}", self.first_key, self.last_key)
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
        let bloom_filter_offset_raw =
            file.read(len - BLOOM_FILTER_OFFSET_LEN, BLOOM_FILTER_OFFSET_LEN)?;
        let bloom_filter_offset = (&bloom_filter_offset_raw[..]).get_u32_le() as u64;

        //  use the bloom filter offset to read the data starting
        let raw_bloom_filter = file.read(
            bloom_filter_offset,
            len - bloom_filter_offset - BLOOM_FILTER_OFFSET_LEN,
        )?;
        let bloom_filter = Bloom::decode(&raw_bloom_filter)?;

        //  read the 4 bytes before the bloom offset to get the meta offset
        let raw_meta_offset = file.read(
            bloom_filter_offset - META_BLOCK_OFFSET_LEN,
            META_BLOCK_OFFSET_LEN,
        )?;
        let meta_offset = (&raw_meta_offset[..]).get_u32_le() as u64;

        //  use the meta offset to read the metadata from the file
        let raw_meta = file.read(meta_offset as u64, bloom_filter_offset - 4 - meta_offset)?;
        let meta_length = raw_meta.len() - CHECKSUM_LENGTH;
        let (raw_meta_data, checksum_bytes) = raw_meta.split_at(meta_length);

        //  compare checksums
        let stored_checksum = u32::from_le_bytes(
            checksum_bytes
                .try_into()
                .expect("checksum has incorrect length in sstable meta"),
        );
        let computed_checksum = crc32fast::hash(raw_meta_data);
        if stored_checksum != computed_checksum {
            return Err(anyhow::anyhow!(
                "checksum mismatch in metadata. computed: {}, actual: {}",
                computed_checksum,
                stored_checksum
            ));
        }

        //  decode the meta
        let meta = BlockMeta::decode_block_meta(raw_meta_data);

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

        let block = Block::decode(&block_data_raw[..&block_data_raw.len() - CHECKSUM_LENGTH]);
        let stored_checksum = u32::from_be_bytes(
            block_data_raw[block_data_raw.len() - CHECKSUM_LENGTH..]
                .try_into()
                .expect("checksum has incorrect length"),
        );

        let computed_checksum = crc32fast::hash(&block.data);
        if stored_checksum != computed_checksum {
            return Err(anyhow::anyhow!(
                "checksum mismatch in block {}. computed -> {}, actual -> {}",
                block_idx,
                computed_checksum,
                stored_checksum
            ));
        }
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = &self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow::anyhow!("Error reading cached block: {}", e))?;
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
