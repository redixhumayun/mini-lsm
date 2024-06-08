// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

use super::builder::CHECKSUM_LENGTH;

const NUM_HASH_FN_LEN: usize = 1;

/// Implements a bloom filter
pub struct Bloom {
    /// data of filter in bits
    pub(crate) filter: Bytes,
    /// number of hash functions
    pub(crate) k: u8,
}

pub trait BitSlice {
    fn get_bit(&self, idx: usize) -> bool;
    fn bit_len(&self) -> usize;
}

pub trait BitSliceMut {
    fn set_bit(&mut self, idx: usize, val: bool);
}

impl<T: AsRef<[u8]>> BitSlice for T {
    fn get_bit(&self, idx: usize) -> bool {
        let pos = idx / 8;
        let offset = idx % 8;
        (self.as_ref()[pos] & (1 << offset)) != 0
    }

    fn bit_len(&self) -> usize {
        self.as_ref().len() * 8
    }
}

impl<T: AsMut<[u8]>> BitSliceMut for T {
    fn set_bit(&mut self, idx: usize, val: bool) {
        let pos = idx / 8;
        let offset = idx % 8;
        if val {
            self.as_mut()[pos] |= 1 << offset;
        } else {
            self.as_mut()[pos] &= !(1 << offset);
        }
    }
}

impl Bloom {
    /// Decode a bloom filter
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let filter = &buf[..buf.len() - NUM_HASH_FN_LEN - CHECKSUM_LENGTH];
        let k = buf[buf.len() - NUM_HASH_FN_LEN - CHECKSUM_LENGTH];
        let stored_checksum_bytes = &buf[buf.len() - CHECKSUM_LENGTH..];
        let stored_checksum = u32::from_le_bytes(
            stored_checksum_bytes
                .try_into()
                .expect("checksum has incorrect length in bloom filter"),
        );
        let computed_checksum = crc32fast::hash(&buf[..buf.len() - CHECKSUM_LENGTH]);
        if stored_checksum != computed_checksum {
            return Err(anyhow::anyhow!(
                "checksum mismatch in bloom filter, computed -> {}, stored -> {}",
                computed_checksum,
                stored_checksum
            ));
        }
        Ok(Self {
            filter: filter.to_vec().into(),
            k,
        })
    }

    /// Encode a bloom filter
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut temp_buf: Vec<u8> = Vec::new();
        temp_buf.extend(&self.filter);
        temp_buf.put_u8(self.k);
        let checksum = crc32fast::hash(&temp_buf);
        buf.extend(temp_buf);
        buf.extend(checksum.to_le_bytes());
    }

    /// Get bloom filter bits per key from entries count and FPR
    pub fn bloom_bits_per_key(entries: usize, false_positive_rate: f64) -> usize {
        let size =
            -1.0 * (entries as f64) * false_positive_rate.ln() / std::f64::consts::LN_2.powi(2);
        let locs = (size / (entries as f64)).ceil();
        locs as usize
    }

    /// Build bloom filter from key hashes
    pub fn build_from_key_hashes(keys: &[u32], bits_per_key: usize) -> Self {
        let k = (bits_per_key as f64 * 0.69) as u32;
        let k = k.min(30).max(1);
        let nbits = (keys.len() * bits_per_key).max(64);
        let nbytes = (nbits + 7) / 8;
        let nbits = nbytes * 8;
        let mut filter = BytesMut::with_capacity(nbytes);
        filter.resize(nbytes, 0);

        for key in keys {
            let mut hash = *key;
            let delta = (hash >> 17) | (hash << 15);
            for hash_function in 0..k {
                filter.set_bit((hash as usize) % nbits, true);
                hash = hash.wrapping_add(delta);
            }
        }

        Self {
            filter: filter.freeze(),
            k: k as u8,
        }
    }

    /// Check if a bloom filter may contain some data
    pub fn may_contain(&self, mut h: u32) -> bool {
        if self.k > 30 {
            // potential new encoding for short bloom filters
            true
        } else {
            let nbits = self.filter.bit_len();
            let delta = (h >> 17) | (h << 15);

            for hash_function in 0..self.k {
                if !self.filter.get_bit((h as usize) % nbits) {
                    return false;
                }
                h = h.wrapping_add(delta);
            }

            true
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn encode_and_decode_bloom_filter() {
        let filter_bytes = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3];
        let bloom_filter = super::Bloom {
            filter: filter_bytes.clone().into(),
            k: 3,
        };
        let mut buf: Vec<u8> = Vec::new();
        bloom_filter.encode(&mut buf);
        let decoded_bloom_filter = super::Bloom::decode(&buf).unwrap();
        assert_eq!(decoded_bloom_filter.k, bloom_filter.k);
    }
}
