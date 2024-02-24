mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

use crate::key::{Key, KeySlice};

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buffer = BytesMut::new();

        //  encode the data
        for value in &self.data {
            buffer.put_u8(*value);
        }

        //  encode the offsets
        for offset in &self.offsets {
            buffer.put_u16_le(*offset);
        }

        // //  encode the number of elements
        buffer.put_u16_le(self.offsets.len() as u16);
        buffer.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        //  read the last 2 bytes to get the number of elements
        let len = data.len();
        let num_of_elems_raw = &data[len - 2..len];
        let num_of_elems = u16::from_le_bytes([num_of_elems_raw[0], num_of_elems_raw[1]]);

        //  read the offset
        let offset_raw = &data[(len - 2 - (num_of_elems * 2) as usize)..len - 2];
        let mut offsets = Vec::new();
        for i in (0..offset_raw.len()).step_by(2) {
            let offset = u16::from_le_bytes([offset_raw[i], offset_raw[i + 1]]);
            offsets.push(offset);
        }

        //  read the data
        let data_raw = &data[0..(len - 2 - (num_of_elems * 2) as usize)];
        let data = data_raw.to_vec();

        Self { data, offsets }
    }

    pub fn print(&self) {
        let sep = "=".repeat(10);
        println!("{} {} {}", sep, format!("{:^10}", "Block"), sep);

        let mut first_key: &[u8] = &[];
        for (i, &offset) in self.offsets.iter().enumerate() {
            let entry_start = offset as usize;
            // Determine next offset or end of data for entry boundary
            let entry_end = if i + 1 < self.offsets.len() {
                self.offsets[i + 1] as usize
            } else {
                self.data.len()
            };

            // Extracting the entry data
            let entry_data = &self.data[entry_start..entry_end];

            // Decode the overlap (not directly printed but essential for understanding structure)
            let key_overlap = u16::from_le_bytes([entry_data[0], entry_data[1]]) as usize;

            // Decode the rest of the key length
            let rest_of_key_len = u16::from_le_bytes([entry_data[2], entry_data[3]]) as usize;

            // Decode the rest of the key
            let rest_of_key = &entry_data[4..4 + rest_of_key_len];

            if first_key.len() == 0 {
                first_key = rest_of_key;
            }
            let full_key = &mut first_key[..key_overlap].to_vec();
            full_key.extend_from_slice(rest_of_key);

            // Decode the value length
            let value_length_pos = 4 + rest_of_key_len;
            let value_length = u16::from_le_bytes([
                entry_data[value_length_pos],
                entry_data[value_length_pos + 1],
            ]) as usize;

            // Decode the value
            let value = &entry_data[value_length_pos + 2..value_length_pos + 2 + value_length];

            // Print key and value
            println!("Key   {}: {}", i, String::from_utf8_lossy(full_key));
            println!("Value {}: {}", i, String::from_utf8_lossy(value));
            println!("{}", "-".repeat(12));
        }
        println!("Offsets: {:?}", self.offsets);
        println!("{} {} {}", sep, format!("{:^10}", "End Block"), sep);
    }
}
