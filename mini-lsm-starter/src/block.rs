mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        println!("ENTER: Block::encode");
        let mut buffer = BytesMut::new();

        //  encode the data
        for value in &self.data {
            buffer.put_u8(*value);
        }

        //  encode the offsets
        println!(
            "Offsets within the block for key value pairs: {:?}",
            self.offsets
        );
        for offset in &self.offsets {
            buffer.put_u16_le(*offset);
        }

        // //  encode the number of elements
        buffer.put_u16_le(self.offsets.len() as u16);
        println!("EXIT: Block::encode");
        buffer.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        println!("ENTER: Block::decode");
        //  read the last 2 bytes to get the number of elements
        let len = data.len();
        let num_of_elems_raw = &data[len - 2..len];
        let num_of_elems = u16::from_le_bytes([num_of_elems_raw[0], num_of_elems_raw[1]]);
        println!("The number of elements in the block {}", num_of_elems);

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
}
