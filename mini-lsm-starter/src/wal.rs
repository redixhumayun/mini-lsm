use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

const CHECKSUM_LENGTH: usize = 4;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .context("failed to create WAL file")?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .context("failed to open WAL file")?;
        let mut reader = BufReader::new(&file);
        loop {
            let mut key_length_buf = [0u8; 8];
            match reader.read_exact(&mut key_length_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => eprintln!("encountered error while reading wal: {}", e),
            }
            let key_length = u64::from_be_bytes(key_length_buf);

            let mut key = vec![0u8; key_length as usize];
            reader
                .read_exact(&mut key)
                .context("failed to read key from wal")?;

            let mut value_length_buf = [0u8; 8];
            reader
                .read_exact(&mut value_length_buf)
                .context("failed to read value length from wal")?;
            let value_length = u64::from_be_bytes(value_length_buf);

            let mut value = vec![0u8; value_length as usize];
            reader
                .read_exact(&mut value)
                .context("failed to read value from wal")?;

            let mut checksum_buf = [0u8; CHECKSUM_LENGTH];
            reader
                .read_exact(&mut checksum_buf)
                .context("failed to read checksum from wal")?;
            let stored_checksum = u32::from_be_bytes(checksum_buf);
            let computed_checksum = crc32fast::hash(&[key.clone(), value.clone()].concat());
            if stored_checksum != computed_checksum {
                return Err(anyhow::anyhow!(
                    "checksum mismatch in wal. stored -> {}, computed -> {}",
                    stored_checksum,
                    computed_checksum
                ));
            }

            skiplist.insert(Bytes::from(key), Bytes::from(value));
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let key_length = key.len() as u64;
        let value_length = value.len() as u64;
        let checksum = crc32fast::hash(&[key, value].concat());
        let data = [
            &key_length.to_be_bytes(),
            key,
            &value_length.to_be_bytes(),
            value,
            &checksum.to_be_bytes(),
        ]
        .concat();
        file.write_all(&data).context("failed to write to WAL file")
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()
            .map_err(|e| anyhow::anyhow!("failed to flush wal from program buffer: {}", e))?;
        file.get_mut()
            .sync_all()
            .map_err(|e| anyhow::anyhow!("failed to flush wal from os buffer: {}", e))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn create_and_read_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        let wal = super::Wal::create(&wal_path).unwrap();
        wal.put(b"key", b"value").unwrap();
        wal.put(b"key2", b"value2").unwrap();
        wal.put(b"key3", b"value3").unwrap();
        drop(wal);

        let skiplist = crossbeam_skiplist::SkipMap::new();
        let _wal = super::Wal::recover(&wal_path, &skiplist).unwrap();
        assert_eq!(skiplist.len(), 3);
        assert!(skiplist.contains_key(b"key".as_ref()));
        assert!(skiplist.contains_key(b"key2".as_ref()));
        assert!(skiplist.contains_key(b"key3".as_ref()));
    }
}
