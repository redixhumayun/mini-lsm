use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

const CHECKSUM_LENGTH: usize = 4;

#[derive(Serialize, Deserialize)]
pub struct ManifestRecordWrapper {
    len: u64,
    record: Vec<u8>,
    checksum: u32,
}

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .create_new(true)
            .write(true)
            .open(path)
            .context("failed to create manifest file")?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open manifest file")?;
        let reader = BufReader::new(&file);
        let stream =
            serde_json::Deserializer::from_reader(reader).into_iter::<ManifestRecordWrapper>();

        let mut records = Vec::new();
        for result in stream {
            let record =
                result.map_err(|e| anyhow::anyhow!("failed to read manifest file: {:?}", e))?;
            records.push(record);
        }

        let deserialized_records = records
            .iter()
            .map(|record_wrapper| {
                let data = &record_wrapper.record;
                let stored_checksum = record_wrapper.checksum;
                let computed_checksum = crc32fast::hash(&data);
                if stored_checksum != computed_checksum {
                    return Err(anyhow::anyhow!("checksum comparison failed while reading manifest. stored -> {}, computed -> {}", stored_checksum, computed_checksum));
                }
                let record = serde_json::from_slice::<ManifestRecord>(&data)?;
                Ok(record)
            })
            .collect::<Result<Vec<ManifestRecord>>>()?;

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            deserialized_records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let data = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&data);
        let record_length = data.len() + CHECKSUM_LENGTH;
        let record_wrapper = ManifestRecordWrapper {
            len: record_length
                .try_into()
                .expect("record length is too large while writing manifest"),
            record: data,
            checksum,
        };
        let record_bytes = serde_json::to_vec(&record_wrapper)?;
        let mut file = self.file.lock();
        file.write_all(&record_bytes)
            .map_err(|e| anyhow::anyhow!("failed to write to manifest file: {:?}", e))?;
        file.sync_all()?;
        Ok(())
    }
}
