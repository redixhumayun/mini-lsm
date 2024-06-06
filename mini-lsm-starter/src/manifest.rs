use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

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
        let stream = serde_json::Deserializer::from_reader(reader).into_iter::<ManifestRecord>();

        let mut records = Vec::new();
        for result in stream {
            let record =
                result.map_err(|e| anyhow::anyhow!("failed to read manifest file: {:?}", e))?;
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
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
        let mut file = self.file.lock();
        file.write_all(&data)
            .map_err(|e| anyhow::anyhow!("failed to write to manifest file: {:?}", e))?;
        file.sync_all()?;
        Ok(())
    }
}
