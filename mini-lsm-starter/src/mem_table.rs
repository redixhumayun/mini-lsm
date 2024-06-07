#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use core::fmt;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

impl fmt::Debug for MemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let first_entry = self.map.front();
        let last_entry = self.map.back();

        match (first_entry, last_entry) {
            (Some(first), Some(last)) => {
                let first_key = String::from_utf8_lossy(first.key()).into_owned();
                let first_value = String::from_utf8_lossy(first.value());
                let last_key = String::from_utf8_lossy(last.key()).into_owned();
                let last_value = String::from_utf8_lossy(last.value());

                let trimmed_first_value = trim_to_last_4(&first_value);
                let trimmed_last_value = trim_to_last_4(&last_value);

                write!(
                    f,
                    "memtable id: {}, key range: {} -> {} | Value range: {} -> {}",
                    self.id(),
                    first_key,
                    last_key,
                    trimmed_first_value,
                    trimmed_last_value
                )
            }
            _ => {
                write!(f, "Memtable empty")
            }
        }
    }
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Option::None,
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(path)?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let skiplist: SkipMap<Bytes, Bytes> = SkipMap::new();
        let wal = Wal::recover(path, &skiplist)?;
        Ok(MemTable {
            map: Arc::new(skiplist),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(key)
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(lower, upper)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self.map.get(key) {
            Some(entry) => Option::Some(entry.value().clone()),
            None => Option::None,
        }
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.map
            .insert(Bytes::from(key.to_vec()), Bytes::from(value.to_vec()));
        let current_size = self
            .approximate_size
            .load(std::sync::atomic::Ordering::Relaxed);
        let key_value_size = key.len() + value.len();
        let new_size = current_size + key_value_size;
        self.approximate_size
            .store(new_size, std::sync::atomic::Ordering::Relaxed);
        if let Some(wal) = self.wal.as_ref() {
            wal.put(key, value)?;
        }
        self.sync_wal()?; //  TODO: Syncing wal on every put will probably hurt throughput significantly?
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> MemTableIterator {
        let mut iterator = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_bound(_lower), map_bound(_upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        iterator.next().unwrap();
        iterator
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(
                Key::from_bytes(entry.key().clone()).as_key_slice(),
                entry.value(),
            );
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl fmt::Debug for MemTableIterator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let first_key = self
            .with_map(|map| map.front())
            .map(|entry| String::from_utf8_lossy(entry.key().as_ref()).into_owned());
        let last_key = self
            .with_map(|map| map.back())
            .map(|entry| String::from_utf8_lossy(entry.key().as_ref()).into_owned());
        match (first_key, last_key) {
            (Some(first), Some(last)) => {
                let key = self.with_item(|item| String::from_utf8_lossy(&item.0));
                let value = self.with_item(|item| String::from_utf8_lossy(&item.1));
                let trimmed_value = trim_to_last_4(&value);
                write!(f, "Memtable Iterator {{ ")?;
                write!(f, "Key range {} -> {}, ", first, last)?;
                write!(f, "Key: {}, Value: {}", key, trimmed_value)?;
                write!(f, " }} ")
            }
            _ => {
                write!(f, "Memtable empty")
            }
        }
    }
}

fn trim_to_last_4(value: &str) -> String {
    if value.len() <= 4 {
        value.to_string()
    } else {
        value[value.len() - 4..].to_string()
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.with_item(|item| &item.1)
    }

    fn key(&self) -> KeySlice {
        let key = self.with_item(|item| &item.0);
        Key::from_slice(key)
    }

    fn is_valid(&self) -> bool {
        !self.with_item(|item| item.0.is_empty())
    }

    fn next(&mut self) -> Result<()> {
        let next_entry = self.with_iter_mut(|iter| {
            iter.next()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
        });
        self.with_item_mut(|item| *item = next_entry);
        Ok(())
    }
}
