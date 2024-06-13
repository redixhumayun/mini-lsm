use core::fmt;
use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl fmt::Debug for Watermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Watermark {{ readers: {:?} }}", self.readers)
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        if let Some(count) = self.readers.get_mut(&ts) {
            assert!(*count > 0);
            *count -= 1;
            if *count == 0 {
                self.readers.remove(&ts);
            }
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().min().copied()
    }

    pub fn largest_read_ts(&self) -> Option<u64> {
        self.readers.keys().max().copied()
    }
}
