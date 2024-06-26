use core::fmt;
use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};

use crate::key::KeySlice;

use super::StorageIterator;

#[derive(Debug)]
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> fmt::Debug for MergeIterator<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Merge iterator {{ ")?;
        write!(f, "iters: {:?}, current: {:?}", self.iters, self.current)?;
        write!(f, " }} ")
    }
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.len() == 0 {
            return MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();

        //  if none of the iterators are valid, just pick the last one as current
        if iters.iter().all(|iter| !iter.is_valid()) {
            let mut iters = iters;
            return MergeIterator {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                let heap_wrapper = HeapWrapper(index, iter);
                heap.push(heap_wrapper);
            }
        }

        let current = heap.pop().unwrap();
        MergeIterator {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|heap_wrapper| heap_wrapper.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();

        //  Check if there are any keys that are identical - advance the lower ranked iterators in that case
        while let Some(mut heap_wrapper) = self.iters.peek_mut() {
            if heap_wrapper.1.key() == current.1.key() {
                //  The current and the heap top have the same key. Ignore the heap top key because we organised by reverse
                //  chronological order when building the heap. The value in current should be what's upheld. Advance the top
                if let Err(e) = heap_wrapper.1.next() {
                    PeekMut::pop(heap_wrapper);
                    return Err(e);
                }

                if !heap_wrapper.1.is_valid() {
                    PeekMut::pop(heap_wrapper);
                }
            } else {
                break;
            }
        }

        //  advance the current iterator
        current.1.next()?;

        //  check if the current iterator continues to be valid - if not, replace with the top
        if !current.1.is_valid() {
            if let Some(heap_wrapper) = self.iters.pop() {
                self.current = Some(heap_wrapper);
            }
            return Ok(());
        }

        //  check if the current iterator should be replaced by the top value in the heap
        if let Some(mut heap_wrapper) = self.iters.peek_mut() {
            if current < &mut heap_wrapper {
                std::mem::swap(current, &mut *heap_wrapper);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let heap_active_iters: usize = self
            .iters
            .iter()
            .map(|iter| iter.1.num_active_iterators())
            .sum();
        let current_active_iters: usize = self
            .current
            .iter()
            .map(|iter| iter.1.num_active_iterators())
            .sum();
        heap_active_iters + current_active_iters
    }
}
