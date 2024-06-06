use core::fmt;

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    use_iterator: u8, // this can be 0 (use a), 1 (use b), 2 (use both)
}

impl<A: StorageIterator, B: StorageIterator> fmt::Debug for TwoMergeIterator<A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TwoMergeIterator {{ a: {:?}, b: {:?} }}", self.a, self.b)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let use_iterator = TwoMergeIterator::decide_which_iter_to_use(&a, &b);
        Ok(TwoMergeIterator { a, b, use_iterator })
    }

    fn decide_which_iter_to_use(a: &A, b: &B) -> u8 {
        if !a.is_valid() && b.is_valid() {
            return 1;
        }
        if a.is_valid() && !b.is_valid() {
            return 0;
        }
        if !a.is_valid() && !b.is_valid() {
            return u8::MAX;
        }
        if a.key() < b.key() {
            0
        } else if a.key() > b.key() {
            1
        } else {
            2
        }
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_iterator == 0 || self.use_iterator == 2 {
            return self.a.key();
        }
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        if self.use_iterator == 0 || self.use_iterator == 2 {
            return self.a.value();
        }
        self.b.value()
    }

    fn is_valid(&self) -> bool {
        if self.use_iterator == u8::MAX {
            false
        } else if self.use_iterator == 0 {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_iterator == 0 {
            //  advance the first iterator because the second one wasn't used
            if self.a.is_valid() {
                self.a.next()?;
            }
        } else if self.use_iterator == 1 {
            //  advance the second iterator because the first one wasn't used
            if self.b.is_valid() {
                self.b.next()?;
            }
        } else if self.use_iterator == 2 {
            //  advance both
            if self.a.is_valid() {
                self.a.next()?;
            }
            if self.b.is_valid() {
                self.b.next()?;
            }
        }
        self.use_iterator = TwoMergeIterator::decide_which_iter_to_use(&self.a, &self.b);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
