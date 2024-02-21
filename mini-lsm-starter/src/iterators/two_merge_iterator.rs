use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    use_iterator: u8, // this can be 0 (use a), 1 (use b), 2 (use c)
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
        println!("ENTER: TwoMergeIterator::decide_which_iter_to_use()");
        if !a.is_valid() && b.is_valid() {
            println!("Using b");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
            return 1;
        }
        if a.is_valid() && !b.is_valid() {
            println!("Using a");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
            return 0;
        }
        if !a.is_valid() && !b.is_valid() {
            println!("Both are invalid");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
            return u8::MAX;
        }
        if a.key() < b.key() {
            println!("Using a");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
            0
        } else if a.key() > b.key() {
            println!("Using b");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
            1
        } else {
            println!("Using both");
            println!("EXIT: TwoMergeIterator::decide_which_iter_to_use()");
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
            println!("Getting value of a");
            return self.a.value();
        }
        println!("Getting value of b");
        println!("Is b valid {}", self.b.is_valid());
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
        println!("ENTER: TwoMergeIterator::next()");
        println!("Using iterator {}", self.use_iterator);
        if self.use_iterator == 0 {
            //  advance the first iterator because the second one wasn't used
            if self.a.is_valid() {
                println!("Advancing iterator a");
                self.a.next()?;
            }
        } else if self.use_iterator == 1 {
            //  advance the second iterator because the first one wasn't used
            if self.b.is_valid() {
                println!("Advancing iterator b");
                self.b.next()?;
            }
        } else if self.use_iterator == 2 {
            //  advance both
            println!("Advancing both");
            if self.a.is_valid() {
                println!("Advancing a");
                self.a.next()?;
            }
            if self.b.is_valid() {
                println!("Advancing b");
                self.b.next()?;
            }
        }
        self.use_iterator = TwoMergeIterator::decide_which_iter_to_use(&self.a, &self.b);
        Ok(())
    }
}
