# Introduction

This is my implementation of the mini LSM in a week tutorial. I am trying to write answers to all the discussion questions for each day's topic

##  Week 1 Day 1

* Why doesn't the memtable provide a delete API? **Put with empty value serves the purpose**

* Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist? Unsure

* Why do we need a combination of state and state_lock? Can we only use state.read() and state.write()? For this day's implementation, using just state.read() and state.write() should work. **state.write() will wait for all reader threads to flush before acquiring the write lock on the RWLock so should serve the same purpose**

* Why does the order to store and to probe the memtables matter? If a key appears in multiple memtables, which version should you return to the user? **Memtables are reverse chronological order so order matters. This is the order of the query as well.**

* Is the memory layout of the memtable efficient / does it have good data locality? (Think of how Byte is implemented and stored in the skiplist...) What are the possible optimizations to make the memtable more efficient? **Skiplists inherently have poor data locality because of their structure. You have nodes being allocated and links being set up between them dynamically. Using an arena allocator can minimise this slightly, however cannot eliminate it. Byte is a smart pointer so layer of indirection involved here.**

* So we are using parking_lot locks in this tutorial. Is its read-write lock a fair lock? What might happen to the readers trying to acquire the lock if there is one writer waiting for existing readers to stop? **read-write lock is not fair by default but API is provided to make it far. In a fair lock, its FIFO via queue. Otherwise depends on implementation, preference might be given to thread that just released a lock to re-acquire it** 

* After freezing the memtable, is it possible that some threads still hold the old LSM state and wrote into these immutable memtables? How does your solution prevent it from happening? **Writing into immutable memtables is not possible because the write() lock is acquired before memtable is made immutable**

* There are several places that you might first acquire a read lock on state, then drop it and acquire a write lock (these two operations might be in different functions but they happened sequentially due to one function calls the other). How does it differ from directly upgrading the read lock to a write lock? Is it necessary to upgrade instead of acquiring and dropping and what is the cost of doing the upgrade? **Dropping and upgrading the lock requires re-checking the state because underlying structure might have changed. Unsure of how to do the upgrade.**

##  Week 1 Day 2

* What is the time/space complexity of using your merge iterator?
O(n * log(n)) to construct the priority queue for n memtable. O(m * log(n)) to consume the queue assuming m elements per memtable and n memtables. 
The second part is because each call to `next()` consumes the merge iterator and it must do so for each of the m elements in each of the n memtables. 
What would the worst case theoretical complexity be? It would be O(m * n * log (n)) because the `next()` function for the merge iterator will try to compare each element against each iterator in the queue because each iterator contains a duplicate of the element.

* Why do we need a self-referential structure for memtable iterator?
Unsure

* If a key is removed (there is a delete tombstone), do you need to return it to the user? Where did you handle this logic?
No, this is handled in the `skip_deleted_values()` of the `lsm_iterator` where the deleted tombstones are skipped.

* If a key has multiple versions, will the user see all of them? Where did you handle this logic?
No, the user will see the latest version. This is handled in the `next()` function of the `merge_iterator`.

* If we want to get rid of self-referential structure and have a lifetime on the memtable iterator (i.e., MemtableIterator<'a>, where 'a = memtable or LsmStorageInner lifetime), is it still possible to implement the scan functionality?
Unsure

* What happens if (1) we create an iterator on the skiplist memtable (2) someone inserts new keys into the memtable (3) will the iterator see the new key?
Depends if they inserted it before the call to `SkipMapRangeIter` in `mem_table.rs` to construct the range iterator for a memtable. Presumably crossbeam takes a snapshot of the skiplist at this point so no new values will appear after.

* What happens if your key comparator cannot give the binary heap implementation a stable order?
Unsure what stable order means here.

* Why do we need to ensure the merge iterator returns data in the iterator construction order?
To get the latest version of a value first.

* Is it possible to implement a Rust-style iterator (i.e., next(&self) -> (Key, Value)) for LSM iterators? What are the pros/cons?
Unsure.

* The scan interface is like fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>). How to make this API compatible with Rust-style range (i.e., key_a..key_b)? If you implement this, try to pass a full range .. to the interface and see what will happen.
Unsure.

* The starter code provides the merge iterator interface to store Box<I> instead of I. What might be the reason behind that?
Probably has something to do with dynamic dispatch? Also, i don't believe Rust would allow storing a vector of type `I` where `I` is a trait since the size of the object is unknown at compile time. `Box<I>` would allocate memory on the heap so size doesn't need to be known at compile time.

##  Week 1 Day 3
* What is the time complexity of seeking a key in the block? 
Should be O(log(n)) because binary search

* Where does the cursor stop when you seek a non-existent key in your implementation? 
The cursor stops at 0

* So Block is simply a vector of raw data and a vector of offsets. Can we change them to Byte and Arc<[u16]>, and change all the iterator interfaces to return Byte instead of &[u8]? (Assume that we use Byte::slice to return a slice of the block without copying.) What are the pros/cons?
The only thing I've been able to think of for this is that a slice requires the underlying object to be maintained in memory manually but with Arc and Byte, that will be handled a little more easily. Also, Arc and Byte are thread-safe. 

* What is the endian of the numbers written into the blocks in your implementation?
Little endian. No reason just picked one.

* Is your implementation prune to a maliciously-built block? Will there be invalid memory access, or OOMs, if a user deliberately construct an invalid block?
How can a user construct an invalid block? 

* Can a block contain duplicated keys?
Yes, there is no safe-guard against that currently. The user is free to add whatever keys they like.

* What happens if the user adds a key larger than the target block size?
The current implementation stores the key value pair within the block anyway. This will probably lead to page fragmentation on disk.

* Consider the case that the LSM engine is built on object store services (S3). How would you optimize/change the block format and parameters to make it suitable for such services?
This is an interesting question. If my blocks were to live on S3 perhaps the most important thing is reducing the number of network round trips to optimise cost which would presumably imply increasing the size of a block. Perhaps pushing some of the computation of the range scanning into S3 itself? Compress the block storage. Keep a cache on the compute node with "hot" blocks? 


* Do you love bubble tea? Why or why not?
Lol love it

##  Week 1 Day 4

* What is the time complexity of seeking a key in the SST?
Binary search so should be O(log(n*m)) where n is the number of individual blocks in one SST table and m is the number of entries within that block that need to be binary searched over.

* Where does the cursor stop when you seek a non-existent key in your implementation?
When looking for a key 3 in the blocks with key ranges (1, 2) and (4, 5) it will stop at block 2.

* Is it possible (or necessary) to do in-place updates of SST files?
SST files are supposed to be immutable so I don't think this would be correct.

* An SST is usually large (i.e., 256MB). In this case, the cost of copying/expanding the Vec would be significant. Does your implementation allocate enough space for your SST builder in advance? How did you implement it?
I don't think I preallocated any space for the vector. I just used `Vec::new()` and let dynamic allocation do the job for me.

* Looking at the moka block cache, why does it return Arc<Error> instead of the original Error?
Is this because moka is a concurrent cache so it might potentially need to return an error from a separate thread?

* Does the usage of a block cache guarantee that there will be at most a fixed number of blocks in memory? For example, if you have a moka block cache of 4GB and block size of 4KB, will there be more than 4GB/4KB number of blocks in memory at the same time?
There cannot be because the cache limits the number of blocks that can be stored and each block is of an equal size. Of course the last block in an SSTable might be smaller than the maximum allowable limit so in that case the cache would hold more blocks potentially.

* Is it possible to store columnar data (i.e., a table of 100 integer columns) in an LSM engine? Is the current SST format still a good choice?
Yes, columnar data can be stored. The current format might still work assuming that each column is stored in a separate SSTable. The key could be the primary key and the value would be the columnar value required there.

* Consider the case that the LSM engine is built on object store services (i.e., S3). How would you optimize/change the SST format/parameters and the block cache to make it suitable for such services?
Obvious answer - compress the SSTable for storage and increase the size of the SSTable file. Presumably the cache would be placed on the compute node to ensure that network requests are limited.

##  Week 1 Day 5

* Consider the case that a user has an iterator that iterates the whole storage engine, and the storage engine is 1TB large, so that it takes ~1 hour to scan all the data. What would be the problems if the user does so? (This is a good question and we will ask it several times at different points of the tutorial...)
Apart from increased latency because processing power is being consumed by the iteration, the only other thing I could think of is that it is unlikely the iterator would be able to load all data into memory at once. This would require repeated random disk reads and flushes. 

One other thing Alex mentioned on the Discord is "another aspect to think of is how much disk space would it take if we have such an iterator and the user is continuously writing new data/deleting keys or overwriting keys? Think of the case that we have 1TB of the data, create the iterator, and and the user overwrites 1TB data in the next hour. The original 1TB data cannot be released as the iterators are still using the data. So basically having a long-running iterator would cause space amplification as well as performance regressions (in week3 MVCC)"

##  Week 1 Day 6

* What happens if a user requests to delete a key twice?
The key value pair should be set as `{key}{""}` but the user should see no effect apart from getting a null value when attempting to fetch the key
* How much memory (or number of blocks) will be loaded into memory at the same time when the iterator is initialized?
This would just be a function of the number of iterators that passed the filter test

##  Week 1 Day 7

* How does the bloom filter help with the SST filtering process? What kind of information can it tell you about a key? (may not exist/may exist/must exist/must not exist)
Tells you with 100% guarantee if the key doesn't exist.
* Consider the case that we need a backward iterator. Does our key compression affect backward iterators?
It shouldn't. Since the is being uncompressed based on the first key, it should still work just fine. Just have to ensure the first key (in this case the last key is loaded)
* Can you use bloom filters on scan?
Yes, you can. Each block has a bloom filter so check if the block contains the key within the range you want. Although, it wouldn't be a big performance benefit over a range overlap check since its a single comparison on each block anyway.
* What might be the pros/cons of doing key-prefix encoding over adjacent keys instead of with the first key in the block?
Pros:
- Compression is more likely because keys are typically stored in sorted order so the chances of overlap are higher
Cons:
- More complex to implement and maintain

##  Week 2 Day 1

* What are the definitions of read/write/space amplifications? (This is covered in the overview chapter)
Read amplification -> Number of I/O requests per get operation (for instance, if there are 100 SST's and the key is stored on the last SST, there are 100 I/O operations to get there) => I/O requests / get operation

Write amplification -> Ratio of memtables flushed to disk versus total data written to disk => Total data written / number of memtables written. For example, if every SST flush requires a compaction, then the total data written to disk each time will be high because the older SST's will be merged and rewritten into a new SST with the flushed SST.

Space amplification -> Ratio of actual space used by the engine to the data stored by the user (actual rows or key value pairs). A quick way to estimate this is to divide the full storage file size by the last level size with the assumption that the last level contains a snapshot of the user data and the upper levels contain changes.

* What are the ways to accurately compute the read/write/space amplifications, and what are the ways to estimate them?
Answered above

* Is it correct that a key will take some storage space even if a user requests to delete it?
Yes, until it is compacted away

* Paper to read
https://www.usenix.org/conference/atc19/presentation/balmau

* Is it a good idea to use/fill the block cache for compactions? Or is it better to fully bypass the block cache when compaction?
Unsure

* Does it make sense to have a struct ConcatIterator<I: StorageIterator> in the system?
Please no more abstractions and structs

* Some researchers/engineers propose to offload compaction to a remote server or a serverless lambda function. What are the benefits, and what might be the potential challenges and performance impacts of doing remote compaction? (Think of the point when a compaction completes and what happens to the block cache on the next read request...)
Answer by Alex on Discord

The problem is after compaction and the new list of SST files are propagated to the local engine, the reads will now go through the new files, and they are not on the local disk / in the block cache, so it will create a sudden spike in latency to download them from S3 or whatever other places. While in local compaction, the new files are on the disk, so the reads won't be impacted much.

##  Week 2 Day 2

* What is the estimated write amplification of leveled compaction?
The estimated write amp = N (the number of levels in the tree). Using the formula -> total data written to disk / number of memtables flushed, for every memtable flush, the sstable must be compacted and move through all the levels before reaching the final level

* What is the estimated read amplification of leveled compaction?
The estimate read amp = N (the number of levels in the tree). Using the formula -> number of I/O operations per get request. Imagine a tree with 4 levels and the point query being satisifed by the last SSTable in the last level. The query must look through all levels in this case.

* Is it correct that a key will only be purged from the LSM tree if the user requests to delete it and it has been compacted in the bottom-most level?
Not necessarily, the very first compaction on 2 SSTables (one of which contains the key and the other deletes it) will result in a compacted SSTable which does not contain the key.

* Is it a good strategy to periodically do a full compaction on the LSM tree? Why or why not?
No, because this will unnecessarily increase the write amp of the system.

* Actively choosing some old files/levels to compact even if they do not violate the level amplifier would be a good choice, is it true? (Look at the Lethe paper!)
Unsure. Need to read paper

* If the storage device can achieve a sustainable 1GB/s write throughput and the write amplification of the LSM tree is 10x, how much throughput can the user get from the LSM key-value interfaces?
Effective Throughput= Write Amplification / Sustainable Write Throughput
​Effective Throughput = 1 GB/s / 10 = 0.1 GB/s = 100 MBPs

* Can you merge L1 and L3 directly if there are SST files in L2? Does it still produce correct result?
No, it might not because the order of writes might change. For instance if there is a delete that is present in L1 for a key that was written in L2 but L1 and L3 are compacted and
the result written to the level of L3, then L2 will be encountered first during a get query and the key will still be returned even though a delete request was sent by the client.

* So far, we have assumed that our SST files use a monotonically increasing id as the file name. Is it okay to use <level>_<begin_key>_<end_key>.sst as the SST file name? What might be the potential problems with that? (You can ask yourself the same question in week 3...)
What if more than 1 SST at L0 has the same begin and end key because the overlapping ranges haven't been merged yet?

##  Week 2 Day 4
* What is the estimated write amplification of leveled compaction?
The write amplification in the worst case is N (number of levels) because it needs to be compacted through all the levels

* What is the estimated read amplification of leveled compaction?
The read amplification in the worst case is N (number of levels) because the entry might be in the last SST in the last level.

* What happens if compaction speed cannot keep up with the SST flushes?
Typically, the system would experience higher space and read amp. Space amp because more space is taken at the higher levels. Read amp because more SST's would be created and not compacted (yet) which need to be searched through.

* Consider the case that the upper level has two tables of [100, 200], [201, 300] and the lower level has [50, 150], [151, 250], [251, 350]. In this case, do you still want to compact one file in the upper level at a time? Why?
No, it makes sense to compact all the files together at once because otherwise only the first file from the top level will be compacted with the first two files from the bottom level. This will create the following

```
upper -> [201,300]
lower -> [50, 250], [251, 300]
```

and now when running compaction again, all 3 files will need to be compacted. This increases the write amplification. Instead, it would be better to compact everything at once.

##  Week 2 Day 5
* When do you need to call fsync? Why do you need to fsync the directory?
When we want to ensure that nothing is cached in the OS buffer and writes are actually flushed to disk. The directory needs to be fsynced for this reason.

* What are the places you will need to write to the manifest?
Anywhere where the state is changing so that the entire state can be rebuilt by running through the manifest

* Consider an alternative implementation of an LSM engine that does not use a manifest file. Instead, it records the level/tier information in the header of each file, scans the storage directory every time it restarts, and recover the LSM state solely from the files present in the directory. Is it possible to correctly maintain the LSM state in this implementation and what might be the problems/challenges with that?
Yes, it should be possible.
One problem is that each SST file needs to be constantly updated with the level of the file so additional read & write.

* Currently, we create all SST/concat iterators before creating the merge iterator, which means that we have to load the first block of the first SST in all levels into memory before starting the scanning process. We have start/end key in the manifest, and is it possible to leverage this information to delay the loading of the data blocks and make the time to return the first key-value pair faster?
If we have the first/end key in the manifest for each SST, then we can only load the relevant ones when doing a scan by doing a range overlap check.

* Is it possible not to store the tier/level information in the manifest? i.e., we only store the list of SSTs we have in the manifest without the level information, and rebuild the tier/level using the key range and timestamp information (SST metadata).
Yes, this should be possible because the timestamp will tell us the chronological order of the SST's and the key range will tell us in which order the SST's should be stored at a level.

So, we could do something simple like start reading the oldest SST's and storing them at the bottom levels by order of first key and keep going until the level is full and then do that for every level above (but how do we know when a level is full? based on the target size and actual size calculations).

##  Week 2 Day 6
* When can you tell the user that their modifications (put/delete) have been persisted?
In my implementation, I am calling `sync_wal` on every `put` operation but that is probably incorrect. Typically, the `sync_wal` operation will be called once every x milliseconds and that is what will provide a guarantee to the user that the data has been persisted.

Alternatively, the `sync` operation on the engine provides the same functionality. So, whenever `sync` is called is when the data has been persisted.

* How can you handle corrupted data in WAL?
Depends what we mean by corrupted data. Does that mean partial writes? Does that mean malformed writes to the file? 

I know of the ARIES protocol for WAL recovery, I suppose that could work? Or add a checksum to the WAL (for every write maybe?) and use the checksum to determine if the WAL has been corrupted. If the WAL was corrupted, bail out of restoring a memtable from it.


![banner](./mini-lsm-book/src/mini-lsm-logo.png)

# LSM in a Week

[![CI (main)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml/badge.svg)](https://github.com/skyzh/mini-lsm/actions/workflows/main.yml)

Build a simple key-value storage engine in a week! And extend your LSM engine on the second + third week.

## [Tutorial](https://skyzh.github.io/mini-lsm)

The Mini-LSM book is available at [https://skyzh.github.io/mini-lsm](https://skyzh.github.io/mini-lsm). You may follow this guide and implement the Mini-LSM storage engine. We have 3 weeks (parts) of the tutorial, each of them consists of 7 days (chapters).

## Community

You may join skyzh's Discord server and study with the mini-lsm community.

[![Join skyzh's Discord Server](https://dcbadge.vercel.app/api/server/ZgXzxpua3H)](https://skyzh.dev/join/discord)

**Add Your Solution**

If you finished at least one full week of this tutorial, you can add your solution to the community solution list at [SOLUTIONS.md](./SOLUTIONS.md). You can submit a pull request and we might do a quick review of your code in return of your hard work.

## Development

**For Students**

You should modify code in `mini-lsm-starter` directory.

```
cargo x install-tools
cargo x copy-test --week 1 --day 1
cargo x scheck
cargo run --bin mini-lsm-cli
cargo run --bin compaction-simulator
```

**For Course Developers**

You should modify `mini-lsm` and `mini-lsm-mvcc`

```
cargo x install-tools
cargo x check
cargo x book
```

If you changed public API in the reference solution, you might also need to synchronize it to the starter crate.
To do this, use `cargo x sync`.

## Code Structure

* mini-lsm: the final solution code for <= week 2
* mini-lsm-mvcc: the final solution code for week 3 MVCC
* mini-lsm-starter: the starter code
* mini-lsm-book: the tutorial

We have another repo mini-lsm-solution-checkpoint at [https://github.com/skyzh/mini-lsm-solution-checkpoint](https://github.com/skyzh/mini-lsm-solution-checkpoint). In this repo, each commit corresponds to a chapter in the tutorial. We will not update the solution checkpoint very often.

## Demo

You can run the reference solution by yourself to gain an overview of the system before you start.

```
cargo run --bin mini-lsm-cli-ref
cargo run --bin mini-lsm-cli-mvcc-ref
```

And we have a compaction simulator to experiment with your compaction algorithm implementation,

```
cargo run --bin compaction-simulator-ref
cargo run --bin compaction-simulator-mvcc-ref
```

## Tutorial Structure

We have 3 weeks + 1 extra week (in progress) for this tutorial.

* Week 1: Storage Format + Engine Skeleton
* Week 2: Compaction and Persistence
* Week 3: Multi-Version Concurrency Control
* The Extra Week / Rest of Your Life: Optimizations (unlikely to be available in 2024...)

![Tutorial Roadmap](./mini-lsm-book/src/lsm-tutorial/00-full-overview.svg)

| Week + Chapter | Topic                                                       |
| -------------- | ----------------------------------------------------------- |
| 1.1            | Memtable                                                    |
| 1.2            | Merge Iterator                                              |
| 1.3            | Block                                                       |
| 1.4            | Sorted String Table (SST)                                   |
| 1.5            | Read Path                                                   |
| 1.6            | Write Path                                                  |
| 1.7            | SST Optimizations: Prefix Key Encoding + Bloom Filters      |
| 2.1            | Compaction Implementation                                   |
| 2.2            | Simple Compaction Strategy (Traditional Leveled Compaction) |
| 2.3            | Tiered Compaction Strategy (RocksDB Universal Compaction)   |
| 2.4            | Leveled Compaction Strategy (RocksDB Leveled Compaction)    |
| 2.5            | Manifest                                                    |
| 2.6            | Write-Ahead Log (WAL)                                       |
| 2.7            | Batch Write and Checksums                                   |
| 3.1            | Timestamp Key Encoding                                      |
| 3.2            | Snapshot Read - Memtables and Timestamps                    |
| 3.3            | Snapshot Read - Transaction API                             |
| 3.4            | Watermark and Garbage Collection                            |
| 3.5            | Transactions and Optimistic Concurrency Control             |
| 3.6            | Serializable Snapshot Isolation                             |
| 3.7            | Compaction Filters                                          |

## License

The Mini-LSM starter code and solution are under [Apache 2.0 license](LICENSE). The author reserves the full copyright of the tutorial materials (markdown files and figures).
