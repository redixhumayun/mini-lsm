use std::{sync::Arc, thread};

use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

use super::harness::{check_compaction_ratio, compaction_bench};

#[test]
fn test_integration() {
    println!("Running week 2 day 4 test");
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(
        &dir,
        LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(
            LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                level_size_multiplier: 2,
                base_level_size_mb: 1,
                max_levels: 4,
            },
        )),
    )
    .unwrap();

    compaction_bench(storage.clone());
    check_compaction_ratio(storage.clone());
}
