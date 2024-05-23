use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        //  check the l0 trigger
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot
                    .levels
                    .get(0)
                    .map(|x| x.1.clone())
                    .unwrap_or(Vec::new()),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }
        //  check the adjacent levels ratio trigger
        for (level, sst_ids) in snapshot.levels.iter() {
            if *level == 1 {
                continue;
            }
            let lower_level = level - 1;
            let upper_level = level - 2;
            let denom = snapshot.levels[upper_level].1.len();
            let ratio = if denom != 0 {
                sst_ids.len() / denom
            } else {
                continue;
            };
            if ratio * 100 < self.options.size_ratio_percent {
                //  create a task
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(*level - 1),
                    upper_level_sst_ids: snapshot
                        .levels
                        .get(upper_level)
                        .map(|x| x.1.clone())
                        .unwrap_or(Vec::new()),
                    lower_level: *level,
                    lower_level_sst_ids: snapshot
                        .levels
                        .get(lower_level)
                        .map(|x| x.1.clone())
                        .unwrap_or(Vec::new()),
                    is_lower_level_bottom_level: self.options.max_levels == *level,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot_clone = snapshot.clone();
        match task.upper_level {
            //  Non L0 compaction
            Some(level) => {
                //  remove from the upper level
                snapshot_clone
                    .levels
                    .get_mut(level - 1)
                    .expect(&format!("Level {} not found", level - 1))
                    .1
                    .retain(|&sst_id| !task.upper_level_sst_ids.contains(&sst_id));
                //  remove from the lower level
                snapshot_clone
                    .levels
                    .get_mut(task.lower_level - 1)
                    .expect(&format!("Level {} not found", task.lower_level - 1))
                    .1
                    .retain(|&sst_id| !task.lower_level_sst_ids.contains(&sst_id));
                //  add new files to lower level
                snapshot_clone
                    .levels
                    .get_mut(task.lower_level - 1)
                    .expect(&format!("Level {} not found", task.lower_level - 1))
                    .1
                    .extend(output.iter());
            }
            //  L0 compaction
            None => {
                //  remove from l0
                snapshot_clone
                    .l0_sstables
                    .retain(|&sst_id| !task.upper_level_sst_ids.contains(&sst_id));
                //  remove from l1
                snapshot_clone
                    .levels
                    .get_mut(0)
                    .expect("Level 0 not found")
                    .1
                    .retain(|sst_id| !task.lower_level_sst_ids.contains(sst_id));
                //  add new files to lower level
                snapshot_clone
                    .levels
                    .get_mut(0)
                    .expect("Level 0 not found")
                    .1
                    .extend(output.iter());
            }
        }
        (
            snapshot_clone,
            (task
                .upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .copied()
                .collect::<Vec<usize>>()),
        )
    }
}
