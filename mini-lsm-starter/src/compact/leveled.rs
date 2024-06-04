use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{lsm_storage::LsmStorageState, table::SsTable};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

#[derive(Debug)]
struct LevelSizes {
    actual_sizes: Vec<u64>,
    target_sizes: Vec<u64>,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn get_level_sizes(&self, snapshot: &LsmStorageState) -> LevelSizes {
        //  calculate the actual sizes of all levels
        let actual_sizes: Vec<u64> = snapshot
            .levels
            .iter()
            .map(|(_, sst_ids)| {
                let ssts: Vec<&Arc<SsTable>> = sst_ids
                    .iter()
                    .map(|sst_id| snapshot.sstables.get(sst_id).unwrap())
                    .collect();
                let size: u64 = ssts.iter().map(|sst| sst.table_size()).sum();
                size
            })
            .collect();
        let base_level_actual_size = *actual_sizes.last().unwrap();
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        //  calculate the target size for each level
        let mut target_sizes = vec![0; snapshot.levels.len()];
        if let Some(last) = target_sizes.last_mut() {
            *last = base_level_size_bytes as u64;
        }
        if base_level_actual_size <= base_level_size_bytes.try_into().unwrap() {
            return LevelSizes {
                actual_sizes,
                target_sizes,
            };
        }
        for i in (0..target_sizes.len() - 2).rev() {
            let target_size = target_sizes[i + 1] / self.options.level_size_multiplier as u64;
            target_sizes[i] = target_size;
            if target_size < self.options.base_level_size_mb.try_into().unwrap() {
                break;
            }
        }
        LevelSizes {
            actual_sizes,
            target_sizes,
        }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        //  Get the actual size and target size for each level
        let level_sizes = self.get_level_sizes(snapshot);

        //  Decide on the base level
        //  Iterate over each level and pick the first level with a target size greater than 0
        let base_level = {
            let mut result = 0;
            for (index, level_target_size) in level_sizes.target_sizes.iter().enumerate() {
                if *level_target_size > 0 {
                    result = index + 1;
                    break;
                }
            }
            result
        };

        if base_level == 0 {
            println!("could not find any level with a target size > 0");
            return None;
        }

        /*
        Decide what level needs to be compacted
        First, check the L0 level
        Next, check the remaining levels
        */
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            let overlapping_ssts =
                self.find_overlapping_ssts(snapshot, &snapshot.l0_sstables, base_level - 1);
            println!("The overlapping ssts are {:?}", overlapping_ssts);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: overlapping_ssts,
                is_lower_level_bottom_level: snapshot.levels.len() == base_level,
            });
        }

        //  check which level has the highest ratio and compact that
        let mut max = (0, 0); //  level will be 1 based when stored here
        for (level, _) in &snapshot.levels {
            let denom = level_sizes.target_sizes[level - 1];
            if denom == 0 {
                continue;
            }
            let ratio = level_sizes.actual_sizes[level - 1] / denom;
            if ratio > max.1 {
                max = (*level, ratio);
            }
        }

        if max.0 == 0 {
            println!("None of the levels need to be compacted because all their ratios are 0");
            return None;
        }

        if max.0 == base_level {
            println!("The base level cannot be compacted against itself");
            return None;
        }

        //  find overlapping tables to compact
        let level_to_compact = max.0;
        let oldest_sst_id = snapshot
            .levels
            .get(level_to_compact - 1)
            .unwrap()
            .1
            .iter()
            .min()
            .unwrap();
        let overlapping_ssts =
            self.find_overlapping_ssts(snapshot, &[*oldest_sst_id], base_level - 1);

        //  create and return the task
        return Some(LeveledCompactionTask {
            upper_level: Some(level_to_compact),
            upper_level_sst_ids: vec![*oldest_sst_id],
            lower_level: base_level,
            lower_level_sst_ids: overlapping_ssts,
            is_lower_level_bottom_level: snapshot.levels.len() == base_level,
        });
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
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
                    .expect(&format!("level {} not found", level - 1))
                    .1
                    .retain(|&sst_id| !task.upper_level_sst_ids.contains(&sst_id));
            }
            //  L0 compaction
            None => {
                //  remove from l0
                snapshot_clone
                    .l0_sstables
                    .retain(|&sst_id| !task.upper_level_sst_ids.contains(&sst_id));
            }
        };
        //  get reference to lower level
        let lower_level = snapshot_clone
            .levels
            .get_mut(task.lower_level - 1)
            .expect(&format!("level {} not found", task.lower_level - 1));
        //  remove from the lower level
        lower_level
            .1
            .retain(|&sst_id| !task.lower_level_sst_ids.contains(&sst_id));
        //  add new ids to lower level
        lower_level.1.extend(output.iter());
        //  sort the lower level
        lower_level.1.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });
        snapshot_clone.levels[task.lower_level - 1] = lower_level.clone();
        (
            snapshot_clone,
            task.upper_level_sst_ids
                .iter()
                .chain(task.lower_level_sst_ids.iter())
                .copied()
                .collect::<Vec<usize>>(),
        )
    }
}
