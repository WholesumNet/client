
use serde::Deserialize;
use anyhow;

use crate::recursion;

#[derive(Debug, Deserialize)]
pub struct CriteriaConfig {    
    // minimum ram capacity(in GB) for an offer to be accepted
    pub memory_capacity: Option<u32>,
    
    pub benchmark_expiry_secs: Option<i64>,

    pub benchmark_duration_msecs: Option<u128>,
}

#[derive(Debug, Deserialize)]
pub struct ProveConfig {    
    // the po2 limit
    pub po2: u32,

    pub num_segments: u32,

    // the cid of the segments folder
    pub segments_cid: String,
}

#[derive(Debug, Deserialize)]
pub struct VerificationConfig {
    pub journal_file_path: String,
    // image_id as in risc0, it's a hash digest
    pub image_id: String,
}

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {
    pub title: Option<String>,
    // in seconds
    pub timeout: Option<u32>, 
    
    // criteria for matching
    pub criteria: CriteriaConfig, 

    pub prove: ProveConfig,
    
    pub verification: VerificationConfig,
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,

    pub working_dir: String,
   
    pub schema: Schema,

    pub recursion: recursion::Recursion,
}

