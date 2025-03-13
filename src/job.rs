
use serde::Deserialize;
use crate::recursion;

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
    // image_id as in risc0, it's a hash digest
    pub image_id: String,
}

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {    
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

