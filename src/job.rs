
use serde::Deserialize;
use crate::recursion;

#[derive(Debug, Deserialize)]
pub struct ProveConfig {        
    pub num_segments: u32,

    // e.g. "/home/foo/b22000/21"
    pub segment_path: String,

    // e.g. "segment-"
    pub segment_filename_prefix: String,
}

#[derive(Debug, Deserialize)]
pub struct VerificationConfig {
    pub journal_filepath: String,
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

    pub journal: Vec<u8>,
}

