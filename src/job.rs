
use serde::Deserialize;
use crate::pipeline;

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {    
    pub image_id: String
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    // uuid v4
    pub id: u128,

    pub working_dir: String,
   
    pub schema: Schema,

    pub pipeline: pipeline::Pipeline
}

