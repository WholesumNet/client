
use serde::Deserialize;
use crate::pipeline;

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {    
    pub redis_url: String,

    pub image_id: String
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,

    pub working_dir: String,
   
    pub schema: Schema,

    pub pipeline: pipeline::Pipeline
}

