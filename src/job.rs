use uuid::Uuid;
use serde::Deserialize;
use anyhow;

#[derive(Debug, Deserialize)]
pub struct CriteriaConfig {    
    // minimum ram capacity(in GB) for an offer to be accepted
    pub memory_capacity: Option<u32>,
    
    pub benchmark_expiry_secs: Option<i64>,

    pub benchmark_duration_msecs: Option<u128>,
}

#[derive(Debug, Deserialize)]
pub struct ComputeConfig {
    // path on disk; it should contain 000.seg files
    pub segments_path: String,
}

#[derive(Debug, Deserialize)]
pub struct VerificationConfig {
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

    pub compute: ComputeConfig,
    
    pub verification: VerificationConfig,
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
   
    pub schema: Schema,
}

impl Job {
    pub fn new (custom_id: Option<String>, schema: Schema) -> Job {
        Job {                  
            id: custom_id.unwrap_or_else(|| {
                //@ use safer id generation methods              
                Uuid::new_v4().simple().to_string()[..4].to_string()
            }),
            schema: schema,
        }
    }
}

// get base residue path of the host
pub fn get_residue_path() -> anyhow::Result<String> {
    let err_msg = "Home dir is not available";
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}
