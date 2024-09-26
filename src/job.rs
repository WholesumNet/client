use std::error::Error;
use uuid::Uuid;
use serde::Deserialize;

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
    // min number of independent successful verifications to regard an executoin trace as verified
    pub min_required: Option<u8>,    
}

#[derive(Debug, Deserialize)]
pub struct HarvestConfig {
    // min number of verified traces to consider the whole job as verified and done
    pub min_verified_traces: Option<u8>,    
}

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {
    pub title: Option<String>,
    pub timeout: Option<u32>, // in seconds
    
    pub criteria: CriteriaConfig, // criteria for matching
    
    pub compute: ComputeConfig,
    
    pub verification: VerificationConfig,
    
    pub harvest: HarvestConfig,
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
pub fn get_residue_path() -> Result<String, Box<dyn Error>> {
    let home_dir = home::home_dir()
        .ok_or_else(|| Box::<dyn Error>::from("Home dir is not available."))?
        .into_os_string().into_string()
        .or_else(|_| Err(Box::<dyn Error>::from("OS_String conversion failed.")))?;
    Ok(format!("{home_dir}/.wholesum/jobs"))
}
