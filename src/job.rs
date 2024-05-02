use uuid::Uuid;
use std::collections::{
    HashMap, HashSet
};
use serde::Deserialize;

use comms::{
    compute,
};

#[derive(Debug, Deserialize)]
pub struct CriteriaConfig {    
    // minimum ram capacity(in GB) for an offer to be accepted
    pub min_memory_capacity: Option<u32>,

    pub benchmark_duration_secs: Option<u32>,

    pub benchmark_expiry_secs: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ComputeConfig {
    // docker image to run
    pub docker_image: String,
    // invoke this command when container is up
    pub command: String,
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

/*
development stages of a job:
 0. created 
 1. running
 2. execution finished
    a. succeeded, ready for verification
    b. failed, harvest ready(unverified)
 3. verification finished
    a. succeeded(n independent sources), harvest ready(verified)
    b. failed, harvest ready(unverified)
 4. harvest ready

 the ideal development sequence: 0, 1, 2.a, 3.a, 4
*/

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Harvest {
    pub fd12_cid: String,
    //@ more fields TBD
}

// unverified execution trace ids start with this prefix
pub const UNVERIFIED_PREFIX: &str = "<!>";

// an execution trace
#[derive(Debug)]
pub struct ExecutionTrace {
    // aka prover
    pub server: String,
    // true means that verifier approved the receipt, and false otherwise 
    pub verifications: HashMap<String, bool>,
    pub harvests: HashSet<Harvest>,
}

impl ExecutionTrace {

    pub fn new(server_id: String) -> ExecutionTrace {
        ExecutionTrace {
            server: server_id,
            verifications: HashMap::<String, bool>::new(),
            harvests: HashSet::<Harvest>::new(),
        }
    } 

    pub fn num_verifications(
        &self,
        is_approved: bool
    ) -> usize {
        self.verifications.values()
        .filter(|v| is_approved == **v)
        .count()
    }

    pub fn is_verified(
        &self,
        min_required_approved_verifications: u8
    ) -> bool {
        let num_approved = self.num_verifications(true);
        let num_rejected = self.num_verifications(false);
        //@ temporary until strategies go live
        (num_approved > 2 * num_rejected) &&
        (num_approved >= min_required_approved_verifications.into())
    }
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
   
    pub schema: Schema,
      
    // update history from servers
    pub status_history: HashMap::<String, Vec<compute::JobStatus>>, 

    // if a job is finished execution, it leaves a receipt to be verified
    // a server is allowed to have several distinct execution traces 
    pub execution_trace: HashMap::<String, ExecutionTrace>,
}

impl Job {
    pub fn new (custom_id: Option<String>, schema: Schema) -> Job {
        Job {                  
            id: custom_id.unwrap_or_else(|| {
                //@ use safer id generation methods              
                Uuid::new_v4().simple().to_string()[..4].to_string()
            }),
            schema: schema,
            
            status_history: HashMap::<String, Vec<compute::JobStatus>>::new(),

            execution_trace: HashMap::<String, ExecutionTrace>::new(),
        }
    }

    // check to see if we have any verified execution traces
    pub fn has_verified_execution_traces(&self) -> bool {
        if self.schema.verification.min_required.is_some() {
            let min_required_verifications = self.schema.verification.min_required.unwrap();
            return self.execution_trace.values()
            .find(|exec_trace| 
                exec_trace.is_verified(min_required_verifications)
            ).is_some()
        }
        false
    }

    // check to see if we have any harvest-ready execution traces
    pub fn has_harvest_ready_execution_traces(&self) -> bool {
        if let Some(min_required_verifications) = self.schema.verification.min_required {
            // verified traces are required
            return self.execution_trace.values()
            .find(|exec_trace| 
                true == exec_trace.is_verified(min_required_verifications)
            ).is_some()
        } else {
            // un-verified traces are ok
            return false == self.execution_trace.is_empty()
        }
    }
}
