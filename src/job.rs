use uuid::Uuid;
use std::collections::{
    HashMap, 
};
use serde::Deserialize;

use comms::{
    compute,
};

// job template as read in(e.g. from disk)
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct Schema {
    pub docker_image: String,
    pub command: String,
    pub image_id: String,              // image_id as in risc0, it's a hash digest
    pub required_verifications: u8,    // minimum number of independent verification to regard job as verified
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

// // an execution trace
#[derive(Debug, PartialEq, Eq)]
pub struct ExecutionTrace {
    // aka prover
    pub server: String,
    // true means that verifier approved the receipt, and false otherwise 
    pub verifications: HashMap<String, bool>,
}

impl ExecutionTrace {
    pub fn num_verifications(
        &self,
        approved: bool
    ) -> usize {
        self.verifications.values()
        .filter(|v| approved == **v)
        .count()
    }

    pub fn is_verified(
        &self,
        num_required_verifications: u8
    ) -> bool {
        let num_approved = self.num_verifications(true);
        let num_rejected = self.num_verifications(false);
        (num_approved > 2 * num_rejected) &&
        (num_approved >= num_required_verifications.into())
    }
}

#[derive(Debug)]
pub struct Harvest {
    pub fd12_cid: Option<String>,
    //@ more fields TBD
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
   
    pub schema: Schema,
      
    // update history from servers
    pub status_history: HashMap::<String, Vec<compute::JobStatus>>, 

    // a server(prover) is assumed to leave several execution receipts where each
    // receipe may receive up to N successful independent verifications  
    pub execution_trace: HashMap::<String, ExecutionTrace>,

    // an execution trace(identified by the receipt) can become a successful harvest
    pub harvests: HashMap<String, Harvest>,
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

            harvests: HashMap::<String, Harvest>::new(),
        }
    }

    // check to see if we have any verified execution traces
    pub fn has_verified_execution_traces(&self) -> bool {
        self.execution_trace.values()
        .find(|exec_trace| 
            exec_trace.is_verified(self.schema.required_verifications)
        ).is_some()
    }
}
