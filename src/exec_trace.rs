use std::error::Error;
use uuid::Uuid;
use std::collections::{
    HashMap, HashSet
};
use serde::Deserialize;

use comms::compute;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Harvest {
    pub fd12_cid: String,
    //@ more fields TBD
}

#[derive(Debug, Eq, PartialEq)]
pub struct StatusUpdate {
    pub status: compute::JobStatus,
    pub timestamp: i64,
}

// verification result
#[derive(Debug, Eq, PartialEq)]
pub enum VerificationResult {
    Pending,
    Verified,
    Unverified,
}

// an execution trace
#[derive(Debug)]
pub struct ExecutionTrace {
    // aka prover
    pub server_id: String,

    //@ fill it
    pub receipt_cid: Option<String>,

    pub local_verification: VerificationResult,

    // job status
    pub status_update_history: Vec<StatusUpdate>,

    // true means that verifier approved the receipt, and false otherwise 
    pub verifications: HashMap<String, bool>,

    pub harvests: HashSet<Harvest>,
}

impl ExecutionTrace {

    pub fn new(server_id: String) -> ExecutionTrace {
        ExecutionTrace {
            server_id: server_id,
            receipt_cid: None,
            local_verification: VerificationResult::Pending,
            status_update_history: Vec::<StatusUpdate>::new(),
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
