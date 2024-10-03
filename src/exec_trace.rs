use std::collections::{
    HashSet
};

use comms::compute;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Harvest {
    pub fd12_cid: String,
    //@ more fields TBD
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

    pub receipt_cid: String,

    pub compute_type: compute::ComputeType,

    pub local_verification: VerificationResult,

    pub harvests: HashSet<Harvest>,
}
