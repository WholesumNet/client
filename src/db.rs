use mongodb::bson::Bson;
use serde::{
    Serialize, Deserialize
};

// mongodb database models for the job data

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verification {
    
    pub image_id: String,

    pub journal_blob: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {    
    pub prover: String,

    pub blob: Vec<u8>,

    pub hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {    
    pub id: u32,

    //@ rename to job_oid: ObjectId
    pub job_id: Bson,

    // a succinct receipt(proved & lifted)
    pub proof: Proof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub pair_id: u32,

    pub job_id: Bson,

    pub round: u32,


    pub proof: Proof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Groth16 {
    pub job_id: Bson,

    pub proof: Proof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    
    pub id: String,
    
    pub num_segments: u32,

    pub stage: Bson,

    pub verification: Verification,

    pub snark_proof: Option<Proof>,
}
