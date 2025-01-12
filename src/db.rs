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
    //@ how about decoding cids into vec<u8> binary using multibase(cid)

    pub cid: String,

    // the prover
    pub prover: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    
    pub id: u32,

    //@ rename to job_oid: ObjectId
    pub job_id: Bson,

    // proved, lifted, and verified succinct receipt
    pub proof: Proof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {

    pub job_id: Bson,

    pub round: u32,

    pub left_input_proof: String,
    pub right_input_proof: String,
    pub proof: Proof,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    
    pub id: String,

    pub po2: u32,
    pub segments_cid: String,
    pub num_segments: u32,

    pub stage: Bson,

    pub verification: Verification,

    pub snark_receipt: Option<Proof>,
}
