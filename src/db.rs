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
pub struct VerifiedBlob {    
    pub cid: String,

    // in memory blob
    pub blob: Vec<u8>,

    // the prover
    pub prover: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,

    pub job_id: Bson,

    // proved, lifted, and verified succinct receipt
    pub verified_blob: VerifiedBlob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedBlobForJoin {        
    pub input_cid_left: String,
    pub input_cid_right: String,

    // cid of the succint receipt
    pub cid: String,

    // in memory blob
    pub blob: Vec<u8>,

    // the prover
    pub prover: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    
    pub job_id: Bson,

    pub round: u32,

    pub verified_blob: VerifiedBlobForJoin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,

    pub po2: u32,
    pub segments_cid: String,
    pub num_segments: u32,

    pub stage: Bson,

    pub verification: Verification,

    pub snark_receipt: Option<VerifiedBlob>,
}
