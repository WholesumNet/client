use mongodb::bson::Bson;
use serde::{
    Serialize, Deserialize
};

// mongodb database models for the job data

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Kind {
    Aggregate,

    Assumption,

    Groth16
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {    
    pub job_id: Bson,

    pub prover: Vec<u8>,
    pub blob: Option<Vec<u8>>,
    pub hash: String,

    pub round_number: Option<u32>,
    
    pub kind: Kind,

    pub batch_id: String
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {    
    pub id: String,

    pub image_id: [u8; 32],
}
