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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub id: String,

    // proved, lifted, and verified succinct receipt
    pub verified_blob: VerifiedBlob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRound {
    // e.g.
    // segments: [0, 1, 2, 3, 4]
    // round 1: (0, 1) -> agg
    // round 2: (agg, 2) -> agg
    // round 3: (agg, 3) -> agg
    // round 4: (agg, 4) -> agg
    pub index: u32,

    pub verified_blob: VerifiedBlob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,

    pub po2: u32,
    pub segments_cid: String,
    pub num_segments: u32,

    pub verification: Verification,

    pub segments: Vec<Segment>,

    pub join_rounds: Vec<JoinRound>,

    pub stark_receipt: Vec<u8>,

    pub snark_receipt: VerifiedBlob,
}
