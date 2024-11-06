use std::{
    vec::Vec,
    collections::HashMap
};

/*
verifiable computing is resource hungry, with at least 10x more compute steps
compared to untrusted execution. to tackle it, we need to employ parallel
proving.
development stages of a job:
 0. created 
 1. segmented(into N items according to r0 po2_limit)
 2. parallel proving aka recursion
    a. prove and lift
        - N iterations -> N SuccinctReceipts
    b. join
        - Log2(N) + 1 steps of join to obtain the final SuccinctReceipt aka stark receip
          e.g. starting with N = 5 and segments labeled 1-5:
          1: (1, 2) -> 12
          2: (12, 3) -> 123
          3: (123, 4) -> 1234
          4: (1234, 5) -> the final stark receipt
    c. snark extraction
        - apply identity_p254 and then compress -> ~300 bytes snark(Receipt)
 3. verification
    a. succeeded => harvest ready(verified)
    b. failed => harvest ready(unverified)
 4. harvest ready
*/

// stages of the recursion process
#[derive(Debug, PartialEq)]
pub enum Stage {
    // proving(and lifting) segments
    Prove,

    // joining segments
    Join,

    // join completed
    Stark,

    // extracting the final snark
    Snark,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SegmentStatus {
    // r0 segment blob is uploaded to dstorage and awaits proving
    // or is proved but awaits verification, args:
    //   - cid of the segment on dstorage
    ProveReady(String),

    // proved(and lifted), and verified blob, args:
    //   - cid of the succinct receipt on dstorage
    ProvedAndLifted(String),
}

#[derive(Debug)]
pub struct Segment {
    // the id of the segment, ie for file"0000.seg", id is "0000"
    pub id: String,

    // the number of times this segment has been sent for proving. 
    // used when we need to choose the next prove job in response to a new offer.
    pub num_prove_deals: u32,

    // per segment status
    pub status: SegmentStatus,

    // proved(and lifted) segments awaiting verification: <prover_peer_id, receipt_cid>
    pub to_be_verified: HashMap<String, String>,
}

impl Segment {
    pub fn new(
        id: &str,
        base_segment_cid: &str,
    ) -> Self {
        Segment {
            id: id.to_string(),
            num_prove_deals: 0,
            status: SegmentStatus::ProveReady(
                format!("{base_segment_cid}/{id}")
            ),
            to_be_verified: HashMap::<String, String>::new(),
        }
    }
}

#[derive(Debug)]
pub struct ProveAndLift {
    pub segments: Vec<Segment>,
}

#[derive(Debug)]
pub struct Join {    
    // which segment to join next? starts at 1
    pub index: usize,

    // the latest joined and verifier receipt_cid
    pub agg: String,

    // joined segments awaiting verification: <prover_peer_id, receipt_cid>
    pub to_be_verified: HashMap<String, String>,
}

impl Join {
    pub fn new() -> Join {
        Join {
            index: 1,
            agg: String::new(),
            to_be_verified: HashMap::<String, String>::new()
        }
    }
}


#[derive(Debug)]
pub struct Recursion {
    pub stage: Stage,
    
    // prove and lift data
    pub prove_and_lift: ProveAndLift,    

    // join data
    pub join: Join,

    // snark data
    pub snark: Option<String>,
}

impl Recursion {
    pub fn new(
        segments: Vec<Segment>,
    ) -> Recursion {
        Recursion {
            stage: Stage::Prove,
            prove_and_lift: ProveAndLift {
                segments: segments,
            },
            join: Join::new(),
            snark: None,
        }
    }    
}
