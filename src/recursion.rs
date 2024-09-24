use std::collections::{
    HashMap
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
        - Log2(N) rounds of join -> the final SuccinctReceipt
          e.g. starting with N = 5 and segments labeled 1-5:
          r1: (1, 2) -> 12, (3, 4) -> 34, 5 -> 5
          r2: (12, 34) -> 1234, 5 -> 5
          r3: (1234, 5) -> final sr
    c. snark extraction
        - apply identity_p254 and then compress -> ~300 bytes snark(Receipt)
 3. verification
    a. succeeded(k independent sources) => harvest ready(verified)
    b. failed => harvest ready(unverified)
 4. harvest ready
*/

#[derive(Debug)]
pub enum LifeCycle {
    // r0 segment blob, arg is file path of the segment on disk
    Local(String), 

    // r0 segment blob(on dstorage), arg is the cid of the segment
    Unproved(String),

    // proved and lifted blob(on dstorage), arg is the cid of the succinct receipt
    ProvedAndLifted(String),

    // joined blob(on dstorage), args:
    //     round number, left sr(cid), right sr(cid), and the resulting sr(cid)
    Joined(u8, String, String, String),

    // snark blob(on dstorage), arg is r0 receipt's cid
    Snark(String),
}

#[derive(Debug)]
pub struct Segment {
    // the id of the segment, ie for file"0000.seg", id is "0000"
    pub id: String,
    pub life_cycle: LifeCycle,    
}

pub struct Recursion {
    pub job_id: String,
    // segment file stem is the key
    pub segments: HashMap<String, Segment>,
}