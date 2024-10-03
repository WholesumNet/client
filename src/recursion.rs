use std::vec::Vec;

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

#[derive(Debug, PartialEq, Clone)]
pub enum Status {
    Unknown, 

    // r0 segment blob on disk, args:
    //   - file path of the segment on disk
    Local(Option<String>), 

    // r0 segment blob is uploaded to dstorage and awaits proving, args:
    //   - cid of the segment on dstorage
    ProveReady(Option<String>),

    // proved and lifted blob, args:
    //   - cid of the succinct receipt on dstorage
    ProvedAndLifted(Option<String>),

    // joined blob(on dstorage), args:
    //   - round number, left sr(cid), right sr(cid), and the resulting sr(cid)
    Joined(u8, String, String, String),

    // snark blob(on dstorage), arg is r0 receipt's cid
    Snark(String),
}

#[derive(Debug)]
pub struct Segment {
    // the id of the segment, ie for file"0000.seg", id is "0000"
    pub id: String,

    // per segment status
    pub status: Status,    
}

#[derive(Debug, PartialEq)]
pub enum Stage {
    // waiting for the segments to be upload to dstorage
    UploadingSegments,

    // waiting for all segments to be proved and lifted
    Proving,

    // join, param is round number
    Join(u8),

    Snarked,
}

#[derive(Debug)]
pub struct Recursion {
    pub job_id: String,
    
    // segment's file name(without its extension) is used as key
    pub segments: Vec<Segment>,
    
    pub stage: Stage,
}
