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
    // r0 segment blob on disk, args:
    //   - file path of the segment on disk
    Local(String), 

    // r0 segment blob is uploaded to dstorage and awaits proving, args:
    //   - cid of the segment on dstorage
    ProveReady(String),

    // proved and lifted blob, args:
    //   - cid of the succinct receipt on dstorage
    ProvedAndLifted(String),

    // joined blob, args:
    //   - round number, cid of left sr, cid of right sr, and cid of resulting sr
    Joined(u8, String, String, String),

    // snark blob(on dstorage), arg is r0 receipt's cid
    Snark(String),
}

#[derive(Debug)]
pub struct Segment {
    // the id of the segment, ie for file"0000.seg", id is "0000"
    pub id: String,

    // the number of times this segment has been sent for proving. 
    // used when we need to choose the next prove job in response to a new offer.
    pub num_prove_deals: u32,

    // per segment status
    pub status: Status,    
}


// stages of the recursion
#[derive(Debug, PartialEq)]
pub enum Stage {
    // uploading segments to dstorage
    Upload,

    // proving(and lifting) segments
    Prove,

    // joining segments
    Join,

    // Stark,

    // extracting the final snark
    Snark,

    // recursion has been processed completely
    Done,
}

#[derive(Debug)]
pub struct ProveAndLift {
    pub segments: Vec<Segment>,
}

#[derive(Debug)]
pub struct Join {
    // most recent join round, vec<cid>
    pub joined_segments: Vec<Vec<String>>,

    // join candidates for the next round
    pub to_be_joined: Vec<Vec<(String, String)>>,

    // when the join list is odd, the last one called lucky advances to the next round automatically
    lucky_segments: Vec<String>,
}

impl Join {
    pub fn new() -> Join {
        Join {
            joined_segments: vec![],
            to_be_joined: vec![],
            lucky_segments: vec![],
        }
    }

    pub fn cur_join_round(&self) -> usize {
        self.joined_segments.len()        
    }
}


#[derive(Debug)]
pub struct Recursion {
    pub job_id: String,
    
    pub stage: Stage,
    
    // prove and lift data
    pub prove_and_lift: ProveAndLift,    

    // join data
    pub join: Join,
}

impl Recursion {
    pub fn new(
        job_id: String,
        segments: Vec<Segment>,
    ) -> Recursion {
        Recursion {
            job_id: job_id,
            stage: Stage::Upload,
            prove_and_lift: ProveAndLift {
                segments: segments,
            },
            join: Join::new(),
        }
    }

    // prepare for the next join round(starting at 0)
    pub fn prepare_next_join_round(&mut self) {
        if self.join.cur_join_round() == 0 {
            self.join.joined_segments.push(self.prove_and_lift.segments.iter().map(|seg| seg.id.clone()).collect());
        }
        let mut new_join_pairs = Vec::<(String, String)>::new();
        let mut join_candidates: Vec<String> = self.join.joined_segments.last().unwrap().to_vec();
        if false == self.join.lucky_segments.is_empty() {
            join_candidates.extend_from_slice(self.join.lucky_segments.as_slice());
            self.join.lucky_segments.clear();
        }
        for i in (0..join_candidates.len()).step_by(2) {
            if i == join_candidates.len() - 1 {
                // lucky advances to the next round automatically
                self.join.lucky_segments.push(join_candidates[i].clone());
                break;
            }
            new_join_pairs.push(
                (join_candidates[i].clone(), join_candidates[i + 1].clone())
            );
        }
        self.join.to_be_joined.push(new_join_pairs);        
    }
}
