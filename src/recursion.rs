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
pub enum SegmentStatus {
    // r0 segment blob on disk, args:
    //   - file path of the segment on disk
    Local(String), 

    // r0 segment blob is uploaded to dstorage and awaits proving, args:
    //   - cid of the segment on dstorage
    ProveReady(String),

    // proved and lifted blob, args:
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

    // join completed
    Stark,

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
    pub joined: Vec<Vec<String>>,

    // join pairs for the current round
    pub to_be_joined: Vec<(String, String)>,

    // when the join list is odd, the last one called lucky advances to the next round automatically
    lucky: Option<String>,    
}

impl Join {
    pub fn new() -> Join {
        Join {
            joined: vec![],
            to_be_joined: vec![],
            lucky: None,
        }
    }

    pub fn cur_join_round(&self) -> usize {
        self.joined.len()        
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
            stage: Stage::Upload,
            prove_and_lift: ProveAndLift {
                segments: segments,
            },
            join: Join::new(),
            snark: None,
        }
    }

    // prepare for the next join round(starting at 0)
    pub fn prepare_next_join_round(&mut self) {
        if self.join.cur_join_round() == 0 {
            self.join.joined.push(
                self.prove_and_lift.segments.iter()
                .map(|seg| seg.id.clone()).collect()
            );
        }
        let mut join_candidates: Vec<String> = self.join.joined.last().unwrap().to_vec();
        if let Some(lucky) = &self.join.lucky {
            join_candidates.push(lucky.to_string());
            self.join.lucky = None;
        }
        for i in (0..join_candidates.len()).step_by(2) {
            if i == join_candidates.len() - 1 {
                // the lucky segment advances to the next round automatically
                self.join.lucky = Some(join_candidates[i].clone());
                break;
            }
            self.join.to_be_joined.push(
                (join_candidates[i].clone(), join_candidates[i + 1].clone())
            );
        }
        // join is complete
        if true == self.join.to_be_joined.is_empty() {
            self.join.joined.push(vec![self.join.lucky.clone().unwrap()]);            
            self.stage = Stage::Stark;
        }
    }
}
