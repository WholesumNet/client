use std::{    
    vec::Vec,
    collections::{
        HashMap,
        BTreeMap
    },
};
use serde::{
    Serialize, Deserialize
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
        btree fashion
        log2(n) + 1 rounds of join to obtain the final SuccinctReceipt aka stark receip
        e.g. starting with N = 5 and segments labeled 1-5:
          1: (1, 2) -> 12, (3, 4) -> 34, 5 -> 5
          2: (12, 34) -> 1234, 5 -> 5
          3: (1234, 5) -> the final stark receipt
    c. snark extraction
        - apply identity_p254 and then compress -> ~300 bytes snark(Receipt)
 3. verification
    a. succeeded => harvest ready(verified)
    b. failed => harvest ready(unverified)
 4. harvest ready
*/

// stages of the recursion process
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stage {
    // proving(and lifting) segments
    Prove,

    // prove & lift is complete, now joining segments
    Join,

    // join is completed, the receipt is now a stark receipt
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
    // eg segment-0
    pub id: String,

    // the number of times this segment has been sent for proving. 
    // used when we need to choose the next prove job in response to a new offer.
    pub num_prove_deals: u32,

    // per segment status
    pub status: SegmentStatus,

    // proved(and lifted) segments awaiting verification: <receipt_cid, prover_peer_id>
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

impl ProveAndLift {
    pub fn is_finished(
        &self
    ) -> bool {
        self.segments.iter().all(|some_seg| {
            match some_seg.status {
                SegmentStatus::ProvedAndLifted(_) => true,
                _ => false,            
            }        
        })
    }
}

#[derive(Debug)]
pub struct JoinPair {
    // joins are ordered, so use this to preserve consistency
    pub position: usize,

    pub left: String,
    pub right: String,

    pub num_prove_deals: u32,
}

#[derive(Debug)]
pub struct Join {
    // current round
    pub round: i32,

    // to be joined pairs for the current round
    pub pairs: Vec<JoinPair>,

    // receipts of the previous round
    pub joined: BTreeMap<usize, String>,

    // the left over: eg receipts: [0..4] -> pair 1: (0, 1), ..., leftover: (5)
    pub agg: Option<String>,

    // map of verification pool: <receipt_cid, prover>
    pub to_be_verified: HashMap<String, String>,
}

impl Join {
    pub fn new(
        num_segments: usize
    ) -> Self {
        Join {
            round: -1,
            pairs: Vec::<JoinPair>::with_capacity(num_segments),
            joined: BTreeMap::<usize, String>::new(),
            agg: None,
            to_be_verified: HashMap::<String, String>::new(),
        }
    }

    pub fn initiate(
        &mut self,
        receipts: Vec<String>,
    ) {
        for i in 0..receipts.len() {
            self.joined.insert(i, receipts[i].clone());
        }
    }

    pub fn begin_next_round(&mut self) -> bool {
        let mut to_be_joined: Vec<String> = self.joined.values().cloned().collect();
        if to_be_joined.len() == 0 {
            eprintln!("[warn] Nothing is left to join.");
        }
        if let Some(leftover) = &self.agg {
            to_be_joined.push(leftover.clone());
        }    
        self.round += 1;    
        self.agg = None;
        self.joined.clear();
        self.pairs.clear();
        for i in (0..to_be_joined.len()).step_by(2) {
            if i == to_be_joined.len() - 1 {
                self.agg = Some(to_be_joined[i].clone());
            } else {
                let pos = if i > 0 { i - 1 } else { i }; 
                self.pairs.push(
                    JoinPair {
                        position: pos,
                        left: to_be_joined[i].clone(),
                        right: to_be_joined[i + 1].clone(),
                        num_prove_deals: 0,
                    }
                );
            }
        }
        if false == self.is_round_finished() {
            println!("[info] Starting join round `{}`\n pairs: {:#?}\n leftover: `{:?}`",
                self.round,
                self.pairs,
                self.agg
            );
        }
        if self.pairs.len() == 1 && true == self.agg.is_none() {
            println!("[info] This is going to be the last round.");
        }

        self.is_round_finished()
    }

    pub fn is_round_finished(&self) -> bool {
        0 == self.pairs.len()
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
        let num_segments = segments.len();
        Recursion {
            stage: Stage::Prove,
            prove_and_lift: ProveAndLift {
                segments: segments,
            },
            join: Join::new(num_segments),
            snark: None,
        }
    }

    pub fn begin_join(
        &mut self
    ) {
        if self.stage != Stage::Prove {
            eprintln!("[warn] Prove stage must be finished before join begins.");
            return;
        }        
        let receipts = self.prove_and_lift.segments.iter().map(|some_seg| 
            if let SegmentStatus::ProvedAndLifted(receipt) = &some_seg.status { 
                receipt.clone()
            } else {
                eprintln!("[warn] `{}`'s status is not proved and lifted.", some_seg.id);
                String::new()
            }                    
        ).collect();
        self.join.initiate(receipts);
        self.stage = Stage::Join;
    }    
}
