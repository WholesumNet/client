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
use bit_vec::BitVec;

/*
verifiable computing is resource hungry, with at least 10x more compute steps
compared to untrusted execution. to tackle it, we need to employ parallel
proving.
development stages of a job:
 0. created 
 1. segmented(into N items according to r0 po2_limit)
 2. parallel proving via recursion
    a. prove and lift
        - N segments -> N proofs(SuccinctReceipt)
    b. join
        btree fashion
        log2(n) + 1 rounds of join to obtain the final proof(SuccinctReceipt) aka the stark proof
        e.g. starting with N = 5 and segments labeled 1-5:
          1: (1, 2) -> 12, (3, 4) -> 34, 5 -> 5
          2: (12, 34) -> 1234, 5 -> 5
          3: (1234, 5) -> the final stark proof
    c. snark extraction
        - apply identity_p254 and then compress -> ~300 bytes snark proof
*/

#[derive(Debug)]
pub struct Proof {    
    pub prover: String,
    pub filepath: Option<String>
}

#[derive(Debug)]
pub struct ProveAndLift {    
    pub num_segments: u32,

    // <segment-id, <cid, proof>>
    pub proofs: HashMap<u32, HashMap<String, Proof>>,
    
    // each segment is represented by one bit: "true" => proved, "false" => not proved yet
    pub proved_map: BitVec,
}

impl ProveAndLift {
    pub fn new(
        num_segments: u32
    ) -> Self {
        ProveAndLift {
            num_segments: num_segments,
            proofs: HashMap::<u32, HashMap<String, Proof>>::new(),
            proved_map: BitVec::from_elem(num_segments as usize, false)
        }
    }
    
    pub fn is_proving_finished(
        &self
    ) -> bool {
       self.num_segments == self.proved_map.len() as u32
    }

    pub fn are_all_proofs_donwloaded(
        &self
    ) -> bool {
        if self.num_segments != self.proofs.len() as u32{
            return false;        
        }
        // all segments should have at least one proof on disk
        self.proofs
        .values()
        .all(|proofs| 
            proofs
            .values()
            .any(|proof|
                true == proof.filepath.is_some()
            )
        )
    }
}

#[derive(Debug)]
pub struct JoinPair {
    // joins are ordered, so use this to preserve consistency
    pub position: u32,

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
    pub joined: BTreeMap<u32, String>,

    // the result and also the left over(in non-final rounds): eg receipts(round1): [0..4] -> pair 1: (0, 1), ..., leftover: (5)
    pub agg: Option<String>,

    // map of verification pool: <receipt_cid, prover>
    pub to_be_verified: HashMap<String, String>,
}

impl Join {
    pub fn new(
        num_segments: u32
    ) -> Self {
        Join {
            round: -1,
            pairs: Vec::<JoinPair>::with_capacity(num_segments as usize),
            joined: BTreeMap::<u32, String>::new(),
            agg: None,
            to_be_verified: HashMap::<String, String>::new(),
        }
    }

    pub fn initiate(
        &mut self,
        receipts: Vec<String>,
    ) {
        for i in 0..receipts.len() {
            self.joined.insert(i as u32, receipts[i].clone());
        }
    }

    // return value of "true" means join is complete
    pub fn begin_next_round(&mut self) -> bool {
        if self.pairs.len() > 0 {
            eprintln!("[warn] Attempting to begin the next join round while the current one is not finished yet.");
            return false;
        }
        let mut to_be_joined: Vec<String> = self.joined.values().cloned().collect();
        if to_be_joined.len() == 0 {
            eprintln!("[warn] Nothing is left to join.");
            return true;
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
                        position: pos as u32,
                        left: to_be_joined[i].clone(),
                        right: to_be_joined[i + 1].clone(),
                        num_prove_deals: 0,
                    }
                );
            }
        }
        if self.pairs.len() > 0 {
            println!("[info] Starting join round `{}`\n pairs: {:#?}\n leftover: `{:?}`",
                self.round,
                self.pairs,
                self.agg
            );
        }
        if self.pairs.len() == 1 && true == self.agg.is_none() {
            println!("[info] This is going to be the last round.");
        }

        0 == self.pairs.len()
    }
}

// stages of the recursion process
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stage {
    
    // proving(and lifting) segments
    Prove,

    // verifying proved(and lifted) segments
    ProveVerification,

    // prove & lift is complete, now joining segments
    Join,

    // verifying joined proofs
    JoinVerification,

    // join is completed and the stark proof is ready
    Stark,

    // extracting the final snark
    Snark,
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
        num_segments: u32
    ) -> Self {
        Recursion {
            stage: Stage::Prove,
            prove_and_lift: ProveAndLift::new(
                num_segments
            ),
            join: Join::new(num_segments),
            snark: None,
        }
    }

    pub fn begin_prove_verification(&mut self) {
        if self.stage != Stage::Prove {
            eprintln!("[warn] Stage must be `Prove` to initiate verification.");
            return;            
        }
        if false == self.prove_and_lift.is_proving_finished() {
            eprintln!("[warn] All segments must be proved before verification.");
            return;
        }
        self.stage = Stage::ProveVerification;
    }

    pub fn begin_join(
        &mut self
    ) {
        if self.stage != Stage::Prove {
            eprintln!("[warn] Prove stage must be finished before join begins.");
            return;
        }        
        let receipts = vec![];
        //self.prove_and_lift.segments.iter().map(|some_seg| 
        //     if let SegmentStatus::ProvedAndLifted(receipt) = &some_seg.status { 
        //         receipt.clone()
        //     } else {
        //         eprintln!("[warn] `{}`'s status is not proved and lifted.", some_seg.id);
        //         String::new()
        //     }                    
        // ).collect();
        self.join.initiate(receipts);
        self.stage = Stage::Join;
    }    
}
