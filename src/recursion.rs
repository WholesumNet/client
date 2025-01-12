use std::{    
    vec::Vec,
    collections::{
        HashMap,
        BTreeMap
    },
};
use serde::{
    Serialize,
    Deserialize
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
    pub cid: String,

    pub prover: String,

    // whether this proof is chosen for the next round
    pub spent: bool
}

/* Prove */

#[derive(Debug)]
pub struct ProveAndLift {
    pub num_segments: u32,

    // <segment-id, <proofs>>
    pub proofs: BTreeMap<u32, Vec<Proof>>,
    
    // each segment is represented by one bit: "true" => proved, "false" => not proved yet
    pub progress_map: BitVec,
}

impl ProveAndLift {
    pub fn new(num_segments: u32) -> Self {
        ProveAndLift {
            num_segments: num_segments,
            proofs: BTreeMap::new(),
            progress_map: BitVec::from_elem(num_segments as usize, false)
        }
    }
    
    pub fn is_finished(&self) -> bool {
       self.num_segments == self.progress_map.len() as u32
    }
}

/* Join */

#[derive(Debug)]
pub struct JoinRound {
    pub number: u32,

    pub pairs: Vec<(String, String)>,
    
    // advances to the next round automatically as the last proof
    pub leftover: Option<String>,

    // output of this round: <index of the pair in 'pairs', <proofs>>
    pub proofs: BTreeMap<usize, Vec<Proof>>,

    // each pair is represented by one bit: "true" => joined, "false" => not joined yet
    pub progress_map: BitVec,
}

// stages of the recursion process
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Stage {
    
    // proving(and lifting) segments
    Prove,

    // prove & lift is complete, now joining segments
    Join,

    // join is complete, so begin stark verification
    Stark,

    // stark proof is ready, so begin groth16 verification
    Groth16,
}

#[derive(Debug)]
pub struct Recursion {
    pub stage: Stage,
    
    // prove and lift data
    pub prove_and_lift: ProveAndLift,

    // join data        
    pub join_rounds: Vec<JoinRound>,
    // the result of the last join operation aka stark proof
    pub join_proof: Option<String>,
}

impl Recursion {
    pub fn new(num_segments: u32) -> Self {
        Recursion {
            stage: Stage::Prove,
            prove_and_lift: ProveAndLift::new(num_segments),
            join_rounds: Vec::new(),
            join_proof: None,
        }
    }

    pub fn begin_join_stage(&mut self) -> bool {
        if self.stage != Stage::Prove {
            eprintln!("[warn] Join stage follows the Prove stage.");
            return false;
        }        
        if false == self.prove_and_lift.is_finished() {
            eprintln!("[warn] The prove stage is not finished yet.");
            return false;
        }
        if true == self.prove_and_lift.proofs.is_empty() {
            eprintln!("[warn] No proofs to join.");
            return false;
        }
        self.stage = Stage::Join;        
        self.begin_next_join_round()
    }

    pub fn begin_next_join_round(&mut self) -> bool {        
        let mut prev_round_proofs = vec![];
        if true == self.join_rounds.is_empty() {
            for proofs in self.prove_and_lift.proofs.values_mut() {
                //@ beware starvation of other cids
                let chosen_proof = match proofs
                    .iter_mut()
                    .filter(|p| p.spent == false)
                    .nth(0)
                {
                    None => {
                        eprintln!("[warn] No more proofs to choose from, all are spent.");  
                        return false                    
                    },

                    Some(p) => {
                        p.spent = true;
                        p.cid.clone()
                    }
                };
                prev_round_proofs.push(chosen_proof.clone());
            }
        } else {
            let last_round = self.join_rounds.last_mut().unwrap();            
            for proofs in last_round.proofs.values_mut() {
                //@ beware starvation of other cids
                let chosen_proof = match proofs
                    .iter_mut()
                    .filter(|p| p.spent == false)
                    .nth(0)
                {
                    None => {
                        eprintln!("[warn] No more proofs to choose from, all are spent.");  
                        return false                    
                    },

                    Some(p) => {
                        p.spent = true;
                        p.cid.clone()
                    }
                };
                prev_round_proofs.push(chosen_proof);
            }
            if let Some(lo) = &last_round.leftover {
                prev_round_proofs.push(lo.clone());
            }
        }
    
        if prev_round_proofs.len() == 1 {
            println!("[info] Join is finished.");
            self.join_proof = Some(prev_round_proofs.pop().unwrap());
            self.stage = Stage::Stark;
            return true;            
        } else if prev_round_proofs.len() == 2 {
            println!("[info] This is going to be the last join round.");
        }
        
        let leftover = if prev_round_proofs.len() % 2 == 1 {
            prev_round_proofs.pop()
        } else {
            None
        };
        // [0, 1, 2, 3, ..., n - 1] => [(0, 1), (2, 3), ..., (n - 2, n - 1)]        
        let mut pairs = Vec::new();
        let mut iter = prev_round_proofs.into_iter();
        while let Some(left) = iter.next() {
            if let Some(right) = iter.next() {
                pairs.push((left, right));
            }
        }
        let num_pairs = pairs.len();
        self.join_rounds.push(
            JoinRound {
                number: self.join_rounds.len() as u32 + 1,                
                pairs: pairs,
                proofs: BTreeMap::new(),
                leftover: leftover,
                progress_map: BitVec::from_elem(num_pairs, false),
            }
        );        
        false
    }  
}
