use std::{ 
    collections::{BTreeMap, HashMap, HashSet},
};

use rand::prelude::IndexedRandom;
use log::{
    info, warn
};
use xxhash_rust::xxh3::xxh3_128;
use uuid::Uuid;
use risc0_zkvm::{
    ApiClient, ProverOpts,
    Receipt, InnerReceipt,
    SuccinctReceipt, ReceiptClaim,
    Unknown,
    Groth16Receipt,
    Asset, AssetRequest,
    Digest
};
use hex::FromHex;

use peyk::protocol::{
    KeccakRequestObject, ZkrRequestObject,
};

#[derive(Debug, Clone)]
pub struct Proof {
    pub provers: HashSet<String>,

    pub blob: Option<Vec<u8>>,
    
    pub hash: u128,
}

#[derive(Debug)]
pub struct Segment {    
    pub blob: Vec<u8>,

    pub hash: u128
}

#[derive(Debug)]
pub enum Input {    
    SegmentInput(Segment),

    ProofInput(Proof)
}

#[derive(Debug)]
pub struct Assignment {
    pub owners: Vec<String>,

    pub blob: Option<Vec<u8>>,

    pub hash: u128,
}

#[derive(Debug)]
pub enum ResolveItem {
    Keccak(Digest, Vec<u8>),

    Zkr(Digest, Vec<u8>)
}

#[derive(Debug)]
pub struct AggRound {
    pub number: usize,
    
    // <index, input>
    pub inputs: BTreeMap<usize, Input>,
    
    // inputs are grouped into batches of batch size
    pub batch_size: usize,
    // helper for out of order feed arrival handling
    pub partial_batches: BTreeMap<usize, Vec<usize>>,
    // <batch index, batch>
    pub batches: BTreeMap<usize, Vec<usize>>,
    // unique id for the sub-job, params: <batch index, hash>
    pub batch_ids: HashMap<u128, usize>,
    pub inverse_batch_ids: HashMap<usize, u128>,

    // prover p1 has been assigned batch b
    // <batch index, provers>
    pub assigned_batches: HashMap<usize, Vec<String>>,
    // helper(^) reverse map to complement "assigned_batches": <prover, assigned batches>
    pub prover_batch_assignements: HashMap<String, Vec<usize>>,

    // <batch index, <proof, provers>>
    pub proofs: BTreeMap<usize, Proof>,
    // helper(^) reverse map to complement "proofs": <prover, batches>
    pub prover_proofs: HashMap<String, Vec<usize>>,
}

impl AggRound {
    pub fn new(number: usize, batch_size: usize) -> Self {
        Self {
            number: number,
            inputs: BTreeMap::new(),
            batch_size: batch_size,
            partial_batches: BTreeMap::new(),            
            batches: BTreeMap::new(),
            batch_ids: HashMap::new(),
            inverse_batch_ids: HashMap::new(),
            assigned_batches: HashMap::new(),
            prover_batch_assignements: HashMap::new(),
            proofs: BTreeMap::new(),
            prover_proofs: HashMap::new(),
        }
    }

    // append the input to the partial batch and upgrade it needful
    pub fn feed_input(&mut self, index: usize, input: Input) {
        if self.inputs.contains_key(&index) {
            return
        }
        info!("New input with index `{index}` has arrived.");
        self.inputs.insert(index, input);
        //@ handle 0th proof in join
        let batch_index = index / self.batch_size;
        self.partial_batches.entry(batch_index)
            .and_modify(|batch| {
                batch[index % self.batch_size] = index;
            })
            .or_insert_with(|| {
                let mut new_batch = vec![0usize; self.batch_size];
                new_batch[index % self.batch_size] = index;
                new_batch
            })
            ;
        // upgrade if batch is full
        if self.partial_batches
            .get(&batch_index)
            .unwrap()
            .iter()
            .skip(if batch_index == 0 { 1 } else { 0 })
            .all(|bi| *bi != 0usize)
        {
            if batch_index != 0 || (batch_index == 0 && self.inputs.contains_key(&0)) {
                self.batches.insert(
                    batch_index,
                    self.partial_batches.remove(&batch_index).unwrap()
                );
                let batch_id = Uuid::new_v4().as_u128();
                self.batch_ids.insert(batch_id, batch_index);
                self.inverse_batch_ids.insert(batch_index, batch_id);
            }
        }
        info!("partial-batches: {:?}", self.partial_batches);
        info!("batches: {:?}", self.batches);
    }

    // no new input is expected, so upgrade the partial batch into a full batch
    pub fn stop_feeding(&mut self) {
        info!("Partial batches: {:?}", self.partial_batches);
        if self.partial_batches.is_empty() {
            return
        }
        if self.partial_batches.len() > 1 {
            warn!("Too many partial batches, expected just one.");
            return            
        }
        let (index, batch) = self.partial_batches.first_key_value().unwrap();
        let last_batch = batch
            .iter()
            .filter_map(|item| if *item != 0 { Some(*item) } else { None })
            .collect();
        self.batches.insert(*index, last_batch);
        let batch_id = Uuid::new_v4().as_u128();
        self.batch_ids.insert(batch_id, *index);
        self.inverse_batch_ids.insert(*index, batch_id);
        info!("batches: {:?}", self.batches);
    }

    // assign a batch to prover
    pub fn assign_agg_batch(&self, prover: &str) -> Option<(u128, Vec<Assignment>)> {        
        let outstanding_batches: Vec<_> = self.batches.keys()
            .filter(|index| {
                // filter already proved batches
                !self.proofs.contains_key(&index) &&
                // and, if the batch is already assigned to this prover
                !self.assigned_batches.get(&index)
                    .and_then(|assignees| 
                        assignees
                            .iter()
                            .find(|p| *p == prover)
                    )
                .is_some()
            })
            .collect();
        if outstanding_batches.is_empty() {
            info!("round:, {}, b: {:?}", self.number, self.batches);
            return None
        }
        let (pick_closest, other_batches) = if self.prover_proofs.contains_key(prover) {
            // rank batches based on their distance to the proofs and pick the closest
            (true, self.prover_proofs.get(prover).unwrap().clone())
        } else {
            if self.prover_batch_assignements.contains_key(prover) {
                // rank batches based on their distance to the assigned batches and pick the closest
                (true, self.prover_batch_assignements.get(prover).unwrap().clone())
            }
            else {
                // brand new prover
                if !self.proofs.is_empty() {
                    (false, self.proofs.keys().copied().collect())
                } else {
                    (false, self.assigned_batches.keys().copied().collect())
                }
            }
        };
        let selected_batch_index = if !other_batches.is_empty() {
            let batch_distances = outstanding_batches
                .iter()
                .map(|batch_index| {
                    let mut dist = batch_index.abs_diff(other_batches[0]);
                    for other_batch_index in other_batches.iter().skip(1) {
                        let abs_diff = batch_index.abs_diff(*other_batch_index);
                        if abs_diff < dist {
                            dist = abs_diff;
                        }
                    }
                    (batch_index, dist)
                });
            let closest = if pick_closest {
                batch_distances.min_by(|x, y| x.1.cmp(&y.1))
            } else {
                batch_distances.max_by(|x, y| x.1.cmp(&y.1))
            }
            .unwrap()
            .0;
            **closest
        } else {
            0
        };
        info!("outstanding: {outstanding_batches:?}, sbi: {selected_batch_index}, batches: {:?}", self.batches);
        // let selected_batch = outstanding_batches[selected_batch_index];
        let assignment = self.batches[&selected_batch_index]
            .iter()
            .map(|i| {
                let input = self.inputs.get(i).unwrap();            
                match input {
                    Input::SegmentInput(segment) => {
                        Assignment {
                            owners: vec![],
                            blob: Some(segment.blob.clone()),
                            hash: segment.hash
                        }
                    },
                    
                    Input::ProofInput(proof) => 
                        Assignment {
                            owners: proof.provers.iter().cloned().collect(),
                            blob: None,
                            hash: proof.hash
                        }
                }
            })        
            .collect();
        let batch_id = self.inverse_batch_ids.get(&selected_batch_index).unwrap();
        Some((*batch_id, assignment))
    }

    // batch has been uccessfully sent to prover
    pub fn confirm_assignment(&mut self, prover: &str, batch_id: u128) {
        let batch_index = self.batch_ids.get(&batch_id).unwrap();
        self.assigned_batches.entry(*batch_index)
            .and_modify(|batches| {
                batches.push(prover.to_string());
            })
            .or_insert_with(|| {
                vec![prover.to_string()]
            });
        self.prover_batch_assignements.entry(prover.to_string())
            .and_modify(|batches|{
                batches.push(*batch_index);
            })
            .or_insert_with(|| {
                vec![*batch_index]
            });
    }

    pub fn is_finished(&self) -> bool {
        !self.batches.is_empty() &&
        self.batches.len() == self.proofs.len()
    }
}

#[derive(Debug, PartialEq)]
pub enum Stage {
    Aggregate,
    
    Resolve,

    Groth16
}

#[derive(Debug)]
pub struct Pipeline {
    pub stage: Stage,

    pub rounds: Vec<AggRound>,

    // keccak assumptions: <claim_digest, obj>    
    keccak_assumptions: HashMap<Digest, KeccakRequestObject>,
    // zkr assumptions: <claim_digest, obj>
    zkr_assumptions: HashMap<Digest, ZkrRequestObject>,
    // resolved assumptions: <claim_digest, proof i.e. SuccinctReceipt<Unknown>>
    assumption_proofs: HashMap<Digest, Proof>,

    // the final aggregated proof(a SuccinctReceipt)
    pub agg_proof: Option<SuccinctReceipt<ReceiptClaim>>,
    
    // the groth16 proofs: <prover, proof>
    pub groth16_proof: Option<Proof>,

    pub image_id: Digest,
}

impl Pipeline {
    pub fn new(image_id: &[u8]) -> Self {
        Self {
            stage: Stage::Aggregate,            
            rounds: vec![AggRound::new(0, 2)], 
            keccak_assumptions: HashMap::new(),
            zkr_assumptions: HashMap::new(),
            assumption_proofs: HashMap::new(),
            agg_proof: None,
            groth16_proof: None,
            image_id: Digest::from_hex(image_id).unwrap(),
        }
    }

    // number of remaining items to be proved
    pub fn num_outstanding_aggregate_items(&self) -> usize {
        self.rounds
        .last()
        .and_then(|last_round|
            Some(last_round.batches.len() - last_round.proofs.len())
        )
        .or_else(|| Some(0usize))
        .unwrap()
    }

    pub fn num_outstanding_resolve_items(&self) -> usize {
        self.keccak_assumptions.len() +
        self.zkr_assumptions.len() - 
        self.assumption_proofs.len()
    }

    pub fn assign_agg_batch(&mut self, prover: &str) -> Option<(u128, Vec<Assignment>)> {
        self.rounds
            .last_mut()
            .and_then(|cur_round| cur_round.assign_agg_batch(prover))        
    }

    pub fn confirm_assignment(&mut self, prover: &str, batch_id: u128) {
        if let Some(cur_round) = self.rounds.last_mut() {
            cur_round.confirm_assignment(prover, batch_id);
        }
    }

    fn attempt_new_round(&mut self) {
        let prev_round = self.rounds.last().unwrap();        
        if !prev_round.is_finished() {            
            return
        }
        if prev_round.proofs.len() == 1 {
            warn!("Aggregation is finished and we have the final proof.");
            self.stage = Stage::Resolve;
            return
        }
        // batch blob size table
        // a- segment round(max segment blob size ~1mb with po2=21)
        // batch length    total blob size
        // 2               2mb
        // 4               4mb
        // 8               8mb
        //
        // b- join rounds(max proof blob size ~256kb)
        // batch length    total blob size
        // 2               512kb
        // 4               1mb
        // 8               4mb
        let batch_size = match prev_round.proofs.len() {
            2..=16 => 2,
            17..=128 => 4,
            _ => 8,
        };
        let mut new_round = AggRound::new(prev_round.number + 1, batch_size);
        info!(
            "A new round `{}` has begun, there will be up to `{}` batches in total.",            
            prev_round.number + 1,
            prev_round.proofs.len() / batch_size + 1
        );
        for (index, proof) in prev_round.proofs.iter() {
            new_round.feed_input(
                *index,
                Input::ProofInput(proof.clone())
            );
        }
        self.rounds.push(new_round);
    }

    pub fn feed_segment(&mut self, index: usize, blob: Vec<u8>) {
        if self.rounds.len() > 1 {
            warn!("Received segment but we are aggregating proofs.");
            return
        }        
        self.rounds[0].feed_input(
            index,
            Input::SegmentInput(
                Segment{
                    hash: xxh3_128(&blob),
                    blob: blob
                }
            )
        );
    }

    pub fn stop_segment_feeding(&mut self) {
        if self.rounds.len() > 1 {
            warn!("Received stop segment feeding in the wrong round.");
            return
        }
        self.rounds[0].stop_feeding();
    }

    pub fn add_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: String
    ) {
        let round = self.rounds.last_mut().unwrap();
        let batch_index = match round.batch_ids.get(&batch_id) {
            Some(bi) => bi,

            None => {
                //@ maybe its for previous rounds?
                warn!("Unsolicited proof with id `{batch_id}` from `{prover}`.");
                return
            }
        };
        info!("proof `{hash}` for batch_index `{batch_index}`");
        round.proofs
        .entry(*batch_index)
            .and_modify(|proof| {
                //@ majority vote should be taken here, ie hold many hashes and reach consensus
                if proof.hash != hash {
                    warn!("Existing hash `{}` differs from the new hash `{}`.",
                        proof.hash, hash
                    );
                    return
                }
                let _ = proof.provers.insert(prover.clone());
            })
            .or_insert_with(|| {
                Proof {
                    provers: HashSet::from([prover]),
                    blob: None,
                    hash: hash                    
                }
            });
        self.attempt_new_round(); 
    }

    pub fn feed_keccak_assumption(
        &mut self,
        blob: &[u8]
    ) {
        let keccak_req_obj = match bincode::deserialize::<KeccakRequestObject>(blob) {
            Ok(k) => k,

            Err(e) => {
                warn!("Invalid keccak feed: `{e}`");
                return
            }
        };
        let claim_digest: Digest = match keccak_req_obj.claim_digest.try_into() {
            Ok(cd) => cd,
            
            Err(e) => {
                warn!("Invalid keccak feed: `{e}`");
                return
            }
        };
        info!("Received keccak assumption with claim digest: `{claim_digest:?}`");
        if !self.keccak_assumptions.contains_key(&claim_digest) {
            self.keccak_assumptions.insert(
                claim_digest,
                keccak_req_obj
            );
        }
    }

    pub fn feed_zkr_assumption(
        &mut self,
        blob: &[u8]
    ) {
        let zkr_req_obj = match bincode::deserialize::<ZkrRequestObject>(blob) {
            Ok(k) => k,

            Err(e) => {
                warn!("Invalid zkr feed: `{e}`");
                return
            }
        };
        let claim_digest: Digest = match zkr_req_obj.claim_digest.try_into() {
            Ok(cd) => cd,
            
            Err(e) => {
                warn!("Invalid zkr feed: `{e}`");
                return
            }
        };
        info!("Received zkr assumption with claim digest: `{claim_digest:?}`");
        if !self.zkr_assumptions.contains_key(&claim_digest) {
            self.zkr_assumptions.insert(
                claim_digest,
                zkr_req_obj
            );
        }
    }

    pub fn assign_resolve_item(&self) -> Option<ResolveItem> {
        if self.agg_proof.is_none() {
            return None
        }
        // 1: check keccak asumptions
        let outstanding_keccak_items: Vec<_> = self.keccak_assumptions
            .keys()
            .filter(|k|
                !self.assumption_proofs.contains_key(*k)
            )
            .collect();
        let mut rng = rand::rng();
        if !outstanding_keccak_items.is_empty() {
            let claim_digest = outstanding_keccak_items.choose(&mut rng).unwrap();
            let keccak_req_obj = self.keccak_assumptions.get(claim_digest).unwrap();
            let blob = bincode::serialize(keccak_req_obj).unwrap();
            return Some(
                ResolveItem::Keccak(**claim_digest, blob)
            )
        } else {
            // or, 2: check zkr asumptions
            let outstanding_zkr_items: Vec<_> = self.zkr_assumptions
                .keys()
                .filter(|k|
                    !self.assumption_proofs.contains_key(*k)
                )
                .collect();
            if !outstanding_zkr_items.is_empty() {
                let claim_digest = outstanding_zkr_items.choose(&mut rng).unwrap();            
                let zkr_req_obj = self.zkr_assumptions.get(claim_digest).unwrap();                
                let blob = bincode::serialize(zkr_req_obj).unwrap();
                return Some(
                    ResolveItem::Zkr(**claim_digest, blob)
                )
            }
        }
        None
    }

    fn resolve_assumptions(&mut self) {        
        if self.agg_proof.is_none() {
            warn!("Cannot resolve assumptions due to missing aggregated proof.");
            return
        }                
        let r0_client = match ApiClient::from_env() {
            Ok(c) => c,

            Err(e) => {
                warn!("Risc0 client is not available: `{e:?}");
                return
            }
        };
        let opts = ProverOpts::default();
        let conditional_receipt = self.agg_proof.as_ref().unwrap();
        let output = conditional_receipt
            .claim
            .as_value()
            .unwrap()            
            .output
            .as_value()
            .unwrap()
            .as_ref()
            .unwrap();
        let journal = output.journal.as_value().unwrap().clone();
        let assumptions = output
            .assumptions
            .as_value()
            .unwrap();        
        if self.assumption_proofs.len() < assumptions.len() {
            warn!(
                "Not enough assumption proofs to begin resolve, need `{}` more proofs.",
                assumptions.len() - self.assumption_proofs.len()
            );
            return
        }
        info!("Conditional receipt has `{}` assumptions(s) to resolve in total.", assumptions.len());
        let mut succinct_receipt = conditional_receipt.clone();
        for a in assumptions.iter() {
            let assumption = a.as_value().unwrap();
            let assumption_receipt = self.assumption_proofs
                .get(&assumption.claim)
                .unwrap()
                .blob
                .clone()
                .unwrap();
            match r0_client
                .resolve(
                    &opts,
                    succinct_receipt.clone().try_into().unwrap(),
                    Asset::Inline(assumption_receipt.clone().into()),
                    AssetRequest::Inline
                ) 
            {
                Ok(sr) => {
                    succinct_receipt = sr;
                    info!("Assumpton {:?} resolved with success.", assumption.claim);
                },

                Err(e) => {
                    warn!("Failed to resolve assumption: `{e:?}`");
                    continue
                }
            };
        }
        info!("All assumptions have been resolved, let's verify the aggregated proof now.");
        let receipt = Receipt::new(
            InnerReceipt::Succinct(succinct_receipt.clone()),
            journal,
        );
        match r0_client.verify(
            receipt.try_into().unwrap(),
            self.image_id
        ) {
            Ok(_) => {
                self.stage = Stage::Groth16;
                info!("Verified! let's extract a Groth16 proof.");
            },

            Err(e) => {
                warn!("Failed to verify: `{e:?}`");
                return
            } 
        }
        self.agg_proof = Some(succinct_receipt);
    }

    pub fn add_assumption_proof(
        &mut self,
        claim_digest: Digest,
        blob: Vec<u8>,
        prover: String
    ) {
        if !self.keccak_assumptions.contains_key(&claim_digest) &&
           !self.zkr_assumptions.contains_key(&claim_digest)
        {
            warn!("Unsolicited assumption proof with claim `{claim_digest:?}` from `{prover}`.")
        }
        let hash = xxh3_128(&blob);
        if let Err(e) = bincode::deserialize::<SuccinctReceipt<Unknown>>(&blob) {            
            warn!("Assumption proof is invalid: `{e:?}`");
            return                
        }
        self.assumption_proofs.entry(claim_digest)
            .and_modify(|proof| {
                let _ = proof.provers.insert(prover.clone());
                warn!(
                    "Replacing assumption proof with claim `{:?}` from `{}` with the existing one.",
                    claim_digest,
                    prover
                );
            })
            .or_insert_with(|| {
                Proof {
                    provers: HashSet::from([prover]),
                    blob: Some(blob),
                    hash: hash 
                }
            });
        self.resolve_assumptions();
    }

    pub fn add_agg_proof(
        &mut self,
        prover_id: &str,
        blob: &Vec<u8>
    ) {
        let hash = xxh3_128(&blob);
        if self.stage != Stage::Resolve {
            warn!("Received unsolicited aggregated proof `{hash}`.");
            return
        }
        let last_round = self.rounds.last().unwrap();
        let proof = last_round.proofs.values().nth(0).unwrap();
        if proof.hash != hash {
            warn!("STARK proof's hash `{}` differs from the expected one `{}`",
                hash,
                proof.hash
            );
            return
        }
        info!("Received aggregated proof `{hash}` from `{prover_id}`");
        if !proof.provers.contains(prover_id) {
            warn!("Prover is not among the provers who generated the proof.");
        }
        self.agg_proof = match bincode::deserialize::<SuccinctReceipt<ReceiptClaim>>(&blob) {
            Ok(sr) => Some(sr),

            Err(e) => {
                warn!("Proof is invalid: `{e:?}`");
                return
            }

        };
        self.resolve_assumptions();
    }

    // pub fn agg_proof_owners() -> Vec<(String, u128) {
    //     self.rounds
    //         .last()
    //         .unwrap()
    //         .proofs
    //         .values()
    //         .last()
    //         .unwrap()
    // }

    pub fn add_groth16_proof(
        &mut self,
        blob: Vec<u8>,
        prover: &str
    ) {
        let hash = xxh3_128(&blob);
        if self.stage != Stage::Groth16 {
            warn!("Received unsolicited Groth16 proof `{hash}`.");
            return
        }
        info!("Received Groth16 proof `{hash}` from `{prover}`");
        let groth16_receipt = match bincode::deserialize::<Groth16Receipt<ReceiptClaim>>(&blob) {
            Ok(g) => g,

            Err(e) => {
                warn!("Groth16 proof is invalid: `{e:?}`");
                return
            }

        };
        match self.groth16_proof {
            None => {
                self.groth16_proof = Some(
                    Proof {                                                    
                        provers: HashSet::from([prover.to_string()]),
                        blob: Some(blob),
                        hash: hash
                    }
                );
            }

            Some(ref mut proof) => {
                if hash != proof.hash {
                    warn!("Existing hash `{}` differs from the new hash `{}`",
                        proof.hash, hash
                    );
                    return
                }
                proof.provers.insert(prover.to_string());
            }
        };
        // on-chain verification baby!
        let output = groth16_receipt
            .claim
            .as_value()
            .unwrap()
            .output
            .as_value()
            .unwrap()
            .as_ref()
            .unwrap();
        let receipt = Receipt::new(
            InnerReceipt::Groth16(groth16_receipt.clone()),
            output.journal.as_value().unwrap().clone(),
        );
        let r0_client = match ApiClient::from_env() {
            Ok(c) => c,

            Err(e) => {
                warn!("Risc0 client is not available: `{e:?}");
                return
            }
        };
        if let Ok(_) = r0_client
            .verify(
                receipt.try_into().unwrap(),
                self.image_id
            )
        {
            info!("Groth16 proof is verified, viola.");
        }
    }
}
