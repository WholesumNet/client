use std::{ 
    collections::{BTreeMap, HashMap},
    fs,
};
use serde::Deserialize;
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
    AssetRequest,
    Digest,
};
use hex::FromHex;

// job template as read in(e.g. from disk)
#[derive(Debug, Deserialize)]
pub struct Schema {    
    pub image_id: String
}

#[derive(Debug, Clone)]
pub struct Proof {
    pub hash: u128,
}

#[derive(Debug, Clone)]
pub enum Input {    
    // blob, used for the first round(segments)
    Blob(Vec<u8>),

    // (prover, proof)
    Token(Vec<u8>, Proof)
}

#[derive(Debug)]
pub struct Round {
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
    pub assigned_batches: HashMap<usize, Vec<Vec<u8>>>,
    // helper(^) reverse map to complement "assigned_batches": <prover, assigned batches>
    pub prover_batch_assignements: HashMap<Vec<u8>, Vec<usize>>,

    // <batch index, <prover, proof>>
    pub proofs: BTreeMap<usize, HashMap<Vec<u8>, Proof>>,
    // helper(^) reverse map to complement "proofs": <prover, batches>
    pub prover_proofs: HashMap<Vec<u8>, Vec<usize>>,
}

impl Round {
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
        // info!("New input with index `{index}` has arrived.");
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
        // info!("partial-batches: {:?}", self.partial_batches);
        // info!("batches: {:?}", self.batches);
    }

    // no new input is expected, so upgrade the partial batch into a full batch
    pub fn stop_feeding(&mut self) {
        if self.partial_batches.is_empty() {
            return
        }
        let (index, batch) = self.partial_batches.first_key_value().unwrap();
        let last_batch: Vec<_> = batch
            .iter()
            .filter_map(|item| if *item != 0 { Some(*item) } else { None })
            .collect();
        if last_batch.len() == 1 {
            // add it to the last batch
            self.batches
                .values_mut()
                .last()
                .unwrap()
                .push(last_batch.into_iter().next().unwrap());
        } else {
            self.batches.insert(*index, last_batch);
            let batch_id = Uuid::new_v4().as_u128();
            self.batch_ids.insert(batch_id, *index);
            self.inverse_batch_ids.insert(*index, batch_id);            
        }
        info!(
            "No more feeds for round `{}`, inputs: `{}`, batches: `{}`",
            self.number,
            self.inputs.len(),
            self.batches.len()
        );
    }

    // assign a batch to prover
    pub fn assign_batch(&self, prover: &Vec<u8>) -> Option<(u128, Vec<Input>)> {        
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
            // info!("round: {}, b: {:?}", self.number, self.batches);
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
        // info!("outstanding: {outstanding_batches:?}, sbi: {selected_batch_index}, batches: {:?}", self.batches);
        // let selected_batch = outstanding_batches[selected_batch_index];
        let assignment = self.batches[&selected_batch_index]
            .iter()
            .map(|i| {
                let input = self.inputs.get(i).cloned().unwrap();
                input
            })        
            .collect();
        let batch_id = self.inverse_batch_ids.get(&selected_batch_index).unwrap();
        Some((*batch_id, assignment))
    }

    // batch has been successfully sent to prover
    pub fn confirm_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        let batch_index = self.batch_ids.get(&batch_id).unwrap();
        self.assigned_batches.entry(*batch_index)
            .and_modify(|batches| {
                batches.push(prover.clone());
            })
            .or_insert_with(|| {
                vec![prover.clone()]
            });
        self.prover_batch_assignements.entry(prover.clone())
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
    // prove segments and join them
    Aggregate,
    
    // prove keccak requests and union them
    Assumption,

    // poll and resolve the final keccak receipt
    Resolve,

    // extract groth16 proof
    Groth16
}

#[derive(Debug)]
pub struct Pipeline {
    // job id
    pub id: u128,

    pub stage: Stage,

    agg_rounds: Vec<Round>,    
    // the final aggregated proof(a SuccinctReceipt)
    //@ the prover?
    pub agg_proof: Option<SuccinctReceipt<ReceiptClaim>>,

    ass_rounds: Vec<Round>,
    // the final assumption proof(a SuccinctReceipt)
    pub ass_proof: Option<SuccinctReceipt<Unknown>>,

    groth16_round: Round,

    pub image_id: Digest,
}

const GROTH16_ROUND_NUMBER: usize = 8_191;

impl Pipeline {
    pub fn new(job_file: &str) -> anyhow::Result<Self> {
        let schema: Schema = toml::from_str(
            &fs::read_to_string(job_file)?
        )?;   

        Ok(Self {
            id: Uuid::new_v4().as_u128(), 
            stage: Stage::Aggregate,            
            agg_rounds: vec![Round::new(0, 2)], 
            agg_proof: None,
            ass_rounds: vec![Round::new(0, 2)],
            ass_proof: None,
            groth16_round: Round::new(GROTH16_ROUND_NUMBER, 1),
            image_id: Digest::from_hex(schema.image_id.as_bytes()).unwrap(),
        })
    }

    pub fn cur_agg_round_number(&self) -> usize {
        self.agg_rounds.last().unwrap().number
    }

    pub fn assign_agg_batch(&mut self, prover: &Vec<u8>) -> Option<(u128, Vec<Input>)> {
        self.agg_rounds
            .last_mut()
            .and_then(|last_round| last_round.assign_batch(prover))        
    }

    pub fn confirm_agg_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        if let Some(cur_round) = self.agg_rounds.last_mut() {
            cur_round.confirm_assignment(prover, batch_id);
        }
    }

    fn attempt_new_agg_round(&mut self) {
        let prev_round = self.agg_rounds.last().unwrap();        
        if !prev_round.is_finished() {            
            return
        }
        if prev_round.proofs.len() == 1 {
            warn!("Aggregation is finished and we have the final proof.");
            self.stage = Stage::Assumption;
            return
        }
        // batch blob size table
        // a- segment round(max segment blob size ~10mb with po2=21)
        // batch length    total blob size
        // 2               20mb
        // 4               40mb
        // 8               80mb
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
        let mut new_round = Round::new(prev_round.number + 1, batch_size);
        info!("A new round `{}` has begun", prev_round.number + 1);
        for (index, proofs) in prev_round.proofs.iter() {
            // use the first proof
            let (prover, proof) = proofs
                .iter()
                .next()
                .unwrap();
            new_round.feed_input(*index, Input::Token(prover.clone(), proof.clone()));
        }
        new_round.stop_feeding();
        self.agg_rounds.push(new_round);
    }

    pub fn feed_segment(&mut self, index: usize, blob: Vec<u8>) {
        self.agg_rounds[0].feed_input(index, Input::Blob(blob));
    }

    pub fn stop_segment_feeding(&mut self) {
        self.agg_rounds[0].stop_feeding();
    }

    pub fn add_agg_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: Vec<u8>
    ) {
        let round = self.agg_rounds.last_mut().unwrap();
        let batch_index = match round.batch_ids.get(&batch_id) {
            Some(bi) => bi,

            None => {
                //@ maybe its for previous rounds?
                warn!("Unsolicited proof with id `{batch_id}` from `{prover:?}`.");
                return
            }
        };
        let proof = Proof {
            hash: hash
        };
        round.proofs
        .entry(*batch_index)
            .and_modify(|proofs| {
                proofs.entry(prover.clone())
                    .and_modify(|proof| {
                        warn!(
                            "Old proof `{}` is replaced by new proof `{}`",
                            proof.hash,
                            hash
                        );
                        proof.hash = hash;
                    })
                    .or_insert_with(|| proof.clone());                
            })
            .or_insert_with(|| HashMap::from([(prover, proof)]));
        self.attempt_new_agg_round(); 
    }

    pub fn feed_assumption(
        &mut self,
        blob: &[u8]
    ) {            
        let first_round = self.ass_rounds.first_mut().unwrap();
        first_round.feed_input(
            first_round.inputs.len(),
            Input::Blob(blob.into())
        );
    }

    pub fn stop_assumption_feeding(&mut self) {        
        self.ass_rounds
            .first_mut()
            .unwrap()
            .stop_feeding();
    }

    pub fn assign_assumption_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.ass_rounds
            .last_mut()
            .unwrap()
            .assign_batch(prover)
    }

    pub fn cur_ass_round_number(&self) -> usize {
        self.ass_rounds.last().unwrap().number
    }

    pub fn confirm_assumption_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.ass_rounds
            .last_mut()
            .unwrap()
            .confirm_assignment(prover, batch_id);
    }

    fn attempt_new_ass_round(&mut self) {
        let prev_round = self.ass_rounds.last().unwrap();        
        if !prev_round.is_finished() {            
            return
        }
        if prev_round.proofs.len() == 1 {
            warn!("Assumption aggregation is finished and we have the final proof.");
            return
        }
        let batch_size = match prev_round.proofs.len() {
            2..=16 => 2,
            17..=128 => 4,
            _ => 8,
        };
        let mut new_round = Round::new(prev_round.number + 1, batch_size);
        info!(
            "A new assumption round `{}` has begun, there will be up to `{}` batches in total.",            
            prev_round.number + 1,
            prev_round.proofs.len() / batch_size + 1
        );
        for (index, proofs) in prev_round.proofs.iter() {
            // use the first proof
            let (prover, proof) = proofs
                .iter()
                .next()
                .unwrap();
            new_round.feed_input(*index, Input::Token(prover.clone(), proof.clone()));
        }
        new_round.stop_feeding();
        self.ass_rounds.push(new_round);
    }

    fn resolve_assumptions(&mut self) {        
        if self.agg_proof.is_none() {
            warn!("Aggregated proof is missing, cannot resolve assumptions.");
            return
        }                                
        let conditional_receipt = self.agg_proof.as_ref().unwrap();           
        let assumption_receipt = self.ass_proof.as_ref().unwrap();
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

        let r0_client = match ApiClient::from_env() {
            Ok(c) => c,

            Err(e) => {
                warn!("Risc0 client is not available: `{e:?}");
                return
            }
        };
        let opts = ProverOpts::default();        
    
        let unconditional_receipt = match r0_client
            .resolve(
                &opts,
                conditional_receipt.clone().try_into().unwrap(),
                assumption_receipt.clone().try_into().unwrap(),
                AssetRequest::Inline
            ) 
        {
            Ok(sr) => {
                info!("Assumpton {:?} resolved with success.", assumption_receipt.claim);
                sr
            },

            Err(e) => {
                warn!("Failed to resolve assumption: `{e:?}`");
                return
            }
        };
        info!("All asumptions have been resolved, let's verify the aggregated proof now.");
        let receipt = Receipt::new(
            InnerReceipt::Succinct(unconditional_receipt.clone()),
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
        let sr_blob = bincode::serialize(&unconditional_receipt).unwrap();
        self.groth16_round.feed_input(0, Input::Blob(sr_blob));
        self.agg_proof = Some(unconditional_receipt);
    }

    pub fn add_assumption_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: Vec<u8>
    ) {
        let last_round = self.ass_rounds.last_mut().unwrap();
        let batch_index = match last_round.batch_ids.get(&batch_id) {
            Some(bi) => bi,

            None => {
                //@ maybe its for previous rounds?
                warn!("Unsolicited assumption proof with id `{batch_id}` from `{prover:?}`.");
                return
            }
        };
        let proof = Proof {
            hash: hash
        };
        last_round.proofs
        .entry(*batch_index)
            .and_modify(|proofs| {
                proofs.entry(prover.clone())
                    .and_modify(|proof| {
                        warn!(
                            "Old proof `{}` is replaced by new proof `{}`",
                            proof.hash,
                            hash
                        );
                        proof.hash = hash;
                    })
                    .or_insert_with(|| proof.clone());                
            })
            .or_insert_with(|| HashMap::from([(prover, proof)]));
        self.attempt_new_ass_round();
        if self.ass_rounds.last().unwrap().is_finished() {
            info!("Assumption aggregaion is finished.");
            self.stage = Stage::Resolve;            
        } 
    }

    pub fn add_final_ass_proof(
        &mut self,
        prover: Vec<u8>,
        blob: Vec<u8>
    ) {
        let hash = xxh3_128(&blob);
        if self.stage != Stage::Resolve {
            warn!("Received unsolicited assumption proof `{hash}`.");
            return
        }
        let last_round = self.ass_rounds.last().unwrap();
        let proofs = last_round.proofs.values().nth(0).unwrap();
        let proof = proofs.values().nth(0).unwrap();
        if proof.hash != hash {
            warn!("Received final ass proof `{}` differs from the requested one `{}`.",
                hash,
                proof.hash
            );
            return
        }
        if !proofs.contains_key(&prover) {
            warn!("The peer is not among the provers who generated the proof.");
        }
        self.ass_proof = match bincode::deserialize::<SuccinctReceipt<Unknown>>(&blob) {
            Ok(sr) => {
                Some(sr)
            },

            Err(e) => {
                warn!("Proof is invalid: `{e:?}`");
                return
            }

        };
        self.resolve_assumptions();
    }   

    pub fn add_final_agg_proof(
        &mut self,
        prover: Vec<u8>,
        blob: Vec<u8>
    ) {
        let hash = xxh3_128(&blob);
        if self.stage != Stage::Assumption {
            warn!("Received unsolicited aggregated proof `{hash}`.");
            return
        }
        let last_round = self.agg_rounds.last().unwrap();
        let proofs = last_round.proofs.values().nth(0).unwrap();
        let proof = proofs.values().nth(0).unwrap();
        if proof.hash != hash {
            warn!("Received final agg proof `{}` differs from the requested one `{}`.",
                hash,
                proof.hash
            );
            return
        }
        if !proofs.contains_key(&prover) {
            warn!("The peer is not among the provers who generated the proof.");
        }
        self.agg_proof = match bincode::deserialize::<SuccinctReceipt<ReceiptClaim>>(&blob) {
            Ok(sr) => Some(sr),

            Err(e) => {
                warn!("Proof is invalid: `{e:?}`");
                return
            }

        };
        // self.resolve_assumptions();
    }

    pub fn final_agg_proof_token(&self) -> (Vec<u8>, u128) {
        self.agg_rounds
            .last()
            .unwrap()
            .proofs
            .values()
            .nth(0)
            .unwrap()
            .iter()
            .nth(0)
            .map(|(prover, proof)| (prover.clone(), proof.hash))
            .unwrap()
    }

    pub fn final_ass_proof_token(&self) -> (Vec<u8>, u128) {
        self.ass_rounds
            .last()
            .unwrap()
            .proofs
            .values()
            .nth(0)
            .unwrap()
            .iter()
            .nth(0)
            .map(|(prover, proof)| (prover.clone(), proof.hash))
            .unwrap()
    }

    pub fn assign_groth16_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.groth16_round.assign_batch(prover)
    }

    pub fn confirm_groth16_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.groth16_round.confirm_assignment(prover, batch_id);
    }

    pub fn add_groth16_proof(
        &mut self,
        batch_id: u128,
        blob: &Vec<u8>,
        prover: Vec<u8>
    ) {
        let groth16_receipt = match bincode::deserialize::<Groth16Receipt<ReceiptClaim>>(blob) {
            Ok(g) => g,

            Err(e) => {
                warn!("Groth16 proof is invalid: `{e:?}`");
                return
            }
        };
        let batch_index = match self.groth16_round.batch_ids.get(&batch_id) {
            Some(bi) => bi,

            None => {
                //@ maybe its for previous rounds?
                warn!("Unsolicited Groth16 proof with id `{batch_id}` from `{prover:?}`.");
                return
            }
        };
        let hash = xxh3_128(blob);        
        let proof = Proof {
            hash: hash
        };
        self.groth16_round.proofs
        .entry(*batch_index)
            .and_modify(|proofs| {
                proofs.entry(prover.clone())
                    .and_modify(|op| {
                        warn!(
                            "Replaced the old agg proof `{}` with a new one `{}`.",                        
                            op.hash, 
                            hash
                        );
                        *op = proof.clone();
                    })
                    .or_insert_with(|| proof.clone());
            })
            .or_insert_with(|| {
                HashMap::from([(prover, proof)])
            });
        // off-chain verification baby!
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
                //@ wtd?
                warn!("Risc0 client is not available: `{e:?}");
                return
            }
        };
        if let Ok(_) = r0_client
            .verify(
                receipt.clone().try_into().unwrap(),
                self.image_id
            )
        {
            info!("Groth16 proof is verified, viola!");
            fs::write("groth16.proof", bincode::serialize(&receipt).unwrap()).unwrap();
        }
    }
}
