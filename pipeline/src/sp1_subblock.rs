use std::{
    fs,
    collections::HashMap
};
// use serde::Deserialize;
use log::{
    info, warn
};
use uuid::Uuid;

use crate::round;
use round::{Round, Input};

#[derive(Debug, PartialEq)]
pub enum Stage {
    Subblock,
    Agg,
}

#[derive(Debug)]
pub struct Pipeline {
    // job id
    pub id: u128,

    pub stage: Stage,

    subblock_round: Round,    
    agg_round: Round,    
}

impl Pipeline {
    pub fn new() -> anyhow::Result<Self> {        
        Ok(Self {
            id: Uuid::new_v4().as_u128(),
            stage: Stage::Subblock,
            subblock_round: Round::new(0, 1),            
            agg_round: Round::new(2047, 1),
        })
    }
    
    // subblock round
    pub fn feed_subblock_stdin(&mut self, index: usize, blob_hash: u128) {        
        self.subblock_round.feed_input(
            index,
            Input {
                owner_peer_id: None,
                hash: blob_hash
            }
        );
    }

    pub fn stop_subblock_stdin_feeding(&mut self) {
        self.subblock_round.stop_feeding();
    }

    pub fn assign_subblock_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.subblock_round.assign_batch(prover)        
    }

    pub fn confirm_subblock_batch_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.subblock_round.confirm_assignment(prover, batch_id);
    }

    pub fn add_subblock_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: Vec<u8>
    ) {
        let batch_index = match self.subblock_round.batch_ids.get(&batch_id) {
            Some(bi) => bi,

            None => {
                //@ maybe its for previous rounds?
                warn!("Unsolicited proof with id `{batch_id}` from `{prover:?}`.");
                return
            }
        };
        let proof = Input {
            owner_peer_id: Some(prover.clone()),
            hash: hash
        };
        self.subblock_round.proofs
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
        if self.subblock_round.is_finished() {
            info!("Subblock round is finished and agg round begins.");
            self.stage = Stage::Agg;
        }
    }

    pub fn subblock_proofs(&self) -> Vec<Input> {
        // <batch index, <prover, proof>>
        // pub proofs: BTreeMap<usize, HashMap<Vec<u8>, Input>>,
        self.subblock_round
            .proofs
            .values()
            .map(|p| p.values().next().unwrap().clone())
            .collect()
    }

    // agg round
    pub fn feed_agg_stdin(&mut self, index: usize, blob_hash: u128) {
        self.agg_round.feed_input(
            index,
            Input {
                owner_peer_id: None,
                hash: blob_hash
            }
        );
    }

    pub fn stop_agg_stdin_feeding(&mut self) {
        self.agg_round.stop_feeding();
    }

    pub fn assign_agg_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.agg_round.assign_batch(prover)        
    }

    pub fn confirm_agg_batch_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.agg_round.confirm_assignment(prover, batch_id);
    }

    pub fn add_agg_proof(&mut self, proof: Vec<u8>) {
        let _ = fs::write("./agg_proof", &proof);
        // verify it!
    }


}
