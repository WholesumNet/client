use std::{
    collections::{
        HashMap,
    },
    time::{
        Instant,
    },
};
use log::warn;
use uuid::Uuid;
use libp2p::PeerId;

// round input/output
#[derive(Debug, Clone)]
pub struct Token {
    pub owner: PeerId,
    pub hash: u128
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub batch_id: u128,
    pub prover: PeerId,
    // assignment time. helps to handle timeouts.
    pub when: Instant,
}

#[derive(Debug, Clone)]
pub struct Batch {
    pub id: u128,
    pub inputs: Vec<Token>,
    pub proof: Option<Token>,
}

#[derive(Debug)]
pub struct Round {
    pub number: usize,        
    pub batch_size: usize,
    pub batches: HashMap<u128, Batch>,
    // it's handy to have two views.
    pub prover_assignments: HashMap<PeerId, Assignment>,
    pub batch_prover_assignments: HashMap<u128, PeerId>,
}

impl Round {
    pub fn new(number: usize, batch_size: usize) -> Self {
        assert_ne!(batch_size, 0usize);
        Self {
            number: number,
            batch_size: batch_size,
            batches: HashMap::new(),
            prover_assignments: HashMap::new(),
            batch_prover_assignments: HashMap::new()
        }
    }

    pub fn reset(&mut self) {
        self.batches.clear();
        self.prover_assignments.clear();
        self.batch_prover_assignments.clear();
    }

    pub fn feed(&mut self, inputs: &[Token]) {
        let num_batches = inputs.len() / self.batch_size;
        let num_rem_items = inputs.len() % self.batch_size;
        for bi in 0..num_batches {            
            let start = bi * self.batch_size;
            let mut end = start + self.batch_size;
            if bi == num_batches - 1 {
                //@ or last batch as a standalone batch
                end += num_rem_items;
            }
            let batch = Batch {
                id: Uuid::new_v4().as_u128(),
                inputs: inputs[start..end].to_vec(),
                proof: None
            };
            self.batches.insert(batch.id, batch);
        }        
    }

    // assign a batch to prover
    pub fn assign(
        &mut self,
        prover: &PeerId,
    ) -> Option<(u128, Vec<Token>)> {        
        if self.prover_assignments.contains_key(prover) {
            return None
        }        
        let outstanding_batches: Vec<_> = self.batches.values()
            .filter(|b|
                b.proof.is_none() &&
                !self.batch_prover_assignments.contains_key(&b.id)
            )
            .collect();
        if outstanding_batches.is_empty() {
            return None
        }
        let batch = outstanding_batches.first().unwrap();
        self.prover_assignments.insert(
            prover.clone(),
            Assignment {
                batch_id: batch.id,
                prover: prover.clone(),
                when: Instant::now()
            }
        );
        self.batch_prover_assignments.insert(batch.id, prover.clone());

        Some((batch.id, batch.inputs.clone()))
    }

    pub fn add_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: PeerId,
    ) -> bool {
        if !self.batches.contains_key(&batch_id) {
            warn!("Unsolicited proof for batch(`{batch_id}`) from `{prover:?}`.");
            return false;
        };
        let batch = self.batches.get_mut(&batch_id).unwrap();
        batch.proof = Some(Token {
            owner: prover.clone(),
            hash: hash
        });
        self.prover_assignments.remove(&prover);
        self.batch_prover_assignments.remove(&batch_id);
        true
    }

    pub fn is_finished(&self) -> bool {
        self.batches.values()
            .all(|b| b.proof.is_some())
    }

    pub fn proofs(&self) -> Vec<Token> {
        if self.batches.values().any(|b| b.proof.is_none()) {
            warn!("There are unproved batches for this `proofs` request.");
        }
        self.batches.values()
            .filter_map(|b| b.proof.clone())
            .collect::<Vec<Token>>()
    }

    pub fn remove_stale_assignments(&mut self) {
        // 30 minutes        
    }
}
