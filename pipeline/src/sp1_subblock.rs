use std::collections::HashMap;
// use serde::Deserialize;
use log::{
    info, warn
};
use xxhash_rust::xxh3::xxh3_128;
use uuid::Uuid;
// use rs_merkle::{MerkleTree, algorithms::Sha256, Hasher};

use crate::round;
use round::{Round, Input};

#[derive(Debug, PartialEq)]
pub enum Stage {
    ExecuteSubblock,
    ExecuteAgg,
}

#[derive(Debug, Clone)]
struct Blob {
    data: Vec<u8>,

    chunks: Vec<BlobChunk>,
}

#[derive(Debug, Clone)]
struct BlobChunk {
    // start byte    
    start_index: usize,

    // end byte
    end_index: usize,

    // chunk hash
    hash: u128,
}

#[derive(Debug)]
pub struct Pipeline {
    // job id
    pub id: u128,

    pub stage: Stage,

    blobs: HashMap<u128, Blob>,

    execute_subblock_round: Round,    
    execute_agg_round: Round,    
}

impl Pipeline {
    pub fn new() -> anyhow::Result<Self> {        
        Ok(Self {
            id: Uuid::new_v4().as_u128(),
            stage: Stage::ExecuteSubblock,
            blobs: HashMap::new(),
            execute_subblock_round: Round::new(0, 1),            
            execute_agg_round: Round::new(2047, 1),
        })
    }

    pub fn get_blob_info(&self, hash: u128) -> Option<usize> {
        if !self.blobs.contains_key(&hash) {
            warn!("No such blob to get info of: `{hash}`.");
            None
        } else {
            Some(self.blobs.get(&hash).unwrap().chunks.len())
        }
    }

    pub fn get_blob_chunk(&self, hash: u128, index: usize) -> Option<(Vec<u8>, u128)> {
        if !self.blobs.contains_key(&hash) {
            warn!("No such blob to get chunk of: `{hash}`.");
            return None
        }
        let blob = self.blobs.get(&hash).unwrap();
        if index >= blob.chunks.len() {
            warn!("Chunk index `{index}` of blob `{hash}` is out of range.");
            return None
        } 
        let chunk = blob.chunks.get(index).unwrap();
        Some((
            blob.data.get(chunk.start_index..=chunk.end_index).unwrap().to_vec(),
            chunk.hash
        ))
    }
    
    // subblock round
    pub fn feed_subblock_stdin(&mut self, index: usize, blob: Vec<u8>) {
        // chunk the stdin blob
        let chunks = chunk_blob(&blob);        
        info!("Subblock blob `{index}`: `{}` bytes, `{}` chunks", blob.len(), chunks.len());
        let hash = xxh3_128(&blob);
        self.blobs.insert(
            hash,
            Blob {
                data: blob,
                chunks: chunks,
            }
        );
        self.execute_subblock_round.feed_input(
            index,
            Input {
                owner_peer_id: None,
                hash: hash
            }
        );
    }

    pub fn stop_subblock_stdin_feeding(&mut self) {
        self.execute_subblock_round.stop_feeding();
    }

    pub fn assign_execute_subblock_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.execute_subblock_round.assign_batch(prover)        
    }

    pub fn confirm_execute_subblock_batch_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.execute_subblock_round.confirm_assignment(prover, batch_id);
    }

    pub fn add_execute_subblock_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: Vec<u8>
    ) {
        let batch_index = match self.execute_subblock_round.batch_ids.get(&batch_id) {
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
        self.execute_subblock_round.proofs
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
        if self.execute_subblock_round.is_finished() {
            info!("Subblock execution round is finished. Agg round begins.");
            self.stage = Stage::ExecuteAgg;
        }
    }

    // agg round
    pub fn feed_agg_stdin(&mut self, index: usize, blob: Vec<u8>) {
        // chunk the stdin blob
        let chunks = chunk_blob(&blob);        
        info!("Agg blob `{index}`: `{}` bytes, `{}` chunks", blob.len(), chunks.len());
        let hash = xxh3_128(&blob);
        self.blobs.insert(
            hash,
            Blob {
                data: blob,
                chunks: chunks,
            }
        );
        self.execute_agg_round.feed_input(
            index,
            Input {
                owner_peer_id: None,
                hash: hash
            }
        );
    }

    pub fn stop_agg_stdin_feeding(&mut self) {
        self.execute_agg_round.stop_feeding();
    }

    pub fn assign_execute_agg_batch(
        &mut self,
        prover: &Vec<u8>
    ) -> Option<(u128, Vec<Input>)> {
        self.execute_agg_round.assign_batch(prover)        
    }

    pub fn confirm_execute_agg_batch_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        self.execute_agg_round.confirm_assignment(prover, batch_id);
    }
}

fn chunk_blob(blob: &Vec<u8>) -> Vec<BlobChunk> {
    let chunk_size = 256 * 1_000;
    let mut chunks: Vec<BlobChunk> = Vec::new();
    for i in 0..=(blob.len() / chunk_size) {
        let start_index = i * chunk_size;
        let end_index = std::cmp::min(blob.len(), (i + 1) * chunk_size) - 1;
        // info!("chunk {i}: `{start_index}`-`{end_index}`");
        let chunk = blob.get(start_index..=end_index).unwrap();
        let hash = xxh3_128(chunk);
        chunks.push(BlobChunk {
            start_index: start_index,
            end_index: end_index,
            hash: hash,
        });
    }
    chunks
}