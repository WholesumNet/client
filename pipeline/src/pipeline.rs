use std::{
    time::Instant,
    collections::{
        HashMap,
        VecDeque
    },
};
use log::{
    info, warn
};
use xxhash_rust::xxh3::xxh3_128;
use uuid::Uuid;
use libp2p::PeerId;
use crate::round;
use round::{
    Round,
    Token,
};

use crate::verify;
use verify::SP1Handle;

#[derive(Debug, PartialEq)]
pub enum Stage {
    Subblock,
    Agg,
    Verify,
}

pub struct Pipeline {
    // <block number, [stdins]>
    block_blob_hashes: HashMap::<u64, Vec<u128>>,
    current_block: Option<u64>,
    outstanding_blocks: VecDeque::<u64>,

    blob_store: HashMap::<u128, Vec<u8>>,

    // <block number, agg proof blob hash>
    archived_blocks: HashMap<u64, u128>,

    // job id, unique per block
    pub id: u128,
    
    // measure proving time
    start_time: Instant,    
    pub stage: Stage,
    subblock_round: Round,    
    agg_round: Round,    

    // used to verify proofs
    sp1_handle: SP1Handle,
}

impl Pipeline {
    // 120 seconds
    const BLOCK_TIMEOUT: u64 = 120;

    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            block_blob_hashes: HashMap::new(),
            current_block: None,
            outstanding_blocks: VecDeque::new(),

            blob_store: HashMap::new(),

            archived_blocks: HashMap::new(),

            id: 0u128,
            stage: Stage::Verify,
            //@ calling now() here is useless
            start_time: Instant::now(),
            subblock_round: Round::new(1usize),
            agg_round: Round::new(1usize),

            sp1_handle: SP1Handle::new()?,
        })        
    }

    pub fn get_blob(&self, hash: &u128) -> Option<&Vec<u8>> {
        self.blob_store.get(hash)
    }

    pub fn add_block(
        &mut self,
        block_number: u64,
        stdins: Vec<Vec<u8>>
    ) {
        if self.archived_blocks.contains_key(&block_number) {
            warn!(
                "Block(`{}`) is already prved.",
                block_number
            );
            return
        }
        if stdins.is_empty() {
            warn!(
                "Detected an empty block(`{}`).",
                block_number
            );
            return
        }
        info!(
            "Received a new block: `{}` with `{}` stdin blobs.",
            block_number,
            stdins.len()
        );
        if stdins.len() < 2 {
            warn!("At least 2 stdin blobs are required.");
            return
        }
        self.outstanding_blocks.push_back(block_number);        
        let mut blob_hashes = Vec::<u128>::with_capacity(stdins.len());
        for blob in stdins.into_iter() {
            let hash = xxh3_128(&blob);
            self.blob_store.insert(hash, blob);
            blob_hashes.push(hash);
        }
        self.block_blob_hashes.insert(block_number, blob_hashes);
        if self.current_block.is_none() {
            self.begin_next_block();
        }
    }

    fn begin_next_block(&mut self) {        
        if self.stage != Stage::Verify {
            warn!(
                "Stage must be `Verify` to begin the next block: {:?}",
                self.stage
            );
            return;
        }
        
        self.current_block = self.outstanding_blocks.pop_front();        
        if self.current_block.is_none() {
            warn!("All blocks are caught up, no more to prove.");
            return;
        }
        let block_number = self.current_block.clone().unwrap();

        self.start_time = Instant::now();
        self.id = Uuid::new_v4().as_u128();
        self.stage = Stage::Subblock;        
        let num_subblocks = self.feed_stdins(&block_number);
        info!(
            "Started block `{}`: `{}` subblock{} + the aggregation to prove.",
            block_number,
            num_subblocks,
            if num_subblocks == 1 { "" } else { "s" }
        );
    }

    fn feed_stdins(
        &mut self,
        block_number: &u64
    ) -> usize {
        let blob_hashes = self.block_blob_hashes.get(block_number).unwrap();
        let mut tokens: Vec<_> = blob_hashes.into_iter()
            .map(|h| Token {
                owner: None,
                hash: *h
            })
            .collect();
        let agg_token = tokens.split_off(tokens.len() - 1);
        self.subblock_round.feed(&tokens);
        self.agg_round.feed(&agg_token);
        tokens.len()
    }

    pub fn assign(
        &mut self,
        prover: &PeerId
    )-> Option<(u128, Vec<Token>)> {
        match self.stage {
            Stage::Subblock => {
                self.subblock_round.assign(&prover)                
            },

            Stage::Agg => {
                if let Some((batch_id, agg_stdin)) = 
                    self.agg_round.assign(&prover)
                {                
                    let mut tokens = vec![agg_stdin.into_iter()
                        .next()
                        .unwrap()
                    ];
                    tokens.extend(self.subblock_round.proofs());
                    Some((batch_id, tokens))
                } else {
                    None
                }
            },

            _ => None,
        }
    }

    pub fn revoke_stale_assignments(&mut self) {
        if self.current_block.is_none() {
            return;
        }
        if self.start_time.elapsed().as_secs() > Self::BLOCK_TIMEOUT {
            warn!(
                "Block(`{}`) proving has timed out, moving on.",
                self.current_block.as_ref().unwrap()
            );
            self.stage = Stage::Verify;
            self.current_block = None;
            self.begin_next_block();
        } else {
            match self.stage {
                Stage::Subblock => {
                    self.subblock_round.revoke_stale_assignments();
                },

                Stage::Agg => {
                    self.agg_round.revoke_stale_assignments();
                },

                _ => {
                    return
                }
            };
        }
    }

    pub fn add_subblock_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: PeerId
    ) {        
        self.subblock_round.add_proof(batch_id, hash, prover);
        let is_finished = self.subblock_round.is_finished();
        if is_finished {
            info!("Subblock round is finished. Agg round begins now.");
            self.stage = Stage::Agg;
        }             
    }

    pub fn add_agg_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: PeerId
    ) {        
        self.agg_round.add_proof(
            batch_id,
            hash,
            prover,
        );
        if self.agg_round.is_finished() {
            info!("The Agg round is finished, let's verify the proof.");
            self.stage = Stage::Verify;
        }
    }

    pub fn verify_agg_proof(
        &mut self,
        proof_blob: Vec<u8>,
        prover: PeerId
    ) {        
        if self.stage != Stage::Verify {
            warn!(
                "Stage must be `verify` to accept agg proofs from `{}`.",
                prover
            );
            return;
        }
        if !self.agg_round.is_assigned(&prover) {
            warn!(
                "Unsolicited agg proof from `{}`.",
                prover
            );
            return;
        }
        let expected_proof_hash = self.agg_round
            .proofs()
            .into_iter()
            .next()
            .unwrap()
            .hash;
        let received_hash = xxh3_128(&proof_blob);
        if expected_proof_hash != received_hash {
            warn!(
                "Invalid agg proof from `{}`: hash must be `{}` but is `{}`.",
                prover,
                expected_proof_hash,
                received_hash
            );
            return;
        }
        // perform verification
        match self.sp1_handle.verify_agg(
            &proof_blob,
            self.current_block.clone().unwrap()
        ) {
            Ok(_) => {
                info!(
                    "Nice! block(`{}`) is verified.",
                    self.current_block.as_ref().unwrap()
                );                        
                self.archive_block(proof_blob);
            },

            Err(e) => {
                warn!(
                    "Failed to verify block(`{}`)'s proof: {:?}",
                    self.current_block.as_ref().unwrap(),
                    e
                );
                //@ backtrack and detect offending prover(s)
            }
        }
        self.begin_next_block();
    }

    fn archive_block(
        &mut self,
        proof_blob: Vec<u8>
    ) {
        let duration = self.start_time.elapsed().as_secs();
        let rem = duration % 60;
        info!(
            "Total block proving time: `{} minutes{}`",
            duration / 60,
            if rem > 0 { format!(" and {} seconds", rem) } else { format!("") }
        );
        let block_number = self.current_block.as_ref().unwrap();
        // drop subblock and agg stdins of the block
        let blob_hashes = self.block_blob_hashes.remove(block_number).unwrap();
        for hash in blob_hashes.into_iter() {
            self.blob_store.remove(&hash);
        }        
        
        self.subblock_round.reset();
        self.agg_round.reset();

        //@ save the agg proof blob to the db
        let hash = xxh3_128(&proof_blob);
        self.blob_store.insert(hash, proof_blob);
        self.archived_blocks.insert(block_number.clone(), hash);
    }
}
