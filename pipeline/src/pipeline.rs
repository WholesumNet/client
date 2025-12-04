
use log::{
    info, warn
};
use uuid::Uuid;
use libp2p::PeerId;

use crate::round;
use round::{
    Round,
    Token
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
    // job id
    pub id: u128,
    pub block_number: u32,
    
    pub stage: Stage,
    subblock_round: Round,    
    agg_round: Round,    
    // used to verify proofs
    sp1_handle: SP1Handle,
}

impl Pipeline {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            id: 0u128,
            block_number: 0u32,
            stage: Stage::Subblock,
            subblock_round: Round::new(0usize, 1usize),
            agg_round: Round::new(127usize, 1usize),
            sp1_handle: SP1Handle::new()?,
        })        
    }


    pub fn begin_next_block(
        &mut self,
        block_number: u32,
        inputs: &[u128],
        owner: &PeerId,
    ) {        
        if self.stage != Stage::Verify {
            warn!(
                "Stage must be `Verify` to begin next block: {:?}",
                self.stage
            );
        }

        self.archive();
        
        info!("Started to prove block `{:?}`.", block_number);
        self.id = Uuid::new_v4().as_u128();
        self.block_number = block_number;
        self.stage = Stage::Subblock;
        self.subblock_round.reset();
        self.agg_round.reset();
        self.feed_stdins(inputs, owner);
    }

    fn feed_stdins(&mut self, inputs: &[u128], owner: &PeerId) {
        let mut tokens: Vec<_> = inputs.into_iter()
            .map(|h| Token {
                owner: owner.clone(),
                hash: *h
            })
            .collect();
        let agg_token = tokens.split_off(tokens.len() - 1);
        self.subblock_round.feed(&tokens);
        self.agg_round.feed(&agg_token);
        // println!("subblock: {:#?}", self.subblock_round);
        // println!("agg: {:#?}", self.agg_round);
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

    pub fn add_subblock_proof(
        &mut self,
        batch_id: u128,
        hash: u128,
        prover: PeerId
    ) {        
        let _is_valid = self.subblock_round.add_proof(batch_id, hash, prover);
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
        let _is_valid = self.agg_round.add_proof(
            batch_id,
            hash,
            prover,
        );
        if self.agg_round.is_finished() {
            info!("The Agg round is finished, let's verify it.");
            self.stage = Stage::Verify;
        }
    }

    pub fn verify_agg_proof(
        &self,
        proof_blob: &[u8]
    ) -> anyhow::Result<()> {
        self.sp1_handle.verify_agg(proof_blob)
    }

    pub fn archive(&self) {

    }
}
