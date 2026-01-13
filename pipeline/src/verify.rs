use std::{
    fs,
    // time::Instant,
};
use log::info;
use anyhow::{
    Result,
    Context
};
// use bincode;
use sp1_sdk::{
    Prover,
    ProverClient, CpuProver,
    SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

pub struct SP1Handle {
    client: CpuProver,    
    agg_vk: SP1VerifyingKey,
}

impl SP1Handle {
    pub fn new() -> Result<Self> {
        info!("Initializing SP1.");
        let cpu_client = ProverClient::builder()
            .cpu()
            .build();        
        let agg_elf_path = "../elfs/agg_elf.bin";
        let agg_elf = fs::read(agg_elf_path)
            .with_context(||
                format!(
                    "The Agg ELF file was not found in `{}`.",
                    agg_elf_path
                )
            )?;
        let (_agg_pk, agg_vk) = cpu_client.setup(&agg_elf);
        Ok(Self {
            client: cpu_client,
            agg_vk: agg_vk
        })
    }

    pub fn verify_agg(
        &self,
        proof_blob: &[u8],
        block_number: u64
    ) -> Result<()> {
        //@ temporary
        let agg_proof_path = format!(
            "./agg-proof-{}.bin",
            block_number
        );
        fs::write(&agg_proof_path, proof_blob)
            .with_context(||
                format!(
                    "Failed to save the Agg proof to `{}`.",
                    agg_proof_path
                )
            )?;
        let proof = SP1ProofWithPublicValues::load(&agg_proof_path)?;
        self.client.verify(&proof, &self.agg_vk)?;
        Ok(())
    }
}
