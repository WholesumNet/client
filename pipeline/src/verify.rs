use std::{
    fs,
    // time::Instant,
};
use anyhow;
use bincode;
use sp1_sdk::{
    Prover,
    ProverClient, CpuProver,
    SP1VerifyingKey, 
    SP1ProofWithPublicValues,
};

pub struct SP1Handle {
    client: CpuProver,    
    subblock_vk: SP1VerifyingKey,    
    agg_vk: SP1VerifyingKey,
}

impl SP1Handle {
    pub fn new() -> anyhow::Result<Self> {
        let cpu_client = ProverClient::builder().cpu().build();
        // subblock
        let subblock_elf = fs::read("../elfs/subblock_elf.bin")?;
        let (_subblock_pk, subblock_vk) = cpu_client.setup(&subblock_elf);
        // agg
        let agg_elf = fs::read("../elfs/agg_elf.bin")?;
        let (_agg_pk, agg_vk) = cpu_client.setup(&agg_elf);

        Ok(Self {
            client: cpu_client,
            subblock_vk: subblock_vk,
            agg_vk: agg_vk
        })
    }

    pub fn verify_agg(&self, proof_blob: &[u8]) -> anyhow::Result<()> {
        let proof: SP1ProofWithPublicValues = bincode::deserialize(&proof_blob)?;
        self.client.verify(&proof, &self.agg_vk)?;
        Ok(())
    }

    pub fn verify_subblock(&self, proof_blob: &[u8]) -> anyhow::Result<()> {
        let proof: SP1ProofWithPublicValues = bincode::deserialize(&proof_blob)?;
        self.client.verify(&proof, &self.subblock_vk)?;
        Ok(())
    }
}
