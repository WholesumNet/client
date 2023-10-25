use libp2p::PeerId;
use uuid::Uuid;
use std::collections::HashMap;
use serde::Deserialize;
use comms::compute;

// job template as read in(e.g. from disk)
#[derive(Debug, PartialEq, Eq, Deserialize)]
pub struct Schema {
    pub docker_image: String,
    pub command: String,
    pub image_id: String,        // image_id as in risc0, it's a hash digest
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    JustCreated = 0,
    Negotiating,             // collecting offers
    Running,
    ReadyForVerification,    // finished exectution, awaits verification 
    VerificationSucceeded,
    ReadyToHarvest,          //@ payment has to be made
    VerificationFailed,      // verification failed
    ExecutionFailed,         // job exec failed during proving
}

#[derive(Debug)]
pub struct Residue {
    pub stdout_cid: Option<String>,
    pub stderr_cid: Option<String>,
    pub receipt_cid: Option<String>,
}

#[derive(Debug)]
pub struct Update {
    pub status: Status,
    pub residue: Residue,    // cids for stderr, output, receipt, ...
}

// maintains lifecycle for a job
#[derive(Debug)]
pub struct Job {
    pub id: String,
    pub schema: Schema,
    pub overall_status: Status,
    pub updates: HashMap<PeerId, Update>,    // keep track of updates of all servers working on the job
}

impl Job {
    pub fn new (custom_id: Option<String>, schema: Schema) -> Job {
        Job {                  
            id: custom_id.unwrap_or_else(|| {
                //@ use safer id generation methods              
                Uuid::new_v4().simple().to_string()[..4].to_string()
            }),
            schema: schema,
            overall_status: Status::JustCreated,
            updates: HashMap::<PeerId, Update>::new(),
        }
    }
}
