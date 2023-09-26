use libp2p::PeerId;
use uuid::Uuid;
use std::collections::HashMap;

use comms::compute;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    JustCreated = 0,
    BeingNegotiated,         // collecting offers
    Running,
    ReadyForVerification,    // finished exectution, awaits verification 
    VerificationFailed,      // verification failed
    ReadyToHarvest,          // verification succeeded, this is where job's lifecyle comes to an end 
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
    pub details: compute::JobDetails,        // details of the job contract
    pub image_id: Vec<u8>,                   // image_id as in risc0, it's a hash digest
    pub overall_status: Status,
    pub updates: HashMap<PeerId, Update>,    // keep track of updates of all servers dealing with the job
}

impl Job {
    pub fn new (custom_id: Option<String>, job_details: compute::JobDetails) -> Job {
        Job {                  
            id: custom_id.unwrap_or_else(|| {
                //@ use safer id generation methods              
                Uuid::new_v4().to_string()
            }),
            details: job_details,
            image_id: Vec::<u8>::new(), //@ fill this
            overall_status: Status::JustCreated,
            updates: HashMap::<PeerId, Update>::new(),
        }
    }
}

// impl Hash for Job {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.id.hash(state);
//     }
// }

// impl PartialEq for Job {
//     fn eq(&self, other: &Self) -> bool {
//         self.id == other.id
//     }
// }

// impl Eq for Job {}