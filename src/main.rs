#![doc = include_str!("../README.md")]

use futures::{
    prelude::*, select,
};
use async_std::stream;
use libp2p::{
    gossipsub, mdns, request_response,
    swarm::{SwarmEvent},
    PeerId,
};
use std::collections::{
    HashMap,
};
use std::error::Error;
use std::time::Duration;
use rand::Rng;

use toml;
use clap::Parser;
use comms::{
    p2p::{MyBehaviourEvent},
    notice,
    compute
};

mod job;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Client CLI for Wholesum: p2p verifiable computing network.")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Yet another p2p verifiable computing network.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    job: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let cli = Cli::parse();
    println!("<-> client agent for Wholesum network <->");
    
    // Libp2p swarm 
    let mut swarm = comms::p2p::setup_local_swarm();

    // active jobs
    let mut jobs = HashMap::<String, job::Job>::new();  

    if let Some(job_filename) = cli.job {
        let new_job = job::Job::new(None, 
            toml::from_str(&std::fs::read_to_string(job_filename)?)?
        );
        println!("A new job is here: {:#?}", new_job.schema);
        jobs.insert(new_job.id.clone(), new_job);
    }    

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut timer_post_jobs = stream::interval(Duration::from_secs(10)).fuse();
    // let mut idle_timer = stream::interval(Duration::from_secs(5 * 60)).fuse();
    // let mut timer_job_status = stream::interval(Duration::from_secs(30)).fuse();
    // gossip messages are content-addressed, so we add a random nounce to each message
    let mut rng = rand::thread_rng();

    // kick it off
    loop {
        select! {
            // line = input.select_next_some() => {
            //   if let Err(e) = swarm
            //     .behaviour_mut().gossipsub
            //     .publish(topic.clone(), line.expect("Stdin not to close").as_bytes()) {
            //       println!("Publish error: {e:?}")
            //     }
            // },

            // suggest posting jobs using stdin when idle
            // () = idle_timer.select_next_some() => {
            //     if new_jobs.len() == 0 {
            //         println!(
            //             "Since you got no more jobs, how about composing one right here?"
            //         );
            //     }
            // },

            // post need `compute/verify/harvest`, and status updates
            () = timer_post_jobs.select_next_some() => { 
                // got any compute jobs?
                if true == any_compute_jobs(&jobs) {        
                    // need compute                    
                    let need_compute_msg = vec![notice::Notice::Compute.into()];
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap(); 
                    if let Err(e) = gossip
                        .publish(topic.clone(), need_compute_msg) {
                
                        println!("need compute publish error: {e:?}");
                    }
                }
                // need status updates?
                if true == any_pending_jobs(&jobs) {        
                    // status poll
                    let nounce: u8 = rng.gen();
                    let job_status_msg = vec![notice::Notice::JobStatus.into(), nounce];

                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap(); 
                    if let Err(e) = gossip
                        .publish(topic.clone(), job_status_msg) {
                
                        println!("`job status poll` publish error: {e:?}");
                    }
                }
                // need verification
                if true == any_verification_jobs(&jobs) {
                    let need_verify_msg = vec![notice::Notice::Verification.into()];
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap(); 
                    if let Err(e) = gossip
                        .publish(topic.clone(), need_verify_msg) {
                
                        println!("`need verify` publish error: {e:?}");
                    }
                }

                // need to harvest
                if true == any_harvest_ready_jobs(&jobs) {
                    let need_harvest = vec![notice::Notice::Harvest.into()];
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap(); 
                    if let Err(e) = gossip
                        .publish(topic.clone(), need_harvest) {
                
                        println!("`need harvest` publish error: {e:?}");
                    }
                }
            },

            event = swarm.select_next_some() => match event {
                
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }                     
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    // propagation_source: peer_id,
                    // message_id,
                    message,
                    ..
                })) => {
                    // let msg_str = String::from_utf8_lossy(&message.data);
                    // println!("Got message: '{}' with id: {id} from peer: {peer_id}",
                    //          msg_str);
                    println!("received gossip message: {:#?}", message);                    
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: server_peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        ..//request_id,
                    }
                })) => {                
                    match request {
                        notice::Request::ComputeOffer(compute_offer) => {
                            println!("received `compute offer` from server: `{}`, offer: {:#?}", 
                                server_peer_id, compute_offer);
                            if let Some(compute_job) = evaluate_compute_offer(&mut jobs, compute_offer, server_peer_id) {
                                let _ = swarm.behaviour_mut().req_resp
                                    .send_response(
                                        channel,
                                        notice::Response::ComputeJob(compute_job)
                                    );
                                println!("Compute job was sent to the server.");
                            } else {
                                println!("Ignored the offer.");
                            }
                        },      

                        notice::Request::VerificationOffer => {
                            println!("received `verify offer` from server: `{}`", 
                                server_peer_id);
                            //@ evaluate verify offer
                            if let Some(verification_details) = evaluate_verify_offer(&mut jobs, server_peer_id) {
                                let _ = swarm
                                    .behaviour_mut().req_resp
                                    .send_response(
                                        channel,
                                        notice::Response::VerificationJob(verification_details.clone()),
                                    );
                                println!("Verification job has been sent to the verifier!");
                            } else {
                                println!("Got no verification jobs to respond to the offer.");
                            }
                        },

                        // job status update 
                        notice::Request::UpdateForJobs(updates) => {
                            update_jobs(
                                &mut jobs,
                                &updates,
                                server_peer_id,
                            );
                            
                        },
                    }
                },

                _ => {}

            },
        }
    }
}

// check if any compute jobs are there to gossip about
fn any_compute_jobs(
    jobs: &HashMap::<String, job::Job>
) -> bool {
    jobs.values()
    .find(|job| {
        // brand new (short circuit), or failed, or running?
        true == job.execution_trace.is_empty() ||
        // should not have any verified execution traces 
        false == job.has_verified_execution_traces()
    }).is_some()
}

// check if any jobs need status updates
fn any_pending_jobs(
    jobs: &HashMap::<String, job::Job>
) -> bool {
    false == jobs.is_empty()
}

fn any_verification_jobs(
    jobs: &HashMap::<String, job::Job>
) -> bool {
    jobs.values().find(|job| {
        let min_required_verifications = 
            job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
        // job requires to be verified
        true == job.schema.verification.min_required.is_some() &&
        // should have at least one execution succeeded
        false == job.execution_trace.is_empty() &&
        // should have at least one unverified execution trace
        true == job.execution_trace.values()
        .find(|exec_trace| 
            false == exec_trace.is_verified(min_required_verifications)
        ).is_some()
    }).is_some()    
}

fn any_harvest_ready_jobs(
    jobs: &HashMap::<String, job::Job>
) -> bool {
    jobs.values().find(|job|
        true == job.has_harvest_ready_execution_traces()
    ).is_some()
}

// probe compute offer throughly and select the best job
fn evaluate_compute_offer(
    jobs: &mut HashMap::<String, job::Job>,
    new_compute_offer: compute::Offer,
    server_peer_id: PeerId,
) -> Option<compute::ComputeDetails> {
    let server_id58 = server_peer_id.to_base58();
    // basic filtering
    let filtered_jobs = jobs.values()
        .filter(|job| {
            let min_required_memory_capacity = job.schema.compute.min_memory_capacity
                .unwrap_or_else(|| 0u32);

            // should not have an execution trace from this server
            (false == job.execution_trace.values()
            .find(|exec_trace| exec_trace.server == server_id58).is_some()) &&
            // ensure memory requirement is met
            (new_compute_offer.hw_specs.memory_capacity >= min_required_memory_capacity)
        });
    // priority 1: choose from jobs with empty execution trace
    let virgin_job_ids: Vec<String> = filtered_jobs.clone()
        .filter(|job| true == job.execution_trace.is_empty())
        .map(|job| job.id.clone())
        .collect();
    if false == virgin_job_ids.is_empty() {        
        let index = rand::thread_rng()
            .gen_range(0..virgin_job_ids.len());
        let selected_job = jobs.get(&virgin_job_ids[index]).unwrap();
        return Some(compute::ComputeDetails {
            job_id: selected_job.id.clone(),
            docker_image: selected_job.schema.compute.docker_image.clone(),
            command: selected_job.schema.compute.command.clone(),
        })
    }
    // priority 2: choose from jobs with 0 verified execution traces
    let unverified_job_ids: Vec<String> = filtered_jobs
        .filter(|job| {
            let min_required_verifications = job.schema.verification.min_required
                .unwrap_or_else(|| u8::MAX);
            // unverified is ok
            true == job.schema.verification.min_required.is_none() ||
            true == job.execution_trace.values()
                .all(|exec_trace| 
                    false == exec_trace.is_verified(min_required_verifications)
                )
        }).map(|job| job.id.clone())
        .collect();
    if false == unverified_job_ids.is_empty() {
        let index = rand::thread_rng()
            .gen_range(0..unverified_job_ids.len());
        let selected_job = jobs.get(&unverified_job_ids[index]).unwrap();
        return Some(compute::ComputeDetails {
            job_id: selected_job.id.clone(),
            docker_image: selected_job.schema.compute.docker_image.clone(),
            command: selected_job.schema.compute.command.clone(),
        })
    }

    // priority 3: choose from unharvested jobs?

    // if let Some(new_job) = jobs.values().find(|job| {
    //     let min_required_memory_capacity = job.schema.compute.min_memory_capacity
    //         .unwrap_or_else(|| 0u32);

    //     // be brand new(short circuit)
    //     true == job.execution_trace.is_empty() ||
    //     // should not have already been computed by this server
    //     true == job.execution_trace.values()
    //         .find(|exec_trace| exec_trace.server == server_id58).is_none() &&
    //     // has enough memory?
    //     (new_compute_offer.hw_specs.memory_capacity >= min_required_memory_capacity)
    //     //@ harvests?
    // }) {
    //     return Some(compute::ComputeDetails {
    //         job_id: new_job.id.clone(),
    //         docker_image: new_job.schema.compute.docker_image.clone(),
    //         command: new_job.schema.compute.command.clone(),
    //     })       
    // }
    None
}

// probe verify offer and respond needful
fn evaluate_verify_offer(
    jobs: &HashMap::<String, job::Job>,
    server_peer_id: PeerId
) -> Option<compute::VerificationDetails> {
    //@ fifo atm, should change once strategies go live
    let verifier_id58 = server_peer_id.to_base58();
    for job in jobs.values() {
        // should have at least one execution succeeded or
        // if verification is needed at all
        if true == job.execution_trace.is_empty() ||
           false == job.schema.verification.min_required.is_some() {
            continue;
        }
        let min_required_verifications = 
            job.schema.verification.min_required.unwrap();
        for (receipt_cid, exec_trace) in job.execution_trace.iter() {
            // server != verifier and the execution trace should be unverified
            if exec_trace.server == verifier_id58 ||
               exec_trace.is_verified(min_required_verifications) ||
               receipt_cid == "<unverified>" {
                continue;
            }

            return Some(compute::VerificationDetails {
                job_id: job.id.clone(),
                image_id: job.schema.verification.image_id.clone(),
                receipt_cid: receipt_cid.clone(),
                pod_name: format!("receipt_{}", job.id),
            })
        }
    }
    None    
}

fn update_jobs(
    jobs: &mut HashMap::<String, job::Job>,
    updates: &Vec<compute::JobUpdate>,
    server_peer_id: PeerId,
) {
    // process updates for jobs
    for new_update in updates {
        // process a new update for job
        if false == jobs.contains_key(&new_update.id) {
            println!("Status update for an unknown job: `{}`",
                new_update.id);
            continue;
        }

        let server_id58 = server_peer_id.to_base58();
        println!("Status update for job `{}`, from `{}`: `{:#?}`",
            new_update.id, server_id58, new_update);
        let job = jobs.get_mut(&new_update.id).unwrap();
        // ensure job has bare minimum update history
        job.status_history.entry(server_id58.clone())
        .and_modify(|h| h.push(new_update.status.clone()))
        .or_insert(vec![new_update.status.clone()]);

        match &new_update.status {                
            compute::JobStatus::Running => {
                // TBD
            },

            compute::JobStatus::ExecutionFailed(_fd12_cid) => {
                // TBD
            },

            compute::JobStatus::ExecutionSucceeded(receipt_cid) => {
                if true == receipt_cid.is_none() {
                    println!("Warning: execution succeeded but `receipt_cid` is missing.");
                }

                //@ unverified-per-server
                let proper_receipt_cid = receipt_cid.clone()
                    .unwrap_or_else(|| String::from("<unverified>"));
                job.execution_trace.entry(proper_receipt_cid.clone())
                .and_modify(|_v| {
                    println!("Execution trace `{}` is already being tracked.", proper_receipt_cid);
                })
                .or_insert_with(|| job::ExecutionTrace::new(server_id58.clone()));
            },

            compute::JobStatus::VerificationSucceeded(receipt_cid) => {
                if false == job.schema.verification.min_required.is_some() {
                    println!("Warning: job is not required to be verified.");
                }
                if false == job.execution_trace.contains_key(receipt_cid) {
                    //@wtd, its probably a successful execution that we have missed
                    println!("No prior execution trace for this receipt `{}`", receipt_cid);
                    continue;
                }
                let exec_trace = job.execution_trace.get_mut(receipt_cid).unwrap();
                if let Some(old_decision) = exec_trace.verifications.insert(receipt_cid.clone(), true) {
                    if false == old_decision {
                        println!("Warning: verification status flip, `failed` -> `succeeded`");
                    } else {
                        println!("Verification status `succeeded` is already noted.");
                    }
                }                
                let num_approved = exec_trace.num_verifications(true);
                let num_rejected = exec_trace.num_verifications(false);
                let min_required_verifications = 
                    job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
                println!(
                    "Verifications so far, succeded: `{}`, failed: `{}`, required: `{}`",
                    num_approved, num_rejected, min_required_verifications
                );
                if num_approved >= min_required_verifications.into() &&
                   num_approved >= 2 * num_rejected {
                    println!("Congrats!, the job has received the minimum number of required \
                        verifications and is now considered to be verified.");
                } else {
                    println!("Still unverified.");
                }               
            },

            compute::JobStatus::VerificationFailed(receipt_cid) => {                
                if false == job.schema.verification.min_required.is_some() {
                    println!("Warning: job is not required to be verified.");
                }
                if false == job.execution_trace.contains_key(receipt_cid) {
                    //@wtd
                    println!("No prior execution trace for this receipt `{}`", receipt_cid);
                    continue;
                }
                let exec_trace = job.execution_trace.get_mut(receipt_cid).unwrap();
                if let Some(old_decision) = exec_trace.verifications.insert(receipt_cid.clone(), false) {
                    if true == old_decision {
                        println!("Warning: verification status flip, `succeeded` -> `failed`");
                    } else {
                        println!("Verification status `failed` is already noted.");
                    }
                }
                let min_required_verifications = 
                    job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
                let num_approved = exec_trace.num_verifications(true);
                let num_rejected = exec_trace.num_verifications(false);
                println!(
                    "Verifications so far, succeded: `{}`, failed: `{}`, required: `{}`",
                    num_approved, num_rejected, min_required_verifications
                );
                if num_approved >= min_required_verifications.into() &&
                   num_approved >= 2 * num_rejected {
                    println!("Congrats!, the job has received the minimum number of required /
                        verifications and is now considered to be `verified`.");
                } else {
                    println!("Still unverified.");
                }    
            },

            compute::JobStatus::Harvested(harvest_details) => {
                if false == harvest_details.fd12_cid.is_some() {
                    println!("Missing fd12_cid from harvest, ignored.");
                    continue;
                }

                let proper_receipt_cid = harvest_details.receipt_cid.clone()
                    .unwrap_or_else(|| String::from("<unverified>"));
                if false == job.execution_trace.contains_key(&proper_receipt_cid) {
                    println!("No prior execution trace for this receipt `{}`", proper_receipt_cid);
                    continue;
                }

                let exec_trace = job.execution_trace.get_mut(&proper_receipt_cid).unwrap();
                let harvest = job::Harvest {
                    fd12_cid: harvest_details.fd12_cid.clone().unwrap(),
                };

                if false == exec_trace.harvests.insert(harvest) {
                    println!("Execution trace is already harvested.");
                }
                
                println!("Harvested the execution residues left by `{proper_receipt_cid}`");
            },
        };
    }
}
