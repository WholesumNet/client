#![doc = include_str!("../README.md")]

use futures::{
    prelude::*, select,
    // stream::{Stream},
};
use async_std::stream;
use libp2p::{
    gossipsub, mdns, request_response,
    swarm::{SwarmEvent},
    PeerId,
};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use std::fs;
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
#[command(name = "Client CLI for Wholesum: p2p verifiable computing marketplace.")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    job: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let cli = Cli::parse();
    println!("Started client agent for Wholesum.");
    
    // Libp2p swarm 
    let mut swarm = comms::p2p::setup_local_swarm();    
    
    // jobs that are already sent for computation and need lifecycle maintenance 
    let mut jobs = HashMap::<String, job::Job>::new();    

    if let Some(job_filename) = cli.job {
        println!("Loading job from: `{job_filename}`...");
        match fs::read_to_string(job_filename) {
            Ok(ss) => {
                match toml::from_str(ss.as_str()) {
                    Ok(j) => {
                        let new_job = job::Job::new(None, j);
                        jobs.insert(new_job.id.clone(), new_job);
                    },
                    Err(e) => println!("Job parse error: {:#?}", e),
                }                
            },
            Err(e) => println!("Job load error: {e:?}"),
        };
    }    

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut timer_post_jobs = stream::interval(Duration::from_secs(5)).fuse();
    // let mut idle_timer = stream::interval(Duration::from_secs(5 * 60)).fuse();
    let mut timer_job_status = stream::interval(Duration::from_secs(30)).fuse();
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

            // post "need compute/verify" if compute/verification pool is not empty
            () = timer_post_jobs.select_next_some() => {
                // need compute
                if false == any_compute_jobs(&jobs) {
                    println!("No compute jobs to post.");
                } else {
                    let need_compute_msg = vec![notice::Notice::Compute.into()];
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap();
                    if let Err(e) = gossip
                        .publish(topic.clone(), need_compute_msg) {
                
                        println!("need compute publish error: {e:?}");
                    }
                }
                // need verification
                if false == any_verification_jobs(&jobs) {
                    println!("No verification jobs to post.");
                } else {
                    let need_verify_msg = vec![notice::Notice::Verify.into()];
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap();
                    if let Err(e) = gossip
                        .publish(topic.clone(), need_verify_msg) {
                
                        println!("need verify publish error: {e:?}");
                    }
                }
            },

            // poll jobs' status 
            () = timer_job_status.select_next_some() => {
                if false == any_pending_jobs(&jobs) {
                    continue;
                }
                let job_status_msg = vec![notice::Notice::JobStatus.into()];
                let gossip = &mut swarm.behaviour_mut().gossipsub;
                let topic = gossip.topics().nth(0).unwrap();
                if let Err(e) = gossip
                    .publish(topic.clone(), job_status_msg) {
            
                    println!("job status publish error: {e:?}");
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
                            println!("received `compute offer` from server: `{}`, offer: {:?}", 
                                server_peer_id, compute_offer);
                            if let Some(compute_job) = examine_compute_offer(&mut jobs, compute_offer) {
                                let _ = swarm.behaviour_mut().req_resp
                                    .send_response(
                                        channel,
                                        notice::Response::ComputeJob(compute_job)
                                    );
                            } else {
                                println!("Got no compute jobs to respond to the offer.");
                            }
                        },      

                        notice::Request::VerificationOffer => {
                            println!("received `verify offer` from server: `{}`", 
                                server_peer_id);
                            // if false == any_verification_jobs(jobs) {
                            //     println!("No verification jobs to post.");
                            //     continue;
                            // }
                            //@ examine verify offer
                            if let Some(verification_details) = examine_verify_offer(&mut jobs, server_peer_id) {
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
                            update_job(
                                &mut jobs,
                                updates,
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

fn any_compute_jobs(jobs: &HashMap::<String, job::Job>) -> bool {
    jobs.values().find(|j| j.overall_status == job::Status::JustCreated).is_some()
}

fn any_verification_jobs(jobs: &HashMap::<String, job::Job>) -> bool {
    for (_, job) in jobs.iter() {
        if let Some(_) = job.updates.values()
            .find(|u| u.status == job::Status::ReadyForVerification) {
                return true;
            }
    }
    false
}

fn any_pending_jobs(jobs: &HashMap::<String, job::Job>) -> bool {
    jobs.values().find(|j| (j.overall_status <= job::Status::ReadyToHarvest))
        .is_some()
}

fn examine_compute_offer(
    jobs: &mut HashMap::<String, job::Job>,
    new_compute_offer: compute::Offer
) -> Option<compute::ComputeDetails> {
    // probe the offer throughly and respond needful
    //@ find the first and send it, needs to be changed when strategies are live
    if let Some(new_job) = jobs.values()
        .find(|j| j.overall_status == job::Status::JustCreated) {
        return Some(compute::ComputeDetails {
            job_id: new_job.id.clone(),
            docker_image: new_job.schema.docker_image.clone(),
            command: new_job.schema.command.clone(),
        });
    } 
    None
}

fn examine_verify_offer(
    jobs: & HashMap::<String, job::Job>,
    server_peer_id: PeerId
) -> Option<compute::VerificationDetails> {
    // probe verify offer and respond needful
    for (_, job) in jobs.iter() {
        for(prover_id, update) in job.updates.iter() {
            // ignore when prover === verifier  
            if server_peer_id == *prover_id {
                continue;
            }
            //@ find the first, needs to be changed when strategies go live
            if update.status == job::Status::ReadyForVerification {
                return Some(compute::VerificationDetails {
                    job_id: job.id.clone(),
                    image_id: job.image_id.clone(),
                    receipt_cid: update.residue.receipt_cid.clone().unwrap(),
                })
            }
        }
    }
    None
}

fn update_job(
    jobs: &mut HashMap::<String, job::Job>,
    updates: Vec<compute::JobUpdate>,
    server_peer_id: PeerId,
) {
    for new_update in updates {
        // process a new update for job
        if false == jobs.contains_key(&new_update.id) {
            println!("update for an unknown job: `{}`",
                new_update.id);
            continue;
        }
        println!("job update for `{}`, from `{}`",
            new_update.id, server_peer_id);
        let job = jobs.get_mut(&new_update.id).unwrap();
        if job.overall_status == job::Status::ReadyToHarvest {
            println!("job is ready to harvest, and needs no more updates.");
            continue;
        }
        // ideal development cycle of a job:
        // negotiated -> running -> ready for verification -> harvest ready
        match new_update.status {        
            compute::JobStatus::Running => {
                println!("Still running...");
                job.updates.entry(server_peer_id)
                    .and_modify(|e| {
                        if e.status < job::Status::Running {
                            println!("job is already running, ignored.");
                        } else {
                            e.status = job::Status::Running;
                        }
                    })
                    .or_insert(job::Update {
                        status: job::Status::Running,
                        residue: job::Residue {
                            stderr_cid: None,
                            stdout_cid: None,
                            receipt_cid: None,
                        },
                    });
                job.overall_status = job::Status::Running;
            },

            compute::JobStatus::ExecutionFailed(stderr_cid, stdout_cid) => {                                    
                println!("finished with error, stderr_cid: `{:?}`, stdout_cid: `{:?}`",
                    stderr_cid, stdout_cid);
                job.updates.entry(server_peer_id)
                    .and_modify(|e| {
                        if e.status < job::Status::ExecutionFailed {
                            println!("job is already failed, ignored.");
                        } else {
                            e.status = job::Status::ExecutionFailed;
                        }
                    })
                    .or_insert(job::Update {
                        status: job::Status::ExecutionFailed,
                        residue: job::Residue {
                            stderr_cid: stderr_cid,
                            stdout_cid: stdout_cid,
                            receipt_cid: None,
                        },
                    });
                //@ wtd with overall_status?
            },

            compute::JobStatus::ReadyForVerification(receipt_cid) => {
                println!("Ready to be verified!");
                job.updates.entry(server_peer_id)
                    .and_modify(|e| {
                        if e.status > job::Status::ReadyForVerification {
                            println!("job is already finished, ignored.");
                        } else {
                            e.status = job::Status::ReadyForVerification;
                        }
                    })
                    .or_insert(job::Update {
                        status: job::Status::ReadyForVerification,
                        residue: job::Residue {
                            stderr_cid: None,
                            stdout_cid: None,
                            receipt_cid: receipt_cid,
                        },
                    });            
            },

            compute::JobStatus::VerificationSucceeded => {                                    
                println!("verification succeeded");
                job.updates.entry(server_peer_id)
                    .and_modify(|e| {
                        if e.status > job::Status::VerificationSucceeded {
                            println!("job is already succeeded in verification, ignored.");
                        } else {
                            e.status = job::Status::VerificationSucceeded;
                        }
                    })
                    .or_insert(job::Update {
                        status: job::Status::VerificationSucceeded,
                        residue: job::Residue {
                            stderr_cid: None,
                            stdout_cid: None,
                            receipt_cid: None,
                        },
                    });
                //@ wtd with overall_status?
            },

            compute::JobStatus::VerificationFailed(e) => {
                println!("Verification failed, error: `{e:?}`");
                job.updates.entry(server_peer_id)
                    .and_modify(|e| {
                        if e.status > job::Status::VerificationFailed {
                            println!("job is already finished, ignored.");
                        } else {
                            e.status = job::Status::VerificationFailed;
                        }
                    })
                    .or_insert(job::Update {
                        status: job::Status::VerificationFailed,
                        residue: job::Residue {
                            stderr_cid: None,
                            stdout_cid: None,
                            receipt_cid: None,
                        },
                    });
            },
            _ => (),
        };
    }
}