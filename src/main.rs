#![doc = include_str!("../README.md")]

use futures::{
    // prelude::*,
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
    },
};

use async_std::stream;
use libp2p::{
    gossipsub, mdns, request_response,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};
use std::collections::{
    HashMap, HashSet
};
use std::fs;
use std::error::Error;
use std::time::Duration;
use rand::Rng;
use bincode;
use chrono::{Utc};

use toml;
use tracing_subscriber::EnvFilter;

use reqwest;

use bollard::Docker;
use jocker::exec::{
    // import_docker_image,
    run_docker_job,
};

use clap::Parser;
use comms::{
    p2p::{MyBehaviourEvent},
    notice,
    compute
};
use dstorage::lighthouse;
use benchmark;

mod job;
use job::Job;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Client CLI for Wholesum: p2p verifiable computing network.")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Wholesum is a P2P verifiable computing marketplace and \
                   this program is a CLI for client nodes.",
          long_about = None)
]
struct Cli {
    #[arg(short, long)]
    dstorage_key_file: Option<String>,

    #[arg(short, long)]
    job: Option<String>,

    #[arg(long, action)]
    dev: bool,

    #[arg(short, long)]
    key_file: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let cli = Cli::parse();
    println!("<-> Client agent for Wholesum network <->");
    println!("operating mode: `{}` network",
        if false == cli.dev {"global"} else {"local(development)"}
    );

    // dStorage keys
    let ds_key_file = cli.dstorage_key_file
        .ok_or_else(|| "dStorage key file is missing.")?;
    let ds_key = toml::from_str(&std::fs::read_to_string(ds_key_file)?)?;

    let ds_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60)) //@ how much timeout is enough?
        .build()?;

    println!("Connecting to docker daemon...");
    let docker_con = Docker::connect_with_socket_defaults()?;

    // jobs
    let mut jobs = HashMap::<String, Job>::new(); 
    // execution traces: (job_id, (receipt_cid, exec_trace))
    let mut jobs_execution_traces = HashMap::<String, HashMap::<String, job::ExecutionTrace>>::new();

    // future for 
    let mut offer_evaluation_futures = FuturesUnordered::new();

    if let Some(job_filename) = cli.job {
        let new_job = job::Job::new(None, 
            toml::from_str(&fs::read_to_string(job_filename)?)?
        );
        println!("A new job is here: {:#?}", new_job.schema);
        jobs.insert(new_job.id.clone(), new_job);
    }    
    
    // key 
    let local_key = {
        if let Some(key_file) = cli.key_file {
            let bytes = fs::read(key_file).unwrap();
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = fs::write("./key.secret", bytes);
            println!("No keys were supplied, so one has been generated for you and saved to `{}` file.", "./ket.secret");
            new_key
        }
    };    
    println!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));    

    // swarm 
    let mut swarm = comms::p2p::setup_swarm(&local_key).await?;
    let topic = gossipsub::IdentTopic::new("<-- p2p compute bazaar -->");
    let _ = 
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&topic); 

    // bootstrap 
    if false == cli.dev {
        // get to know bootnodes
        const BOOTNODES: [&str; 1] = [
            "12D3KooWLVDsEUT8YKMbZf3zTihL3iBGoSyZnewWgpdv9B7if7Sn",
        ];
        for peer in &BOOTNODES {
            swarm.behaviour_mut()
                .kademlia
                .add_address(&peer.parse()?, "/ip4/80.209.226.9/tcp/20201".parse()?);
        }
        // find myself
        if let Err(e) = 
            swarm
                .behaviour_mut()
                .kademlia
                .bootstrap() {
            eprintln!("bootstrap failed to initiate: `{:?}`", e);

        } else {
            println!("self-bootstrap is initiated.");
        }
    }

    // if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
    //     eprintln!("failed to initiate bootstrapping: {:#?}", e);
    // }
    
    // listen on all interfaces and whatever port the os assigns
    //@ should read from the config file
    swarm.listen_on("/ip4/0.0.0.0/udp/20201/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/udp/20201/quic-v1".parse()?)?;


    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    let mut timer_peer_discovery = stream::interval(Duration::from_secs(5 * 60)).fuse();

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

            // try to discover new peers
            () = timer_peer_discovery.select_next_some() => {
                if true == cli.dev {
                    continue;
                }
                let random_peer_id = PeerId::random();
                println!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            // post need `compute/verify/harvest`, and status updates
            () = timer_post_jobs.select_next_some() => { 
                // got any compute jobs?
                if let Some(job_id) = any_compute_jobs(&jobs, &jobs_execution_traces) {        
                    // need compute
                    let job = jobs.get(job_id).unwrap();
                    let need_compute = notice::Notice::Compute(compute::NeedCompute {
                        job_id: job_id.clone(),
                        criteria: compute::Criteria {
                            memory_capacity: job.schema.criteria.memory_capacity,
                            benchmark_expiry_secs: job.schema.criteria.benchmark_expiry_secs,
                            benchmark_duration_msecs: job.schema.criteria.benchmark_duration_msecs,
                        }
                    });
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    // println!("known peers: {:#?}", gossip.all_peers().collect::<Vec<_>>());
                    if let Err(e) = gossip
                        .publish(topic.clone(), bincode::serialize(&need_compute)?) {
                
                        eprintln!("(gossip) `need compute` publish error: {e:?}");
                    }
                }
                // need status updates?
                // let update_set = to_be_updated_jobs(&jobs_execution_traces);
                if true == any_pending_jobs(&jobs) {
                // for(server_id58, job_set) in update_set.iter() {                    
                //     // let job_status = notice::Request::JobStatus;
                //     // let gossip = &mut swarm.behaviour_mut().gossipsub;
                //     // if let Err(e) = gossip
                //     //     .publish(topic.clone(), bincode::serialize(&job_status)?) {
                
                //     //     eprintln!("`job status poll` publish error: {e:?}");
                //     // }
                //     let server_peer_id = {
                //         use std::str::FromStr;
                //         PeerId::from_str(server_id58.as_str())?
                //     };
                //     let job_id_list = job_set.iter().map(|s| String::from(*s)).collect();
                //     let _req_id = swarm.behaviour_mut()
                //     .req_resp
                //     .send_request(
                //         &server_peer_id,
                //         notice::Request::JobStatus(job_id_list),
                //     );
                    let nonce: u8 = rng.gen();
                    let status_update = notice::Notice::StatusUpdate(nonce);
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    let topic = gossip.topics().nth(0).unwrap(); 
                    if let Err(e) = gossip
                        .publish(
                            topic.clone(),
                            bincode::serialize(&status_update)?
                    ) {                
                        eprintln!("`status update` publish error: {e:?}");
                    }
                }
                // need verification
                // if true == _any_verification_jobs(&jobs, &jobs_execution_traces) {
                //     // let need_verify_msg = vec![notice::Notice::Verification.into()];
                //     let nonce: u8 = rng.gen();
                //     let need_verify = notice::Notice::Verification(nonce);
                //     let gossip = &mut swarm.behaviour_mut().gossipsub;
                //     let topic = gossip.topics().nth(0).unwrap(); 
                //     if let Err(e) = gossip
                //         .publish(
                //             topic.clone(),
                //             bincode::serialize(&need_verify)?
                //     ) {                
                //         eprintln!("`need verify` publish error: {e:?}");
                //     }
                // }

                // need to harvest
                if true == any_harvest_ready_jobs(&jobs, &jobs_execution_traces) {
                    // let need_harvest = vec![notice::Notice::Harvest.into()];
                    let nonce: u8 = rng.gen();
                    let need_harvest = notice::Notice::Harvest(nonce);
                    let gossip = &mut swarm.behaviour_mut().gossipsub;
                    if let Err(e) = gossip
                        .publish(topic.clone(), bincode::serialize(&need_harvest)?) {
                
                        eprintln!("`need harvest` publish error: {e:?}");
                    }
                }
            },

            // offer has been evaluated 
            eval_res = offer_evaluation_futures.select_next_some() => {
                let (job_id, server_peer_id): (String, PeerId) = match eval_res { 
                    Err(failed) => {
                        eprintln!("Evaluation error: {:#?}", failed);
                        // offer timeouts in server side and evaluation must be requested again
                        continue;                        
                    },

                    Ok(opt) => {
                        if let None = opt {
                            println!("Evaluation not passed.");
                            continue;
                        }
                        opt.unwrap()
                    },
                };  
                println!("we have a deal! server `{}` has successfully passed all evaluation checks.",
                    server_peer_id.to_string());
                let job = jobs.get(&job_id).unwrap();             
                let _req_id = swarm.behaviour_mut()
                    .req_resp
                    .send_request(
                        &server_peer_id,
                        notice::Request::ComputeJob(compute::ComputeDetails {
                            job_id: job_id,
                            docker_image: job.schema.compute.docker_image.clone(),
                            command: job.schema.compute.command.clone(),
                        })
                    );
                set_to_track_job_updates(
                    &job.id, 
                    server_peer_id,
                    &mut jobs_execution_traces
                );               
            },

            event = swarm.select_next_some() => match event {
                
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                // mdns events
                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Discovered(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer_id);
                    }                     
                },

                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Expired(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm
                            .behaviour_mut()
                            .gossipsub
                            .remove_explicit_peer(&peer_id);
                    }
                },

                // identify events
                SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received {
                    peer_id,
                    info,
                    ..
                })) => {
                    println!("Inbound identify event `{:#?}`", info);
                    if false == cli.dev {
                        for addr in info.listen_addrs {
                            // if false == addr.iter().any(|item| item == &"127.0.0.1" || item == &"::1"){
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr);
                            // }
                        }
                    }
                },

                // kademlia events
                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::GetClosestPeers(Ok(ok)),
                //             ..
                //         }
                //     )
                // ) => {
                //     // The example is considered failed as there
                //     // should always be at least 1 reachable peer.
                //     if ok.peers.is_empty() {
                //         eprintln!("Query finished with no closest peers.");
                //     }

                //     println!("Query finished with closest peers: {:#?}", ok.peers);
                // },

                // SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                //     result:
                //         kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                //             ..
                //         })),
                //     ..
                // })) => {
                //     eprintln!("Query for closest peers timed out");
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::Bootstrap(Ok(ok)),
                //             ..
                //         }
                //     )
                // ) => {                    
                //     println!("bootstrap inbound: {:#?}", ok);
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::OutboundQueryProgressed {
                //             result: kad::QueryResult::Bootstrap(Err(e)),
                //             ..
                //         }
                //     )
                // ) => {                    
                //     println!("bootstrap error: {:#?}", e);
                // },

                // SwarmEvent::Behaviour(
                //     MyBehaviourEvent::Kademlia(
                //         kad::Event::RoutingUpdated{
                //             peer,
                //             is_new_peer,
                //             addresses,
                //             ..
                //         }
                //     )
                // ) => {
                //     println!("Routing updated:\npeer: `{:?}`\nis new: `{:?}`\naddresses: `{:#?}`",
                //         peer, is_new_peer, addresses
                //     );
                // },

                // SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::UnroutablePeer{
                //     peer: peer_id
                // })) => {
                //     eprintln!("unroutable peer: {:?}", peer_id);
                // },


                // gossipsub events
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

                // requests
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
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
                            if false == jobs.contains_key(&compute_offer.job_id) {
                                println!("No such job, ignored offer.");
                                continue;
                            }
                            let job = jobs.get(&compute_offer.job_id).unwrap();        
                            offer_evaluation_futures.push(
                                evaluate_compute_offer(
                                    &docker_con,
                                    &ds_client,
                                    &job,
                                    compute_offer,
                                    server_peer_id
                                )
                            );                            
                        },      

                        notice::Request::VerificationOffer => {
                            println!("received `verify offer` from server: `{}`", 
                                server_peer_id);
                            //@ evaluate verify offer
                            if let Some(verification_details) = evaluate_verify_offer(
                                &jobs,
                                &jobs_execution_traces,
                                server_peer_id
                            ) {
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

                        notice::Request::UpdateForJobs(updates) => {
                            update_jobs(
                                &jobs,
                                &mut jobs_execution_traces,
                                server_peer_id,
                                &updates,
                            );                            
                        },

                        _ => {},
                    }
                },

                // responses
                // SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                //     peer: server_peer_id,
                //     message: request_response::Message::Response {
                //         response,
                //         ..
                //     }
                // })) => {                
                //     match response {
                //         // job status update 
                //         notice::Response::UpdateForJobs(updates) => {
                //             update_jobs(
                //                 &jobs,
                //                 &mut jobs_execution_traces,
                //                 server_peer_id,
                //                 &updates,
                //             );                            
                //         },

                //         _ => {}
                //     }
                // },

                _ => {
                    // println!("{:#?}", event)
                },

            },
        }
    }
}

// begin to track job updates
fn set_to_track_job_updates(
    job_id: &str,
    server_peer_id: PeerId,
    jobs_execution_traces: &mut HashMap<String, HashMap<String, job::ExecutionTrace>>
) {
    let new_unverified_exec_trace = |server_id: &str| -> job::ExecutionTrace {
        let mut new_deal_trace = job::ExecutionTrace::new(server_id.to_owned());
        new_deal_trace.status_update_history.push(
            job::StatusUpdate {
                status: compute::JobStatus::DealStruck,
                timestamp: Utc::now().timestamp()
            }
        );
        new_deal_trace
    };

    let server_id58 = server_peer_id.to_base58();
    jobs_execution_traces.entry(job_id.to_owned())
    .and_modify(|exec_trace| {
        exec_trace.entry(job::UNVERIFIED_PREFIX.to_owned())
        .and_modify(|_| {
            println!("[warning]: (unverified) execution trace is not empty.");
        })
        .or_insert_with(|| {
            new_unverified_exec_trace(&server_id58)         
        });
    })
    .or_insert_with(|| {        
        let mut exec_trace = HashMap::<String, job::ExecutionTrace>::new();
        exec_trace.insert(
            job::UNVERIFIED_PREFIX.to_owned(),
            new_unverified_exec_trace(&server_id58)
        );

        exec_trace
    });
}

// check if any compute jobs are there to gossip the need
fn any_compute_jobs<'a>(
    jobs: &'a HashMap::<String, Job>,
    jobs_execution_traces: &'a HashMap::<String, HashMap::<String, job::ExecutionTrace>>,

) -> Option<&'a String> {
    //@ beware starvation
    //@ the exact strategy TBD
    // jobs.values()
    // .find(|job| {
    //     // brand new (short circuit), or failed, or running?
    //     true == job.execution_trace.is_empty() ||
    //     // should not have any verified execution traces 
    //     false == job.has_verified_execution_traces()
    // })
    for (job_id, _) in jobs.iter() {
        // no execution traces at all
        if false == jobs_execution_traces.contains_key(job_id) {
            return Some(job_id);
        }
    }
    None
}

// check if any jobs need status updates
fn _to_be_updated_jobs<'a>(
    jobs_execution_traces: &'a HashMap::<String, HashMap<String, job::ExecutionTrace>>
) -> HashMap<String, HashSet<&'a str>> {
    // map: (server_id, job_id list)
    let mut to_be_updated = HashMap::<String, HashSet<&'a str>>::new();
    for (job_id, exec_trace) in jobs_execution_traces.iter() {
        // if false == jobs_execution_traces.contains_key(job_id) {
        //     continue;
        // }
        for (_, specific_exec_trace) in exec_trace.iter() {
            to_be_updated.entry(specific_exec_trace.server_id.clone())
                .or_insert_with(|| {
                    let mut update_set = HashSet::new();
                    update_set.insert(job_id.as_str());

                    update_set
                });
        }
    }
    to_be_updated
}

fn any_pending_jobs(
    jobs: &HashMap::<String, Job>
) -> bool {
    false == jobs.is_empty()
}

fn _any_verification_jobs(
    jobs: &HashMap::<String, Job>,
    jobs_execution_traces: &HashMap::<String, HashMap<String, job::ExecutionTrace>>
) -> bool {
    // jobs.values().find(|job| {
    //     let min_required_verifications = 
    //         job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
    //     // job requires to be verified
    //     true == job.schema.verification.min_required.is_some() &&
    //     // should have at least one execution succeeded
    //     false == job.execution_trace.is_empty() &&
    //     // should have at least one unverified execution trace
    //     true == job.execution_trace.values()
    //     .find(|exec_trace| 
    //         false == exec_trace.is_verified(min_required_verifications)
    //     ).is_some()
    // }).is_some()  

    for (_, job) in jobs.iter() {
        if false == jobs_execution_traces.contains_key(&job.id) {
            continue;
        }
        let min_required_verifications = 
            job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
        let exec_trace = jobs_execution_traces.get(&job.id).unwrap();
        if (true == job.schema.verification.min_required.is_some()) &&
           (false == exec_trace.is_empty()) &&
           (true == exec_trace.values()
                .find(|specific_exec_trace| 
                    false == specific_exec_trace.is_verified(min_required_verifications)
                ).is_some()
            ) {
            return true;
        }
    }
    false
}

fn any_harvest_ready_jobs(
    jobs: &HashMap::<String, Job>,
    jobs_execution_traces: &HashMap::<String, HashMap<String, job::ExecutionTrace>>
) -> bool {
    for (_, job) in jobs.iter() {
        if false == jobs_execution_traces.contains_key(&job.id) {
            continue;
        }
        // let min_required_verifications = 
        //         job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
        let exec_trace = jobs_execution_traces.get(&job.id).unwrap();
        if true == exec_trace.values().find(|specific_exec_trace|
            // true == specific_exec_trace.is_verified(min_required_verifications)
            true == specific_exec_trace.is_locally_verified
        ).is_some() {
            return true;
        }
    }
    false
}

// download benchmark from dstorage and put it into the docker volume
async fn download_benchmark(
    ds_client: &reqwest::Client,
    server_benchmark: &compute::ServerBenchmark,
) -> Result<String, Box<dyn Error>> {
    // save the benchmark to the docker volume
    let residue_path = format!(
        "{}/benchmark/{}/residue",
        job::get_residue_path()?,
        server_benchmark.cid,
    );
    fs::create_dir_all(residue_path.clone())?;
    lighthouse::download_file(
        ds_client,
        &server_benchmark.cid,
        format!("{residue_path}/benchmark")
    ).await?;
    Ok(residue_path)
}

// evaluate the offer:
//  - sybil, ... checks
//  - receipt verification
//  - timestamp expiry checks
//  - score/duration checks

async fn evaluate_compute_offer(
    docker_con: &Docker,
    ds_client: &reqwest::Client,
    job: &Job,
    compute_offer: compute::Offer,
    server_peer_id: PeerId,
) -> Result<Option<(String, PeerId)>, Box<dyn Error>> {
    //@ test for sybils, ... 
    println!("let's evaluate `{:?}`'s offer.", server_peer_id.to_string());
    // asking a verifier is slow, so verify locally
    // pull in the benchmark blob from dstorage
    //@ how about skipping disk save? we need to read it in memory shortly.
    let residue_path = download_benchmark(
        ds_client,
        &compute_offer.server_benchmark
    ).await?;
    // unpack it
    let benchmark_blob: Vec<u8> = fs::read(format!("{residue_path}/benchmark"))?;
    let benchmark_result: benchmark::Benchmark = 
        bincode::deserialize(benchmark_blob.as_slice())?;
    // 1- check timestamp for expiry
    //@ honesty checks with timestamps in benchmark execution
    //@ negativity in config file
    let expiry = Utc::now().timestamp() - benchmark_result.timestamp;
    if expiry > job.schema.criteria.benchmark_expiry_secs.unwrap_or_else(|| i64::MAX) {
        println!("Benchmark expiry check failed: good for `{:?}` secs, but `{:?}` secs detected.",
            job.schema.criteria.benchmark_expiry_secs, expiry);
        return Ok(None);
    } else {
        println!("Benchmark expiry check `passed`.");
    }
    // 2- check if duration is acceptable
    let max_acceptable_duration = 
        job.schema.criteria.benchmark_duration_msecs.unwrap_or_else(|| u128::MAX);
    if benchmark_result.duration_msecs > max_acceptable_duration {
        println!("Benchmark execution duration check `failed`.");
        return Ok(None);
    } else {
        println!("Benchmark execution duration `passed`.");        
    }
    // 3- verify the receipt
    let image_id_58 = {
        // [u32; 8] -> [u8; 32] -> base58 encode
        let mut image_id_bytes = [0u8; 32];
        //@ watch for panics here
        use byteorder::{ByteOrder, BigEndian};
        BigEndian::write_u32_into(
            &benchmark_result.r0_image_id,
            &mut image_id_bytes
        );
        bs58::encode(&image_id_bytes)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .with_check()
            .into_string()
    };
    // write benchmark's receipt into the vol used by warrant
    fs::write(
        format!("{residue_path}/receipt"),
        benchmark_result.r0_receipt_blob.as_slice()
    )?;
    println!("Benchmark's receipt is ready to be verified: `{}`", compute_offer.server_benchmark.cid);
    // run the job
    let verification_image = String::from("rezahsnz/warrant:0.21");
    let local_receipt_path = String::from("/home/prince/residue/receipt");
    let command = vec![
        String::from("/bin/sh"),
        String::from("-c"),
        format!(
            "/home/prince/warrant --image-id {} --receipt-file {}",
            image_id_58,
            local_receipt_path,
        )
    ];                          
    match run_docker_job(
        &docker_con,
        compute_offer.server_benchmark.cid,
        verification_image,
        command,
        residue_path.clone()
    ).await {
        Err(failed) => {
            eprintln!("Failed to verify the receipt: `{:#?}`", failed);       
            return Ok(None);
        },

        Ok(job_exec_res) => {
            if job_exec_res.exit_status_code != 0 { 
                println!("Verification failed with error: `{}`",
                    job_exec_res.error_message.unwrap_or_else(|| String::from("")),
                );
                return Ok(None);
            } else {
                println!("Verification suceeded.");
            }            
        }
    }
    Ok(
        Some(
        (
            job.id.clone(),
            server_peer_id
        ))
    )
}

// probe verify offer and respond needful
fn evaluate_verify_offer(
    jobs: &HashMap::<String, Job>,
    jobs_execution_traces: &HashMap<String, HashMap<String, job::ExecutionTrace>>,
    server_peer_id: PeerId
) -> Option<compute::VerificationDetails> {
    //@ fifo atm, should change once strategies go live
    let verifier_id58 = server_peer_id.to_base58();
    for job in jobs.values() {
        let exec_trace = jobs_execution_traces.get(&job.id).unwrap();
        // should have at least one execution succeeded or
        // if verification is needed at all
        if true == exec_trace.is_empty() ||
           false == job.schema.verification.min_required.is_some() {
            continue;
        }
        let min_required_verifications = job.schema.verification.min_required.unwrap();
        for (receipt_cid, specific_exec_trace) in exec_trace.iter() {
            // server != verifier,
            // the execution trace should be unverified, 
            // and receipt_cid must be present
            if specific_exec_trace.server_id == verifier_id58 ||
               specific_exec_trace.is_verified(min_required_verifications) ||
               true == receipt_cid.starts_with(job::UNVERIFIED_PREFIX) {
                continue;
            }

            return Some(compute::VerificationDetails {
                job_id: job.id.clone(),
                risc0_image_id: job.schema.verification.image_id.clone(),
                receipt_cid: receipt_cid.clone(),
                pod_name: format!("receipt_{}", job.id),
            })
        }
    }
    None    
}

// process bulk status updates
fn update_jobs(
    jobs: &HashMap::<String, Job>,
    jobs_execution_traces: &mut HashMap::<String, HashMap::<String, job::ExecutionTrace>>,
    server_peer_id: PeerId,
    updates: &Vec<compute::JobUpdate>,
) {
    // process updates for jobs
    for new_update in updates {
        // process a new update for job
        if false == jobs.contains_key(&new_update.id) {
            println!("Status update for an unknown job: `{}`",
                new_update.id);
            continue;
        }
        let job = jobs.get(&new_update.id).unwrap();

        let server_id = server_peer_id.to_string();
        println!("Status update for job `{}`, from `{}`: `{:#?}`",
            new_update.id, server_id, new_update);

        if false == jobs_execution_traces.contains_key(&new_update.id) {
            println!("Execution trace not found for status update: `{:#?}`",
                new_update);
            continue;
        }
        let exec_trace = jobs_execution_traces.get_mut(&job.id).unwrap();
        let unverified_prefix_key = job::UNVERIFIED_PREFIX.to_string();

        let timestamp = Utc::now().timestamp();
        match &new_update.status {  
            compute::JobStatus::DealStruck => {
                // TBD: post deal sync
            },

            compute::JobStatus::Running => {
                exec_trace.entry(unverified_prefix_key)                
                    .and_modify(|specific_exec_trace| {
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::Running,
                                timestamp: timestamp,
                            }
                        );
                    })
                    .or_insert_with(|| {
                        let mut specific_exec_trace = job::ExecutionTrace::new(server_id);
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::Running,
                                timestamp: timestamp,
                            }
                        );
                        specific_exec_trace
                    });
            },

            compute::JobStatus::ExecutionFailed(_fd12_cid) => { 
                exec_trace.entry(unverified_prefix_key)                
                    .and_modify(|specific_exec_trace| {
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::ExecutionFailed(_fd12_cid.clone()),
                                timestamp: timestamp,
                            }
                        );
                    })
                    .or_insert_with(|| {
                        let mut specific_exec_trace = job::ExecutionTrace::new(server_id);
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::ExecutionFailed(_fd12_cid.clone()),
                                timestamp: timestamp,
                            }
                        );
                        specific_exec_trace
                    });
            },

            compute::JobStatus::ExecutionSucceeded(receipt_cid) => {
                if true == receipt_cid.is_none() {
                    println!("[warning]: execution succeeded but `receipt_cid` is missing, so trace is unverified.");
                }
                //@ unverified-per-server
                let proper_receipt_cid = receipt_cid.clone().unwrap_or_else(|| unverified_prefix_key);
                exec_trace.entry(proper_receipt_cid.clone())
                    .and_modify(|specific_exec_trace| {
                        println!("Execution trace is already being tracked.");
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::ExecutionSucceeded(receipt_cid.clone()),
                                timestamp: timestamp,
                            }
                        );
                    })
                    .or_insert_with(|| {
                        let mut specific_exec_trace = job::ExecutionTrace::new(server_id);
                        specific_exec_trace.status_update_history.push(
                            job::StatusUpdate {
                                status: compute::JobStatus::ExecutionSucceeded(receipt_cid.clone()),
                                timestamp: timestamp,
                            }
                        );
                        specific_exec_trace.receipt_cid = Some(proper_receipt_cid);
                        // locally verify the receipt
                        // if false == receipt_cid.is_none() {                            
                        // }

                        specific_exec_trace
                    });
            },

            compute::JobStatus::VerificationSucceeded(receipt_cid) => {
                if false == job.schema.verification.min_required.is_some() {
                    println!("[info]: job is `not even required` to be verified.");
                }
                if false == exec_trace.contains_key(receipt_cid) {
                    //@wtd, its probably a successful execution that we have missed
                    println!("[warning]: no prior execution trace for this receipt `{}`", receipt_cid);
                    continue;
                }
                let specific_exec_trace = exec_trace.get_mut(receipt_cid).unwrap();
                specific_exec_trace.status_update_history.push(
                    job::StatusUpdate {
                        status: compute::JobStatus::VerificationSucceeded(receipt_cid.clone()),
                        timestamp: timestamp,
                    }
                );
                if let Some(old_decision) = specific_exec_trace.verifications.insert(server_id.clone(), true) {
                    if false == old_decision {
                        println!("[warning]: verification status flip, `failed` -> `succeeded`");
                    } else {
                        println!("[info]: verification status `succeeded` is already noted.");
                    }
                }                
                
                let num_approved = specific_exec_trace.num_verifications(true);
                let num_rejected = specific_exec_trace.num_verifications(false);
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
                    println!("[warning]: job is `not even required` to be verified.");
                }
                if false == exec_trace.contains_key(receipt_cid) {
                    //@wtd
                    println!("No prior execution trace for this receipt `{}`", receipt_cid);
                    continue;
                }
                let specific_exec_trace = exec_trace.get_mut(receipt_cid).unwrap();         
                specific_exec_trace.status_update_history.push(
                    job::StatusUpdate {
                        status: compute::JobStatus::VerificationFailed(receipt_cid.clone()),
                        timestamp: timestamp,
                    }
                );
                if let Some(old_decision) = specific_exec_trace.verifications.insert(server_id.clone(), false) {
                    if true == old_decision {
                        println!("[warning]: verification status flip, `succeeded` -> `failed`");
                    } else {
                        println!("[info]: verification status `failed` is already noted.");
                    }
                }
                let min_required_verifications = 
                    job.schema.verification.min_required.unwrap_or_else(|| u8::MAX);
                let num_approved = specific_exec_trace.num_verifications(true);
                let num_rejected = specific_exec_trace.num_verifications(false);
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
                    println!("[warning]: missing `fd12_cid` from harvest, ignored.");
                    continue;
                }

                let proper_receipt_cid = harvest_details.receipt_cid.clone().unwrap_or_else(|| unverified_prefix_key);                
                if false == exec_trace.contains_key(&proper_receipt_cid) {
                    println!("[warning]: no prior execution trace for this receipt.");
                    continue;
                }

                let specific_exec_trace = exec_trace.get_mut(&proper_receipt_cid).unwrap();
                specific_exec_trace.status_update_history.push(
                    job::StatusUpdate {
                        status: compute::JobStatus::Harvested(harvest_details.clone()),
                        timestamp: timestamp,
                    }
                );
                // ignore unverified harvests that need to be verified first, @ how about being opportunistic? 
                // let min_required_verifications = 
                //     job.schema.verification.min_required.unwrap_or_else(|| u8::MAX); 
                // if job.schema.verification.min_required.is_some() && 
                //    false == specific_exec_trace.is_verified(min_required_verifications) {
                //     println!("[warning]: execution trace must first be verified.");
                //     continue;
                // }
                let harvest = job::Harvest {
                    fd12_cid: harvest_details.fd12_cid.clone().unwrap(),
                };

                if false == specific_exec_trace.harvests.insert(harvest) {
                    println!("[info]: execution trace is already harvested.");
                }
                
                println!("Harvested the execution residues for `{proper_receipt_cid}`");
            },
        };
    }
}

async fn locally_verify_receipt(

) -> bool {
    true
}