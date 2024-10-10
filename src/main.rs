#![doc = include_str!("../README.md")]

use futures::{
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
// use std::collections::{
//     HashMap, HashSet
// };
use std::{
    fs,
    time::Duration,
    path::PathBuf
};
use rand::Rng;
use bincode;
// use chrono::{Utc};

use toml;
use tracing_subscriber::EnvFilter;
use anyhow;
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

use uuid::Uuid;

use dstorage::lighthouse;
use benchmark;

mod job;
use job::Job;

mod recursion;
use recursion::{
    Recursion, Stage, SegmentStatus, Segment
};

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
async fn main() -> anyhow::Result<()> {
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
        .ok_or_else(|| anyhow::Error::msg("dStorage key file is missing."))?;
    let lighthouse_config: lighthouse::Config = 
        toml::from_str(&fs::read_to_string(ds_key_file)?)?;
    let ds_key = lighthouse_config.apiKey;

    let ds_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60)) //@ how much timeout is enough?
        .build()?;

    println!("Connecting to docker daemon...");
    let docker_con = Docker::connect_with_socket_defaults()?;

    // execution traces: (receipt_cid => exec_trace)
    // let mut execution_trace: HashMap<String, ExecutionTrace> = HashMap::new();

    // future for offer evaluation
    let mut offer_evaluation_futures = FuturesUnordered::new();

    // future for local verification of receipts
    // let mut local_verification_futures = FuturesUnordered::new();

    // futures for segments' uploads
    let mut segments_upload_futures = FuturesUnordered::new();   
    
    // local libp2p key 
    let local_key = {
        if let Some(key_file) = cli.key_file {
            let bytes = fs::read(key_file).unwrap();
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = fs::write("./key.secret", bytes);
            println!("No keys were supplied, so one has been generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    println!("my peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));  

    // job
    let job_schema: job::Schema = toml::from_str(&fs::read_to_string(cli.job.unwrap())?)?;
    let job_rec = {
        let mut segments = Vec::<Segment>::new();        
        // read segments off disk
        let mut segment_files: Vec<PathBuf> = vec![];
        for entry in fs::read_dir(&job_schema.compute.segments_path)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str().map(|s| s.to_ascii_lowercase())) == Some(String::from("seg")) {
                segment_files.push(path);
            }
        }
        segment_files.sort();
        println!("read {} segments in total.", segment_files.len());
        for seg_file in segment_files {
            let seg_name = seg_file.file_stem()
                    .ok_or_else(|| anyhow::Error::msg("<0>"))?
                    .to_str().ok_or_else(|| anyhow::Error::msg("<0>"))?;
            let seg_path = String::from(seg_file.to_string_lossy());
            segments_upload_futures.push(
                lighthouse::upload_file(
                    &ds_client,
                    &ds_key,
                    seg_path.clone(),
                    seg_name.to_string(),
                )
            );
            segments.push(
                Segment {
                    id: seg_name.to_string(),
                    num_prove_deals: 0,
                    status: SegmentStatus::Local(seg_path)
                }
            );
        }               
        Recursion::new(        
            segments
        )
    };

    let mut job = Job {
        id: Uuid::new_v4().simple().to_string()[..4].to_string(),
        schema: job_schema,
        recursion: job_rec,
    };  

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

            // post need `compute/harvest`, and status updates
            () = timer_post_jobs.select_next_some() => { 
                // got any compute jobs?
                let compute_type = match job.recursion.stage {
                    Stage::Prove => compute::ComputeType::ProveAndLift,
                    Stage::Join => compute::ComputeType::Join,
                    Stage::Snark => compute::ComputeType::Snark,
                    _ => continue,
                };
                let need_compute = compute::NeedCompute {
                    criteria: compute::Criteria {
                        compute_type: compute_type,
                        memory_capacity: job.schema.criteria.memory_capacity,
                        benchmark_expiry_secs: job.schema.criteria.benchmark_expiry_secs,
                        benchmark_duration_msecs: job.schema.criteria.benchmark_duration_msecs,
                    }
                };
                let gossip = &mut swarm.behaviour_mut().gossipsub;
                // println!("known peers: {:#?}", gossip.all_peers().collect::<Vec<_>>());
                if let Err(e) = gossip
                .publish(
                    topic.clone(),
                    bincode::serialize(&need_compute)?
                ) {            
                    eprintln!("(gossip) `need compute` publish error: {e:?}");
                }
                
                // need status updates?
                if job.recursion.stage != Stage::Upload {                
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
                // if true == any_harvest_ready_jobs(&jobs, &jobs_execution_traces) {
                //     // let need_harvest = vec![notice::Notice::Harvest.into()];
                //     let nonce: u8 = rng.gen();
                //     let need_harvest = notice::Notice::Harvest(nonce);
                //     let gossip = &mut swarm.behaviour_mut().gossipsub;
                //     if let Err(e) = gossip
                //         .publish(topic.clone(), bincode::serialize(&need_harvest)?) {
                
                //         eprintln!("`need harvest` publish error: {e:?}");
                //     }
                // }
            },

            // offer has been evaluated 
            eval_res = offer_evaluation_futures.select_next_some() => {
                let (server_peer_id, offer): (PeerId, compute::Offer) = match eval_res { 
                    Err(failed) => {
                        eprintln!("[warn] Evaluation error: {:#?}", failed);
                        // offer timeouts in server side and evaluation must be requested again
                        continue;
                    },

                    Ok(opt) => {
                        if let None = opt {
                            println!("[warn] Evaluation not passed.");
                            continue;
                        }
                        opt.unwrap()
                    },
                };  
                println!(
                    "[info] We have a deal! server `{}` has successfully passed all evaluation checks.",
                    server_peer_id.to_string()
                );
                match offer.compute_type {
                    compute::ComputeType::ProveAndLift => {
                        if job.recursion.stage != Stage::Prove {
                            continue;
                        }
                        // segment with the lowest prove deals has the highest priority
                        let chosen_segment = job.recursion.prove_and_lift
                        .segments.iter_mut()
                        .min_by(|x, y| x.num_prove_deals.cmp(&y.num_prove_deals))
                        .unwrap();                        
                        chosen_segment.num_prove_deals += 1;
                        let cmd = format!("/home/prince/reprove prove \
                                          -i /home/prince/residue/in.seg \
                                          -o /home/prince/residue/out.sr \
                                          1 > /home/prince/residue/stdout \
                                          2 > /home/prince/residue/stderr");                            
                        let cid = if let SegmentStatus::ProveReady(cid) = &chosen_segment.status { cid } else { continue };
                        let prove_job_id = format!("{}-{}", job.id, chosen_segment.id);
                        let _req_id = swarm.behaviour_mut()
                        .req_resp
                        .send_request(
                            &server_peer_id,
                            notice::Request::ComputeJob(compute::ComputeDetails {
                                job_id: prove_job_id,
                                //@ job details
                                docker_image: String::from("rezahsnz/r0-zoo:1.0.5"),
                                command: cmd,
                                compute_type: compute::ComputeType::ProveAndLift,
                                input: vec![cid.clone()],
                            })
                        );                     
                    },

                    compute::ComputeType::Join => {
                        if job.recursion.stage != Stage::Join {
                            continue;
                        }
                        let (left_cid, right_cid) = {
                            let join_pair = job.recursion.join.to_be_joined.first().unwrap();
                            (join_pair.0.clone(), join_pair.1.clone())
                        };                        
                        
                        let cmd = format!("/home/prince/reprove join \
                                          -l /home/prince/residue/left.sr \
                                          -r /home/prince/residue/right.sr \
                                          -o /home/prince/residue/out.sr \
                                          1 > /home/prince/residue/stdout \
                                          2 > /home/prince/residue/stderr");                            
                        //@ cid as job_id is tricky                        
                        let join_job_id = format!("{}-{}-{}", job.id, left_cid, right_cid);
                        let _req_id = swarm.behaviour_mut()
                        .req_resp
                        .send_request(
                            &server_peer_id,
                            notice::Request::ComputeJob(compute::ComputeDetails {
                                job_id: join_job_id,
                                //@ job details
                                docker_image: String::from("rezahsnz/r0-zoo:1.0.5"),
                                command: cmd,
                                compute_type: compute::ComputeType::Join,
                                input: vec![left_cid.to_string(), right_cid.to_string()],
                            })
                        );                                             
                    },

                    compute::ComputeType::Snark => {
                        if job.recursion.stage != Stage::Stark {
                            continue;
                        }
                        let cmd = format!("/home/prince/reprove snark \
                                          -i /home/prince/residue/in.sr \
                                          -o /home/prince/residue/out.snark \
                                          1 > /home/prince/residue/stdout \
                                          2 > /home/prince/residue/stderr");                            
                        let cid = job.recursion.join.joined.last().unwrap().first().unwrap();
                        let prove_job_id = format!("{}-stark", job.id);
                        let _req_id = swarm.behaviour_mut()
                        .req_resp
                        .send_request(
                            &server_peer_id,
                            notice::Request::ComputeJob(compute::ComputeDetails {
                                job_id: prove_job_id,
                                //@ job details
                                docker_image: String::from("rezahsnz/r0-zoo:1.0.5"),
                                command: cmd,
                                compute_type: compute::ComputeType::Snark,
                                input: vec![cid.clone()],
                            })
                        );                     

                    },
                }                             
            },

            // segments upload is finished
            ur = segments_upload_futures.select_next_some() => {
                let upload_res = ur?;
                let seg_id = upload_res.id;
                
                if job.recursion.stage != recursion::Stage::Upload {
                    continue;
                }
                let segment = job.recursion.prove_and_lift.segments.iter_mut()
                .find(|x| x.id == seg_id);
                if false == segment.is_some() {
                    eprintln!("[warn] Uploaded segment `{seg_id}`'s data is missing.");
                    continue;
                }
                segment.unwrap().status = SegmentStatus::ProveReady(upload_res.cid);
                println!("segment `{seg_id}`'s status changed to ProveReady");
                // check if recursion can go to prove&lift stage
                if false == job.recursion.prove_and_lift.segments.is_empty() &&
                   true == job.recursion.prove_and_lift.segments.iter().all(|s| 
                       if let SegmentStatus::ProveReady(_) = s.status { true } else { false }
                   ) {
                   // recursion is ready for prove and lift 
                   println!("[info] all segments of the job are uploaded to dstorage and it is ready for the `prove and lift` stage.");
                   job.recursion.stage = recursion::Stage::Prove;
                } 
            },

            // local verification has been finished
            // verification_res = local_verification_futures.select_next_some() => {
            //     let (exec_trace_id, receipt_cid, is_verified): (String, String, bool) = match verification_res {
            //         Err(failed) => {
            //             eprintln!("Verification error: {:#?}", failed);
            //             continue;                        
            //         },

            //         Ok(res) => res
            //     };
                
            //     if false == jobs_execution_traces.contains_key(&exec_trace_id) {
            //         println!("[warn] No such job `{exec_trace_id}`, ignored.");
            //         continue;
            //     }
            //     let exec_trace = jobs_execution_traces.get_mut(&exec_trace_id).unwrap();
            //     if false == exec_trace.contains_key(&receipt_cid) {
            //         eprintln!("[warn] No such execution trace `{receipt_cid}`, ignored.");
            //         continue;
            //     }
            //     let specific_exec_trace = exec_trace.get_mut(&receipt_cid).unwrap();
            //     let rec = recursions.get_mut(&exec_trace_id).unwrap();
            //     let segment_id = match specific_exec_trace.compute_type {
            //         compute::ComputeType::ProveAndLift |
            //         compute::ComputeType::Snark => {
            //             // job-id: "job_id-seg_id"
            //             let tokens: Vec<&str> = exec_trace_id.split("-").collect();
            //             if tokens.len() != 2 {
            //                 eprintln!("[warn] Execution trace id `{:?}` is invalid for the `prove and lift`/`to snark` compute type.",
            //                     exec_trace_id);
            //                 continue;
            //             }
            //             String::from(tokens[1])
            //         },
            //         compute::ComputeType::Join => {
            //             // job-id: "job_id-left_seg_id-right_seg_id"
            //             let tokens: Vec<&str> = exec_trace_id.split("-").collect();
            //             if tokens.len() != 3 {
            //                 eprintln!("[warn] Execution trace id `{:?}` is invalid for the `join` compute type.",
            //                     exec_trace_id);
            //                 continue;
            //             }
            //             format!("{}-{}", tokens[1], tokens[2])
            //         }, 
            //     };
            //     let segment = rec.prove_and_lift.segments.iter_mut().find(|x| x.id == segment_id);
            //     if false == segment.is_some() {
            //         eprintln!("[warn] Segment `{segment_id}` is missing.");
            //         continue;
            //     }
            //     // segment = segment.unwrap();
            //     println!("[info] Verification result for `{exec_trace_id}-{receipt_cid}`: `{}`",
            //         if true == is_verified {"verified!"} else {"unverified"});
            //     if false == is_verified {
            //         specific_exec_trace.local_verification = VerificationResult::Unverified;
            //         continue;
            //     }
            //     specific_exec_trace.local_verification = VerificationResult::Verified;
            //     segment.unwrap().status = SegmentStatus::ProvedAndLifted(receipt_cid.clone());
            //     // if all segments are verified, prepare for join
            //     if true == rec.prove_and_lift.segments.iter().all(|some_seg| {
            //         match some_seg.status {
            //             SegmentStatus::ProvedAndLifted(_) => true,
            //             _ => false,
            //         }                        
            //     }) {
            //         // begin join
            //         println!("[info] All segments are proved and lifted so the join stage begins.");
            //         rec.stage = Stage::Join;
            //         rec.prepare_next_join_round();                    
            //     }
            // },

            event = swarm.select_next_some() => match event {
                
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("[info] Local node is listening on {address}");
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
                        // channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        notice::Request::ComputeOffer(compute_offer) => {
                            println!("received `compute offer` from server: `{}`, offer: {:#?}", 
                                server_peer_id, compute_offer);       
                            offer_evaluation_futures.push(
                                evaluate_compute_offer(
                                    &docker_con,
                                    &ds_client,
                                    compute_offer,
                                    server_peer_id
                                )
                            );                            
                        },      

                        notice::Request::UpdateForJobs(updates) => {
                            // let mut to_be_locally_verified: Vec<(String, String, String)> = vec![];
                            update_jobs(                   
                                &mut job,
                                server_peer_id,
                                &updates,
                                // &mut to_be_locally_verified
                            );
                            // for (job_id, exec_trace_id, receipt_cid) in to_be_locally_verified {
                            //     local_verification_futures.push(
                            //         locally_verify_receipt(
                            //             &docker_con,
                            //             &ds_client,
                            //             exec_trace_id.clone(),
                            //             &jobs.get(&job_id).unwrap().schema.verification.image_id,
                            //             receipt_cid,
                            //         )
                            //     );                                   
                            // }
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


// download benchmark from dstorage and put it into the docker volume
async fn _download_benchmark(
    ds_client: &reqwest::Client,
    server_benchmark: &compute::ServerBenchmark,
) -> anyhow::Result<String> {
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
    _docker_con: &Docker,
    _ds_client: &reqwest::Client,
    compute_offer: compute::Offer,
    server_peer_id: PeerId,
) -> anyhow::Result<Option<(PeerId, compute::Offer)>> {
    //@ test for sybils, ... 
    println!("let's evaluate `{:?}`'s offer.", server_peer_id.to_string());
    Ok(Some((server_peer_id, compute_offer)))
    //@ vbs needs adjustments
    // asking a verifier is slow, so verify locally
    // pull in the benchmark blob from dstorage
    //@ how about skipping disk save? we need to read it in memory shortly.
    // let residue_path = download_benchmark(
    //     ds_client,
    //     &compute_offer.server_benchmark
    // ).await?;
    // // unpack it
    // let benchmark_blob: Vec<u8> = fs::read(format!("{residue_path}/benchmark"))?;
    // let benchmark_result: benchmark::Benchmark = 
    //     bincode::deserialize(benchmark_blob.as_slice())?;
    // // 1- check timestamp for expiry
    // //@ honesty checks with timestamps in benchmark execution
    // //@ negativity in config file
    // let expiry = Utc::now().timestamp() - benchmark_result.timestamp;
    // if expiry > job.schema.criteria.benchmark_expiry_secs.unwrap_or_else(|| i64::MAX) {
    //     println!("Benchmark expiry check failed: good for `{:?}` secs, but `{:?}` secs detected.",
    //         job.schema.criteria.benchmark_expiry_secs, expiry);
    //     return Ok(None);
    // } else {
    //     println!("Benchmark expiry check `passed`.");
    // }
    // // 2- check if duration is acceptable
    // let max_acceptable_duration = 
    //     job.schema.criteria.benchmark_duration_msecs.unwrap_or_else(|| u128::MAX);
    // if benchmark_result.duration_msecs > max_acceptable_duration {
    //     println!("Benchmark execution duration check `failed`.");
    //     return Ok(None);
    // } else {
    //     println!("Benchmark execution duration `passed`.");        
    // }
    // // 3- verify the receipt
    // let image_id_58 = {
    //     // [u32; 8] -> [u8; 32] -> base58 encode
    //     let mut image_id_bytes = [0u8; 32];
    //     //@ watch for panics here
    //     use byteorder::{ByteOrder, BigEndian};
    //     BigEndian::write_u32_into(
    //         &benchmark_result.r0_image_id,
    //         &mut image_id_bytes
    //     );
    //     bs58::encode(&image_id_bytes)
    //         .with_alphabet(bs58::Alphabet::BITCOIN)
    //         .with_check()
    //         .into_string()
    // };
    // // write benchmark's receipt into the vol used by warrant
    // fs::write(
    //     format!("{residue_path}/receipt"),
    //     benchmark_result.r0_receipt_blob.as_slice()
    // )?;
    // println!("Benchmark's receipt is ready to be verified: `{}`", compute_offer.server_benchmark.cid);
    // // run the job
    // let verification_image = String::from("rezahsnz/warrant:0.21");
    // let local_receipt_path = String::from("/home/prince/residue/receipt");
    // let command = vec![
    //     String::from("/bin/sh"),
    //     String::from("-c"),
    //     format!(
    //         "/home/prince/warrant --image-id {} --receipt-file {}",
    //         image_id_58,
    //         local_receipt_path,
    //     )
    // ];                          
    // match run_docker_job(
    //     &docker_con,
    //     compute_offer.server_benchmark.cid,
    //     verification_image,
    //     command,
    //     residue_path.clone()
    // ).await {
    //     Err(failed) => {
    //         eprintln!("Failed to verify the receipt: `{:#?}`", failed);       
    //         return Ok(None);
    //     },

    //     Ok(job_exec_res) => {
    //         if job_exec_res.exit_status_code != 0 { 
    //             println!("Verification failed with error: `{}`",
    //                 job_exec_res.error_message.unwrap_or_else(|| String::from("")),
    //             );
    //             return Ok(None);
    //         } else {
    //             println!("Verification suceeded.");
    //         }            
    //     }
    // }
    // Ok(
    //     Some(
    //     (
    //         job.id.clone(),
    //         server_peer_id
    //     ))
    // )
}

// process bulk status updates
fn update_jobs(
    job: &mut Job,
    server_peer_id: PeerId,
    updates: &Vec<compute::JobUpdate>,
) {
    // process updates for jobs
    for new_update in updates {
        let (job_id, seg_id) = match new_update.compute_type {
            compute::ComputeType::ProveAndLift |
            compute::ComputeType::Snark => {
                // job-id: "job_id-seg_id"
                let tokens: Vec<&str> = new_update.id.split("-").collect();
                if tokens.len() != 2 {
                    eprintln!("[warn] update_id `{:?}` is invalid for the `prove and lift`/`to snark` compute type.",
                        new_update.id
                    );
                    continue;
                }
                (String::from(tokens[0]), String::from(tokens[1]))
            },

            compute::ComputeType::Join => {
                // job-id: "job_id-left_seg_id-right_seg_id"
                let tokens: Vec<&str> = new_update.id.split("-").collect();
                if tokens.len() != 3 {
                    eprintln!("[warn] update_id `{:?}` is invalid for the `join` compute type.",
                        new_update.id
                    );
                    continue;
                }
                (String::from(tokens[0]), format!("{}-{}", tokens[1], tokens[2]))
            },            
        };    
        if job.id != job_id {
            println!("[warn] Status update for an unknown job `{job_id}`, ignored.");
            continue;
        }         

        let server_id = server_peer_id.to_string();
        println!("[info] Status update for job from `{}`: `{:#?}`",
            server_id, new_update
        );

        // let timestamp = Utc::now().timestamp();
        match &new_update.status {
            compute::JobStatus::Running => {
                println!("[info] Job `{}` is running.", new_update.id);
            },

            compute::JobStatus::ExecutionFailed(fd12_cid) => { 
                println!("[info] Execution failed for details: `{fd12_cid:?}`");
                // let's not keep track of failed jobs
            },

            compute::JobStatus::ExecutionSucceeded(receipt_cid) => {                
                println!("[info] Execution succeded, receipt_cid: `{receipt_cid}`");
                match new_update.compute_type {
                    compute::ComputeType::ProveAndLift => {
                        if job.recursion.stage != Stage::Prove { 
                            continue;
                        }
                        if let Some(proved_segment) = job.recursion.prove_and_lift.segments.iter_mut()
                        .find(|some_seg| 
                            some_seg.id == seg_id && 
                            if let SegmentStatus::ProveReady(_) = some_seg.status { true } else { false }
                        ) {                            
                            proved_segment.status = SegmentStatus::ProvedAndLifted(receipt_cid.to_string());
                        } else {
                            println!("[warn] Prove for an unknown segment `{seg_id}`");
                            continue;
                        }
                        if true == job.recursion.prove_and_lift.segments.iter()
                        .all(|some_seg|
                            if let SegmentStatus::ProvedAndLifted(_) = some_seg.status
                            { true } else { false }
                        ) {
                            job.recursion.stage = Stage::Join;
                            job.recursion.prepare_next_join_round();
                            //@ local verification is needed
                        }
                    },

                    compute::ComputeType::Join => {
                        if job.recursion.stage != Stage::Join { 
                            continue;
                        }
                        let (left_id, right_id) = {
                            let mut tokens = seg_id.split("-");
                            (tokens.nth(0).unwrap(), tokens.nth(1).unwrap())
                        };
                        let joined_pair_index = job.recursion.join
                        .to_be_joined.last().iter()
                        .position(|&p| p.0 == left_id && p.1 == right_id);
                        if true == joined_pair_index.is_none() {
                            continue;
                        }
                        let _removed_pair = job.recursion.join.to_be_joined.remove(joined_pair_index.unwrap());
                        job.recursion.join.joined.last_mut().unwrap()
                        .push(receipt_cid.to_string());
                        if true == job.recursion.join.to_be_joined.is_empty() {
                            job.recursion.prepare_next_join_round();
                        }
                    },

                    compute::ComputeType::Snark => {
                        if job.recursion.stage != Stage::Stark { 
                            continue;
                        }
                        job.recursion.snark = Some(receipt_cid.to_string());
                        job.recursion.stage = Stage::Done;
                        println!("[info] a SNARK proof has been extracted for the job `{job_id}`: {receipt_cid} ");
                    },


                }
            },

            compute::JobStatus::Harvested(harvest_details) => {
                // if false == harvest_details.fd12_cid.is_some() {
                //     println!("[warning]: missing `fd12_cid` from harvest, ignored.");
                //     continue;
                // }

                // let proper_receipt_cid = harvest_details.receipt_cid.clone().unwrap_or_else(|| unverified_prefix_key);                
                // if false == exec_trace.contains_key(&proper_receipt_cid) {
                //     println!("[warning]: no prior execution trace for this receipt.");
                //     continue;
                // }

                // let specific_exec_trace = exec_trace.get_mut(&proper_receipt_cid).unwrap();
                // specific_exec_trace.status_update_history.push(
                //     job::StatusUpdate {
                //         status: compute::JobStatus::Harvested(harvest_details.clone()),
                //         timestamp: timestamp,
                //     }
                // );
                // // ignore unverified harvests that need to be verified first, @ how about being opportunistic? 
                // // let min_required_verifications = 
                // //     job.schema.verification.min_required.unwrap_or_else(|| u8::MAX); 
                // // if job.schema.verification.min_required.is_some() && 
                // //    false == specific_exec_trace.is_verified(min_required_verifications) {
                // //     println!("[warning]: execution trace must first be verified.");
                // //     continue;
                // // }
                // let harvest = job::Harvest {
                //     fd12_cid: harvest_details.fd12_cid.clone().unwrap(),
                // };

                // if false == specific_exec_trace.harvests.insert(harvest) {
                //     println!("[info]: Execution trace is already harvested.");
                // }
                
                // println!("Harvested the execution residues for `{proper_receipt_cid}`");
            },
        };
    }
}

// download blob from dStorage
async fn download_receipt(
    ds_client: &reqwest::Client,
    cid: &str
) -> anyhow::Result<String> {
    // save the benchmark to the docker volume
    let residue_path = format!(
        "{}/verification/{}/residue",
        job::get_residue_path()?,
        cid,
    );
    fs::create_dir_all(residue_path.clone())?;
    lighthouse::download_file(
        ds_client,
        cid,
        format!("{residue_path}/receipt")
    ).await?;
    Ok(residue_path)
}

// verify any receipt locally
async fn locally_verify_receipt(
    docker_con: &Docker,
    ds_client: &reqwest::Client,
    exec_trace_id: String,
    image_id: &str,
    receipt_cid: String
) -> anyhow::Result<(String, String, bool)> {
    let residue_path = download_receipt(
        ds_client,
        &receipt_cid
    ).await?;
    // run the job
    let verification_image = String::from("rezahsnz/r0-zoo:1.0.5");
    let local_receipt_path = String::from("/home/prince/residue/receipt");
    let command = vec![
        String::from("/bin/sh"),
        String::from("-c"),
        format!(
            "/home/prince/veritas --image-id {} --receipt-file {}",
            image_id,
            local_receipt_path,
        )
    ];                          
    match run_docker_job(
        &docker_con,
        receipt_cid.clone(),
        verification_image,
        command,
        residue_path.clone()
    ).await {
        Err(failed) => {
            eprintln!("Failed to verify the receipt: `{:#?}`", failed);       
            return Ok(
                (
                    exec_trace_id,
                    receipt_cid,
                    false
                )
            );
        },

        Ok(job_exec_res) => {
            if job_exec_res.exit_status_code != 0 { 
                println!("Verification failed with error: `{}`",
                    job_exec_res.error_message.unwrap_or_else(|| String::from("")),
                );
                return Ok(
                    (
                        exec_trace_id,
                        receipt_cid,
                        false
                    )
                );
            } else {
                println!("Verification suceeded.");
            }            
        }
    }
    Ok(
        (
            exec_trace_id,
            receipt_cid,
            true
        )
    )
}