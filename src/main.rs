#![doc = include_str!("../README.md")]

use futures::{
    select,
    stream::{
        FuturesUnordered,
        StreamExt,
    },
    TryStreamExt,
};

use async_std::stream;
use libp2p::{
    gossipsub, mdns, request_response,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};
use std::collections::{
    HashMap,
    BTreeMap,
};
use std::{
    fs,
    time::Duration,
    path::PathBuf,
    future::IntoFuture,
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

use clap::{
    Parser, Subcommand
};

use comms::{
    p2p::{MyBehaviourEvent},
    notice,
    compute
};

use uuid::Uuid;

use risc0_zkvm::{
    Receipt, InnerReceipt, SuccinctReceipt, ReceiptClaim,
    Journal,
    // sha::Digest,
};
use hex::FromHex;

use mongodb::{
    bson,
    bson::{
        Bson,
        doc,
        oid::ObjectId
    },
    options::{
        ClientOptions,
        ServerApi,
        ServerApiVersion
    },
    Client
};

use dstorage::lighthouse;

mod job;
use job::Job;

mod recursion;
use recursion::{
    Recursion, Stage, SegmentStatus, Segment
};

mod db;

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


    #[arg(long, action)]
    dev: bool,

    #[arg(short, long)]
    key_file: Option<String>,
    
    #[command(subcommand)]
    job: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start a new job
    New {
        /// The job file on disk
        #[arg(short, long)]
        job_file: String,
    },

    /// Resume the job
    Resume {
        /// The job id
        #[arg(short, long)]
        job_id: String,        
    }
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

    // dStorage initialization
    let ds_key_file = cli.dstorage_key_file
        .ok_or_else(|| anyhow::Error::msg("dStorage key file is missing."))?;
    let lighthouse_config: lighthouse::Config = 
        toml::from_str(&fs::read_to_string(ds_key_file)?)?;
    let ds_key = lighthouse_config.apiKey;

    let ds_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60)) //@ how much timeout is enough?
        .build()?;

    // setup docker
    println!("[info] Connecting to the Docker daemon...");
    let docker_con = Docker::connect_with_socket_defaults()?;
    println!("[info] Docker daemon handle is up and ready.");

    // setup mongodb
    let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // future for offer evaluation
    let mut offer_evaluation_futures = FuturesUnordered::new();

    // future for local verification of succinct receipts
    let mut succinct_receipt_verification_futures = FuturesUnordered::new();

    // future for local verification of receipts
    let mut receipt_verification_futures = FuturesUnordered::new();
    
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
            println!("[warn] No keys were supplied, so one has been generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    println!("[info] Peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));  



    let col_jobs = db_client
        .database("wholesum")
        .collection::<db::Job>("jobs");
    let col_segments = db_client
        .database("wholesum")
        .collection::<db::Segment>("segments");
    let col_joins = db_client
        .database("wholesum")
        .collection::<db::Join>("joins");

    // the job
    let mut db_job_oid = bson::Bson::Null;
    let mut job = match &cli.job {
        Some(Commands::New{ job_file }) => {
            let job = new_job(job_file)?;
            db_job_oid = col_jobs.insert_one(
                db::Job {
                    id: job.id.clone(),
                    po2: job.schema.prove.po2,
                    segments_cid: job.schema.prove.segments_cid.clone(),
                    num_segments: job.schema.prove.num_segments,
                    stage: bson::to_bson(&Stage::Prove)?,
                    verification: db::Verification {
                        image_id: job.schema.verification.image_id.clone(),
                        journal_blob: 
                            fs::read(
                                job.schema.verification.journal_file_path.clone()
                            )?                 
                    },
                    snark_receipt: None,
                }
            )
            .await?
            .inserted_id;
            println!("[info] Job's progress will be recorded to the db, id: `{db_job_oid:?}`");
            job
        },

        Some(Commands::Resume{ job_id }) => {
            let (synced_job, joid) = resume_job(
                &db_client.database("wholesum").collection("jobs"),
                &col_segments,
                &col_joins,
                job_id
            )
            .await?;
            db_job_oid = joid;
            synced_job
        },

        _ => {
            panic!("[warn] Missing command, not sure what to do.");
        },
    };     
    // futures for mongodb progress saving 
    let mut db_insert_futures = FuturesUnordered::new();
    let mut db_job_update_futures = FuturesUnordered::new();

    // swarm 
    let mut swarm = comms::p2p::setup_swarm(&local_key).await?;
    let topic = gossipsub::IdentTopic::new("<-- p2p compute bazaar -->");
    let _ = swarm
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

    let mut timer_peer_discovery = stream::interval(Duration::from_secs(5 * 60)).fuse();

    let mut timer_post_jobs = stream::interval(Duration::from_secs(10)).fuse();
    // let mut idle_timer = stream::interval(Duration::from_secs(5 * 60)).fuse();
    // let mut timer_job_status = stream::interval(Duration::from_secs(30)).fuse();
    // gossip messages are content-addressed, so we add a random nounce to each message
    let mut rng = rand::thread_rng();

    // kick it off
    loop {
        select! {
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
                let needed_compute_type = match job.recursion.stage {
                    Stage::Prove => compute::ComputeType::ProveAndLift,
                    Stage::Join => compute::ComputeType::Join,
                    Stage::Stark => compute::ComputeType::Snark,
                    Stage::Snark => continue,                    
                };
                let need_compute = compute::NeedCompute {
                    criteria: compute::Criteria {
                        compute_type: needed_compute_type,                        
                    }
                };
                let notice_need_compute = notice::Notice::Compute(need_compute);                
                let gossip = &mut swarm.behaviour_mut().gossipsub;
                // println!("known peers: {:#?}", gossip.all_peers().collect::<Vec<_>>());
                if let Err(e) = gossip
                .publish(
                    topic.clone(),
                    bincode::serialize(&notice_need_compute)?
                ) {            
                    eprintln!("(gossip) `need compute` publish error: {e:?}");
                }
                
                // need status updates?
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
                            .segments
                            .iter_mut()
                            .filter(|some_seg|
                                if let SegmentStatus::ProveReady(_) = some_seg.status { true } else { false } 
                            )
                            .min_by(|x, y| x.num_prove_deals.cmp(&y.num_prove_deals))
                            .unwrap();                        
                        let cid = if let SegmentStatus::ProveReady(cid) = &chosen_segment.status { 
                            cid 
                        } else { 
                            eprintln!("[warn] `{}`'s cid is missing.", chosen_segment.id);
                            continue
                        };
                        chosen_segment.num_prove_deals += 1;                           
                        let _req_id = swarm.behaviour_mut()
                        .req_resp
                        .send_request(
                            &server_peer_id,
                            notice::Request::ComputeJob(compute::ComputeDetails {
                                job_id: format!("{}@{}", job.id, chosen_segment.id),
                                compute_type: compute::ComputeType::ProveAndLift,
                                input_type: compute::ComputeJobInputType::Prove(
                                    cid.to_string()
                                ),
                            })
                        );                     
                    },

                    compute::ComputeType::Join => {
                        if job.recursion.stage != Stage::Join {
                            continue;
                        }                        
                        let chosen_pair = job
                            .recursion
                            .join
                            .pairs
                            .iter_mut()
                            .min_by(|x, y| x.num_prove_deals.cmp(&y.num_prove_deals))
                            .unwrap();
                        //@ inc once ensured the req has been sent
                        chosen_pair.num_prove_deals += 1;                             
                        let _req_id = swarm.behaviour_mut()
                        .req_resp
                        .send_request(
                            &server_peer_id,
                            notice::Request::ComputeJob(compute::ComputeDetails {
                                job_id: format!("{}@{}-{}", job.id, chosen_pair.left, chosen_pair.right),
                                compute_type: compute::ComputeType::Join,
                                input_type: compute::ComputeJobInputType::Join(
                                    chosen_pair.left.clone(),
                                    chosen_pair.right.clone(),
                                ),
                            })
                        );                                             
                    },

                    compute::ComputeType::Snark => {
                        if job.recursion.stage != Stage::Stark {
                            continue;
                        }                                         
                    },
                }                             
            },

            // succinct receipt's verification has been finished
            v_res = succinct_receipt_verification_futures.select_next_some() => {                
                let  verification_res: VerificationResult = match v_res {
                    Err(failed) => {
                        let failed: VerificationError = failed;
                        match job.recursion.stage {
                            Stage::Prove => {
                                eprintln!("[warn] Proved receipt(`{}`)'s verification has failed. error: `{}`", 
                                  failed.item_id,
                                  failed.err_msg
                                );                                
                                if let Some(unverified_segment) = job.recursion.prove_and_lift
                                .segments.iter_mut()
                                .find(|some_seg| some_seg.id == failed.item_id) {
                                    unverified_segment.to_be_verified.remove(&failed.receipt_cid);
                                } else {
                                    eprintln!("[warn] Missing unverified segment's data.")
                                }
                            },

                            Stage::Join => {
                                eprintln!("[warn] Joined receipt(`{}`)'s verification has failed. error: `{:#?}`", 
                                  failed.item_id,
                                  failed.err_msg
                                );
                                job.recursion.join.to_be_verified.remove(&failed.receipt_cid);
                            },

                            _ => {}
                        }                        

                        continue;                        
                    },

                    Ok(r) => r
                };
                match job.recursion.stage {
                    Stage::Prove => {
                        println!(
                            "[info] Segment's proof is verified! \
                            segment: `{}`, receipt_cid: `{}`, prover: `{}`",
                            verification_res.item_id,
                            verification_res.receipt_cid,
                            verification_res.prover_id
                        );
                        let segment = job.recursion.prove_and_lift.segments.iter_mut()
                            .find(|x| x.id == verification_res.item_id);
                        if false == segment.is_some() {
                            eprintln!("[warn] Segment `{}` is missing.", verification_res.item_id);
                            continue;
                        }                
                        segment.unwrap().status = SegmentStatus::ProvedAndLifted(
                            verification_res.receipt_cid.clone()
                        );
                        // push the verified segment to the db
                        db_insert_futures.push(
                            col_segments.insert_one(
                                db::Segment {
                                    id: verification_res.item_id,
                                    job_id: db_job_oid.clone(),
                                    verified_blob: db::VerifiedBlob {
                                        cid: verification_res.receipt_cid.clone(),
                                        blob: verification_res.receipt_blob,
                                        prover: verification_res.prover_id
                                    }
                                }
                            )
                            .into_future()
                        );
                        // if all segments are verified, prepare for join
                        if true == job.recursion.prove_and_lift.is_finished() {
                            // begin join
                            println!("[info] All segments are proved and lifted. so, the join stage begins.");
                            job.recursion.begin_join();
                            // update stage to Join on db
                            db_job_update_futures.push(
                                col_jobs.update_one(
                                    doc! {
                                        "_id": db_job_oid.clone()
                                    },
                                    doc! {
                                        "$set": doc! {
                                            "stage": bson::to_bson(&Stage::Join)?,
                                        }
                                    }
                                )
                                .into_future()
                            );
                            if true == job.recursion.join.begin_next_round() {
                                //@ a single segmented job does not need joins
                                receipt_verification_futures.push(
                                    verify_stark_receipt(
                                        format!("{}/prove/{}",
                                            job.working_dir,
                                            verification_res.receipt_cid
                                        ),
                                        job.schema.verification.journal_file_path.clone(),
                                        job.schema.verification.image_id.clone()
                                    )
                                );
                            }
                        }
                        
                    },

                    Stage::Join => {
                        println!(
                            "[info] Join's proof is verified! \
                            pair: `{}`, receipt_cid: `{}`, prover: `{}`",
                            verification_res.item_id, verification_res.receipt_cid, verification_res.prover_id
                        );
                        if false == job.recursion.join.to_be_verified.contains_key(&verification_res.receipt_cid) {
                            eprintln!("[warn] Missing join receipt.");
                            continue;
                        }
                        let (left_cid, right_cid) = {
                            let tokens: Vec<&str> = verification_res.item_id.split("-").collect();                               
                            (
                                String::from(tokens[0]),
                                String::from(tokens[1])
                            )
                        };

                        let joined_pair_index = job.recursion.join.pairs
                            .iter()
                            .position(|p| p.left == left_cid && p.right == right_cid);
                        if true == joined_pair_index.is_none() {
                            eprintln!("[warn] Missing join pair.");
                            continue;
                        }
                        let joined_pair = job.recursion.join.pairs.swap_remove(joined_pair_index.unwrap());
                        job.recursion.join.joined.insert(
                            joined_pair.position,
                            verification_res.receipt_cid.clone()
                        );
                        // record join to db
                        println!("join round for insert {}", job.recursion.join.round);
                        db_insert_futures.push(
                            col_joins.insert_one(
                                db::Join {
                                    job_id: db_job_oid.clone(),
                                    round: job.recursion.join.round.try_into().unwrap_or_else(|_| 0u32),
                                    verified_blob: db::VerifiedBlobForJoin {
                                        input_cid_left: left_cid,
                                        input_cid_right: right_cid,
                                        cid: verification_res.receipt_cid,
                                        blob: verification_res.receipt_blob,
                                        prover: verification_res.prover_id
                                    }
                            })
                            .into_future()
                        );
                        if true == job.recursion.join.begin_next_round() {
                            // begin snark extraction?
                            println!("[info] Join is complete. Stark receipt is here, let's verify it.");
                            receipt_verification_futures.push(
                                verify_stark_receipt(
                                    verification_res.receipt_file_path,
                                    job.schema.verification.journal_file_path.clone(),
                                    job.schema.verification.image_id.clone()
                                )
                            );
                        }                        
                    },

                    _ => {},
                }
            },

            v_res = receipt_verification_futures.select_next_some() => {
                let receipt: Receipt = match v_res {
                    Err(failed) => {
                        //@ which segment/join/...? 
                        eprintln!("[warn] Stark receipt's verification has failed. error: `{:#?}`", failed);
                        continue;                        
                    },

                    Ok(r) => r
                };
                job.recursion.stage = Stage::Stark;
                println!("[info] Stark receipt has been verified, join aggregation is complete.");
                db_job_update_futures.push(
                    col_jobs.update_one(
                        doc! {
                            "_id": db_job_oid.clone()
                        },
                        doc! {
                            "$set": doc! {
                                "stage": bson::to_bson(&Stage::Stark)?,
                            }
                        }
                    )
                    .into_future()
                );
            },

            res = db_insert_futures.select_next_some() => {
                match res {
                    Err(e) => eprintln!("[warn] Failed to record progress(insert) to the db: `{:#?}`", e),

                    Ok(oid) => println!("[info] Progress recorded(insert) to the db: `{:?}`", oid)
                }                
            },

            res = db_job_update_futures.select_next_some() => {
                match res {
                    Err(e) => eprintln!("[warn] Failed to record progress(update) to the db: `{:#?}`", e), 

                    Ok(oid) => println!("[info] Progress recorded(update) to the db: `{:?}`", oid)
                } 
            }


            // libp2p events
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
                    // println!("Inbound identify event `{:#?}`", info);
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

                        notice::Request::UpdateForJobs(new_updates) => {
                            let to_be_locally_verified = update_jobs(
                                &mut job,
                                server_peer_id,
                                new_updates
                            );
                            let stage_str = match job.recursion.stage {
                                Stage::Prove => String::from("prove"),
                                Stage::Join  => format!("join/r{}", job.recursion.join.round),
                                Stage::Stark => String::from("stark"),
                                Stage::Snark => String::from("snark"),
                            };
                            for verification_item in to_be_locally_verified {
                                succinct_receipt_verification_futures.push(                                    
                                    verify_succinct_receipt(
                                        &ds_client,
                                        format!("{}/{stage_str}", job.working_dir),
                                        verification_item.prover_id,
                                        verification_item.receipt_cid,
                                        verification_item.item_id
                                    )
                                );
                            }                            
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

pub fn get_home_dir() -> anyhow::Result<String> {
    let err_msg = "Home dir is not available";
    let binding = home::home_dir()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    let home_dir = binding.to_str()
        .ok_or_else(|| anyhow::Error::msg(err_msg))?;
    Ok(home_dir.to_string())
}

async fn mongodb_setup(
    uri: &str,
) -> anyhow::Result<mongodb::Client> {
    println!("[info] Connecting to the MongoDB daemon...");
    let mut client_options = ClientOptions::parse(
        uri
    ).await?;
    let server_api = ServerApi::builder().version(
        ServerApiVersion::V1
    ).build();
    client_options.server_api = Some(server_api);
    let client = mongodb::Client::with_options(client_options)?;
    // Send a ping to confirm a successful connection
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await?;
    println!("[info] Successfully connected to the MongoDB instance!");
    Ok(client)
}

pub fn new_job(
    job_file: &str
) -> anyhow::Result<job::Job> {
    let schema: job::Schema = toml::from_str(
        &fs::read_to_string(job_file)?
    )?;

    let generated_job_id = Uuid::new_v4().simple().to_string()[..4].to_string();
    let working_dir = format!(
        "{}/.wholesum/jobs/client/{}",
        get_home_dir()?,
        generated_job_id
    );
    // create folders for residues
    for action in ["prove", "stark", "snark"] {
        fs::create_dir_all(
            format!("{working_dir}/{action}")
        )?;
    }
    if schema.prove.num_segments == 0 {
        eprintln!("[warn] Number of segments is 0.");
    }
    let num_rounds = (schema.prove.num_segments as f32).log2().ceil() as u32;
    for round in 0..num_rounds {
        fs::create_dir_all(
            format!("{working_dir}/join/r{round}")
        )?;
    } 
    let rec = recursion::Recursion::new(
        &schema.prove.segments_cid,
        schema.prove.num_segments as usize
    );    
    Ok(
        Job {
            id: generated_job_id.clone(),
            working_dir: working_dir,
            schema: schema,
            recursion: rec,
        }
    )
        
}

async fn resume_job(
    col_jobs_generic: &mongodb::Collection<bson::Document>,
    col_segments: &mongodb::Collection<db::Segment>,
    col_joins: &mongodb::Collection<db::Join>,
    job_id: &str,
) -> anyhow::Result<(job::Job, Bson)> {
    println!("[info] Resuming job `{job_id}`");
    let doc = col_jobs_generic
        .find_one(doc! {
            "id": job_id.to_string()
        })
        .await?
        .ok_or_else(|| anyhow::anyhow!("No such job in db."))?;        
    let db_job_oid = 
        doc.get("_id")
        .ok_or_else(|| anyhow::anyhow!("`_id` is not available"))?
        .clone();
    println!("[info] Job is found, oid: `{db_job_oid:?}`");
    let db_job: db::Job = bson::from_document(doc)?;
    let working_dir = format!("{}/.wholesum/jobs/client/{}",
        get_home_dir()?,
        job_id
    );
    // create folders for residues
    for action in ["prove", "stark", "snark"] {
        fs::create_dir_all(
            format!("{working_dir}/{action}")
        )?;
    }
    let num_rounds = (db_job.num_segments as f32).log2().ceil() as u32;
    for round in 0..num_rounds {
        fs::create_dir_all(
            format!("{working_dir}/join/r{round}")
        )?;
    }
    let mut job = Job {
        id: job_id.to_string(),            
        working_dir: working_dir.clone(),
        schema: job::Schema { 
            prove: job:: ProveConfig {
                po2: db_job.po2,
                num_segments: db_job.num_segments,
                segments_cid: db_job.segments_cid.clone(),
            },
            verification: job::VerificationConfig {
                journal_file_path: {
                    let journal_file_path = format!("{working_dir}/journal");
                    fs::write(
                        journal_file_path.clone(),
                        &db_job.verification.journal_blob
                    )?;
                    journal_file_path
                },
                image_id: db_job.verification.image_id,
            },
        },

        recursion: recursion::Recursion {
            stage: Stage::Prove,    
            prove_and_lift: recursion::ProveAndLift::new(
                &db_job.segments_cid,
                db_job.num_segments as usize
            ),    
            join: recursion::Join::new(0),
            snark: None,
        } 
    };
    // prove and lift
    let verified_segments = retrieve_verified_segments_from_db(
        col_segments,
        &db_job_oid,
        &job.working_dir
    ).await?;
    if verified_segments.len() == 0 {
        println!("[warn] No verified segments to sync with.");
        return Ok(
            (
                job,
                db_job_oid
            )
        )
    }
    // sync segments with the db
    for db_segment in verified_segments.iter() {
        if let Some(found_segment) = job
            .recursion
            .prove_and_lift
            .segments
            .iter_mut()
            .find(|some_seg| some_seg.id == db_segment.id)
        {
            found_segment.status = db_segment.status.clone();
             println!(
                "[info] Segment `{}` is synced, status: `{:?}`.",
                db_segment.id,
                db_segment.status                    
            );
        } else {
            eprintln!("[warn] Segment with id `{}` from the db is not found.", db_segment.id);
        }
    }
    if true == job.recursion.prove_and_lift.is_finished() {
        println!("[info] The prove and lift stage is set to finished according to the data from the local db.");
        job.recursion.begin_join();
        if true == job.recursion.join.begin_next_round() {
            println!("[info] Join is already finished.");
            return Ok(
                (
                    job,
                    db_job_oid
                )
            )            
        }
    }
    // join
    let join_btree = retrieve_verified_joins_from_db(
        col_joins,
        &verified_segments,
        &db_job_oid,
        &job.working_dir
    ).await?;
    if join_btree.len() == 0 {
        eprintln!("[warn] No verified joins to sync with.");
        return Ok(
            (
                job,
                db_job_oid
            )
        )
    }
    let db_cur_round = join_btree.keys().last().unwrap();
    // println!("db round: {db_cur_round}");    
    // println!("db rounds: {:?}", join_btree.keys());
    // emulate a full join and sync with    
    loop {
        let round_joins = match join_btree
            .get(&(job.recursion.join.round as u32))
        {
            Some(v) => v,
            None => {
                println!("no more joins is possible.");
                break;
            }
        };            
        // println!("r: {}, len: {}", job.recursion.join.round, round_joins.len());
        for db_join in round_joins {
            let joined_pair_index = job
                .recursion
                .join
                .pairs
                .iter()
                .position(|p| 
                    p.left == db_join.verified_blob.input_cid_left &&
                    p.right == db_join.verified_blob.input_cid_right
                );
            if true == joined_pair_index.is_none() {
                eprintln!("[warn] Missing join pair, sync may be incomplete.");
                continue;
            }
            // println!("removing pair {}-{}", db_join.verified_blob.input_cid_left,db_join.verified_blob.input_cid_right);
            let joined_pair = job.recursion.join.pairs.swap_remove(joined_pair_index.unwrap());
            job.recursion.join.joined.insert(
                joined_pair.position,
                db_join.verified_blob.cid.clone()
            );            
        }
        if true == job.recursion.join.begin_next_round() ||
           job.recursion.join.round as u32 == *db_cur_round
        {            
            break;
        }
    }
    // println!("{:#?}", job.recursion.join);
    println!("[info] Join is synced with the db.");
    Ok(
        (
            job,
            db_job_oid
        )
    )    
}

async fn retrieve_verified_segments_from_db(
    col_segments: &mongodb::Collection<db::Segment>,
    db_job_oid: &Bson,
    working_dir: &str,
) -> anyhow::Result<Vec<recursion::Segment>> {
    let mut segments = Vec::<recursion::Segment>::new();
    // retrieve all proved segments
    println!("[info] Retrieving verified segments from the db...");
    let mut cursor = col_segments.find(
        doc! {
            "job_id": db_job_oid
        }
    )
    .await?;
    while let Some(db_segment) = cursor.try_next().await? {        
        segments.push(
            recursion::Segment {
                id: db_segment.id.clone(),
                num_prove_deals: 0,
                //@ assume that segments are proved and "verified"
                status: recursion::SegmentStatus::ProvedAndLifted(
                    db_segment.verified_blob.cid.clone()
                ),
                to_be_verified: {
                    let mut to_be_verified = HashMap::new();
                    to_be_verified.insert(
                        db_segment.verified_blob.cid.clone(),
                        db_segment.verified_blob.prover
                    );
                    to_be_verified
                },            
            }
        );
        fs::write(
            format!(
                "{}/prove/{}",
                working_dir,
                db_segment.verified_blob.cid
            ),
            &db_segment.verified_blob.blob
        )?;                
    }
    Ok(segments)
}

async fn retrieve_verified_joins_from_db(
    col_joins: &mongodb::Collection<db::Join>,
    verified_segments: &Vec<recursion::Segment>,
    db_job_oid: &Bson,
    working_dir: &str,
) -> anyhow::Result<BTreeMap<u32, Vec<db::Join>>> {
    println!("[info] Retrieving verified joins from the db...");
    // println!("{db_job_oid:?}");
    let mut cursor = col_joins.find(
        doc! {
            "job_id": db_job_oid
        }
    )
    // .projection(
    //     doc! {
    //         "verified_blob": 0
    //     }
    // )
    .sort(
        doc! {
            "round": 1 
        }
    )
    .await?;
    let mut join_btree = BTreeMap::<u32, Vec<db::Join>>::new();
    while let Some(db_join) = cursor.try_next().await? {  
        // println!("{}, {}", db_join.round, db_join.verified_blob.cid);      
        join_btree.entry(db_join.round as u32)
            .and_modify(|j| j.push(db_join.clone()))
            .or_insert(vec![db_join]);
    }
    // emulate
    Ok(join_btree)
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
}

#[derive(Debug, Clone)]
struct VerificationItem {
    pub receipt_cid: String,
    pub prover_id: String,
    pub item_id: String,
}

// process bulk status updates
fn update_jobs(
    job: &mut Job,
    prover_peer_id: PeerId,
    new_updates: Vec<compute::JobUpdate>,
) -> Vec<VerificationItem> {
    let mut to_be_locally_verified = vec![];
    for new_update in new_updates {
        let prover_id = prover_peer_id.to_string();
        // println!("[info] Status update for job from `{}`: `{:#?}`",
        //     prover_id, new_update
        // );

        let (job_id, seg_id) = {            
            // job-id: "job_id@seg_id"
            let tokens: Vec<&str> = new_update.id.split("@").collect();
            if tokens.len() != 2 {
                eprintln!("[warn] update_id `{:?}` is invalid for the `prove and lift`/`to snark` compute type.",
                    new_update.id
                );
                continue;
            }               
            (
                String::from(tokens[0]),
                String::from(tokens[1])
            )
        };
        if job.id != job_id {
            println!("[warn] Status update for an unknown job `{job_id}`, ignored.");
            continue;
        }

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
                        .find(|some_seg| some_seg.id == seg_id) {                            
                            if let SegmentStatus::ProveReady(_) = proved_segment.status { 
                                println!("Segment `{seg_id}` is proved and lifted, waiting to be verified.");
                                //@ prevent multiple verifications
                                if false == proved_segment.to_be_verified.contains_key(receipt_cid) {
                                    proved_segment.to_be_verified.insert(
                                        receipt_cid.to_string(),
                                        prover_id.clone()
                                    );
                                    to_be_locally_verified.push(
                                        VerificationItem {
                                            receipt_cid: receipt_cid.to_string(),
                                            prover_id: prover_id.clone(),
                                            item_id: proved_segment.id.clone()
                                        }
                                    );
                                }
                            } else {
                                println!("Segment `{seg_id}` is already proved, lifted, and verified.");                                                                
                            }
                            
                        } else {
                            println!("[warn] Prove for an unknown segment `{seg_id}`");
                        }
                    },

                    compute::ComputeType::Join => {
                        if job.recursion.stage != Stage::Join { 
                            continue;
                        }                        
                        //@ wtd with this data?
                        if false == job.recursion.join.to_be_verified.contains_key(receipt_cid) {
                            let join_id = seg_id;
                            if join_id.split("-").count() != 2 {
                                eprintln!("[warn] Join pair `{}` is invalid.",
                                    join_id
                                );
                                continue;
                            }     
                            job.recursion.join.to_be_verified.insert(
                                receipt_cid.to_string(),
                                prover_id.clone()
                            );
                            to_be_locally_verified.push(
                                VerificationItem {
                                    receipt_cid: receipt_cid.to_string(),
                                    prover_id: prover_id.clone(),
                                    item_id: join_id.clone()
                                }
                            );
                        }
                        
                    },

                    compute::ComputeType::Snark => {
                        if job.recursion.stage != Stage::Stark { 
                            continue;
                        }
                        job.recursion.snark = Some(receipt_cid.to_string());
                        job.recursion.stage = Stage::Snark;
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
    to_be_locally_verified    
}

#[derive(Debug, Clone)]
struct VerificationResult {
    pub prover_id: String,
    pub receipt_cid: String,
    pub item_id: String,
    pub receipt_file_path: String,
    pub receipt_blob: Vec<u8>
}

#[derive(Debug, Clone)]
struct VerificationError {
    pub receipt_cid: String,
    pub item_id: String,
    pub err_msg: String,
}

// verify proved and lifted receipt 
async fn verify_succinct_receipt(
    ds_client: &reqwest::Client,
    residue_path: String,
    prover_id: String,
    receipt_cid: String,
    item_id: String
) -> Result<VerificationResult, VerificationError> {    
    let blob_file_path = format!("{residue_path}/{receipt_cid}");
    println!("dl: {blob_file_path}");
    lighthouse::download_file(
        ds_client,
        &receipt_cid,
        blob_file_path.clone()
    ).await
    .map_err(|e| VerificationError {
        receipt_cid: receipt_cid.clone(),
        item_id: item_id.clone(),
        err_msg: format!("Receipt download error: `{}`",  e.to_string())
    })?;
    let blob = fs::read(blob_file_path.clone())
        .map_err(|e| VerificationError {
            receipt_cid: receipt_cid.clone(),
            item_id: item_id.clone(),
            err_msg: format!("Receipt read error: `{}`",  e.to_string())
        })?;
    let succinct_receipt: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(
        &blob        
    )
    .map_err(|e| VerificationError {
        receipt_cid: receipt_cid.clone(),
        item_id: item_id.clone(),
        err_msg: format!("Receipt decode error: `{}`",  e.to_string())
    })?;
    match succinct_receipt.verify_integrity() {
        Err(e) => Err(VerificationError {
            receipt_cid: receipt_cid.clone(),
            item_id: item_id.clone(),
            err_msg: format!("Receipt verification error: `{}`",  e.to_string())
        }),

        Ok(_) => Ok(VerificationResult {
            prover_id: prover_id,
            receipt_cid: receipt_cid,
            item_id: item_id,
            receipt_file_path: blob_file_path,
            receipt_blob: blob,
        })
    }
}

// verify the stark receipt obtained from a joined succinct receipt
async fn verify_stark_receipt(
    succint_receipt_file_path: String,
    journal_file_path: String,
    image_id: String,
) -> anyhow::Result<Receipt> {
    let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(
        &std::fs::read(succint_receipt_file_path)?
    )?;
    let journal: Journal = bincode::deserialize(
        &std::fs::read(journal_file_path)?
    )?;
    let stark_receipt = Receipt::new(
        InnerReceipt::Succinct(sr),
        journal.bytes
    );    
    stark_receipt.verify(
        <[u8; 32]>::from_hex(&image_id)?
    )?;
    Ok(stark_receipt)
}