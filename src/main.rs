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
use std::collections::{
    // BTreeMap,
    HashMap
};
use std::{
    fs,
    time::{
        Instant, 
        Duration,
    },
    future::IntoFuture,
};
use bincode;

use toml;
use tracing_subscriber::EnvFilter;
use anyhow;
// use reqwest;

use clap::{
    Parser, Subcommand
};

use uuid::Uuid;
use xxhash_rust::xxh3;
use risc0_zkvm::{
    Receipt, InnerReceipt, SuccinctReceipt, ReceiptClaim,
    Journal,
};
use hex::FromHex;
use log::{info, warn};
use mongodb::{
    bson,
    bson::{
        // Bson,
        doc,
    },
    options::{
        ClientOptions,
        ServerApi,
        ServerApiVersion
    },
};
// use rand::Rng;
use rand::prelude::IndexedRandom;

use comms::{
    p2p::{MyBehaviourEvent},
    protocol
};

mod job;
use job::Job;

mod recursion;
use recursion::Stage;

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
    info!("<-> Client agent for Wholesum network <->");
    info!("Operating mode: `{}` network",
        if false == cli.dev {"global"} else {"local(development)"}
    );

    // setup mongodb
    let db_client = mongodb_setup("mongodb://localhost:27017").await?;

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
            warn!("No keys were supplied, so one has been generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    info!("Peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));  
    
    // futures for local verification of succinct proofs
    // let mut succinct_proof_verification_futures = FuturesUnordered::new();
    let mut join_proof_verification_futures = FuturesUnordered::new();

    let col_jobs = db_client
        .database("wholesum_client")
        .collection::<db::Job>("jobs");
    let col_segments = db_client
        .database("wholesum_client")
        .collection::<db::Segment>("segments");
    let col_joins = db_client
        .database("wholesum_client")
        .collection::<db::Join>("joins");
    let col_groth16 = db_client
        .database("wholesum_client")
        .collection::<db::Groth16>("groth16");
    
    // futures for mongodb progress saving 
    let mut db_insert_futures = FuturesUnordered::new();
    let mut db_update_futures = FuturesUnordered::new();

    // the job
    let (mut job, db_job_oid) = match &cli.job {
        Some(Commands::New{ job_file }) => {
            let job = new_job(job_file)?;            
            let oid = col_jobs.insert_one(
                db::Job {
                    id: job.id.clone(),                    
                    num_segments: job.schema.prove.num_segments,
                    stage: bson::to_bson(&Stage::Prove)?,
                    verification: db::Verification {
                        image_id: job.schema.verification.image_id.clone(),
                        journal_blob: job.journal.clone(),           
                    },
                    snark_proof: None,
                }
            )
            .await?
            .inserted_id;
            (job, oid)
        },

        // Some(Commands::Resume{ job_id }) => {
            // resume_job(
            //     &db_client.database("wholesum_client").collection("jobs"),
            //     &col_segments,
            //     &col_joins,
            //     job_id
            // )
            // .await?
        // },

        _ => {
            panic!("Missing command, not sure what you meant.");
        },
    };
    info!("Job's progress will be recorded to the DB with Id: `{db_job_oid:?}`");

    // swarm 
    let mut swarm = comms::p2p::setup_swarm(&local_key).await?;
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic); 

    // bootstrap 
    if false == cli.dev {
        // get to know bootnodes
        const BOOTNODES: [&str; 1] = [
            "TBD",
        ];
        for peer in &BOOTNODES {
            swarm.behaviour_mut()
                .kademlia
                .add_address(&peer.parse()?, "/ip4/W.X.Y.Z/tcp/20201".parse()?);
        }
        // find myself
        if let Err(e) = 
            swarm
                .behaviour_mut()
                .kademlia
                .bootstrap() {
            warn!("Failed to bootstrap Kademlia: `{:?}`", e);

        } else {
            info!("Self-bootstraping is initiated.");
        }
    }

    // if let Err(e) = swarm.behaviour_mut().kademlia.bootstrap() {
    //     eprintln!("[warn] Failed to bootstrap Kademlia: `{:?}`", e);
    // }
    
    // listen on all interfaces and whatever port the os assigns
    //@ should read from the config file
    swarm.listen_on("/ip4/0.0.0.0/udp/20201/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/udp/20201/quic-v1".parse()?)?;

    // used for networking
    let mut timer_peer_discovery = stream::interval(Duration::from_secs(5 * 60)).fuse();
    // used for posting job needs
    let mut timer_post_job = stream::interval(Duration::from_secs(60)).fuse();
    let mut timer_post_update = stream::interval(Duration::from_secs(600)).fuse();

    // if job.recursion.stage == Stage::Stark {
    //     // verify the final join proof                                                
    //     info!("Let's verify the final join proof.");
    //     let join_proof_cid = job.recursion.join_proof.clone().unwrap();
    //     let join_proof_filepath = format!("{}/join/proof", job.working_dir);
    //     if let Err(e) = lighthouse::download_file(
    //         &ds_client,
    //         &join_proof_cid,
    //         join_proof_filepath.clone()
    //     ).await {
    //         warn!("Proof download failed: `{e:?}`");
    //     } else {                                                
    //         join_proof_verification_futures.push(
    //             verify_join_proof(
    //                 join_proof_filepath.clone(),
    //                 job.schema.verification.journal_file_path.clone(),
    //                 job.schema.verification.image_id.clone(),
    //             )
    //         );                                                    
    //     }
    // }

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

            // post need compute
            () = timer_post_job.select_next_some() => {
                if let Err(_) = i_need_compute(
                    &mut swarm.behaviour_mut().gossipsub,
                    &topic,
                    &job
                ) {
                    // do nothing
                }                 
            },

            // post job update needs
            () = timer_post_update.select_next_some() => {
                if let Err(_) = i_need_update(
                    &mut swarm.behaviour_mut().gossipsub,
                    &topic,
                    &job.id,
                ) {
                    // do nothing
                }
            },

            v_res = join_proof_verification_futures.select_next_some() => {
                if let Err(err_msg) = v_res {                    
                        //@ which segment/join/... to blame? 
                        warn!("Failed to verify the final join proof: `{err_msg:?}`");
                        continue;                        
                }
                info!("[info] Bingo! the final join proof has been verified, recursion is complete.");
                job.recursion.stage = Stage::Snark;
                db_update_futures.push(
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
                    Err(e) => eprintln!("[warn] DB insert was failed: `{:#?}`", e),

                    Ok(oid) => println!("[info] DB insert was successful: `{:?}`", oid)
                }                
            },

            res = db_update_futures.select_next_some() => {
                match res {
                    Err(e) => eprintln!("[warn] DB update was failed: `{:#?}`", e),

                    Ok(oid) => println!("[info] DB update was successful: `{:?}`", oid)
                } 
            },

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
                    peer: prover_peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        // prover indicates her interest to compute                        
                        protocol::Request::WouldCompute => {
                            let mut rng = rand::rng();
                            match job.recursion.stage {
                                Stage::Prove => {                                    
                                    // pick a random unproved segment and send it                                    
                                    let mut unproved_segments = vec![];
                                    for (segment_index, is_proved) in job.recursion.prove_and_lift.progress_map.iter().enumerate() {
                                        if true == is_proved {
                                            continue;
                                        }                                        
                                        unproved_segments.push(segment_index);                                        
                                    }
                                    if unproved_segments.len() == 0 {
                                        warn!("No more segments to prove.");
                                        continue;
                                    }
                                    let chosen_segment_index = *unproved_segments.choose(&mut rng).unwrap();
                                    info!("Picked `segment {chosen_segment_index}` to prove.");
                                    let chosen_segment = job.recursion.prove_and_lift.segments
                                        .get_mut(chosen_segment_index)
                                        .unwrap();
                                    let compute_job = protocol::ComputeJob {
                                        job_id: job.id.clone(),
                                        job_type: protocol::JobType::Prove(
                                            protocol::ProveDetails {
                                                segment_id: chosen_segment_index as u32,
                                                segment_blob: chosen_segment.blob.clone(),
                                            }
                                        )
                                    };
                                    if let Err(err_msg) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send segment for proving: `{err_msg:?}`")
                                    } else {
                                        chosen_segment.num_deals += 1;
                                    }
                                },

                                Stage::Join => {                                    
                                    let round = job.recursion.join_rounds.last_mut().unwrap();
                                    // pick a random unproved pair and send it                                    
                                    let mut unproved_pairs = vec![];
                                    for (pair_index, is_proved) in round.progress_map.iter().enumerate() {
                                        if false == is_proved {
                                            unproved_pairs.push(pair_index);
                                        }                                        
                                    }
                                    if unproved_pairs.len() == 0 {
                                        warn!("No more pairs to prove.");
                                        continue;
                                    }
                                    let chosen_pair_index = *unproved_pairs.choose(&mut rng).unwrap();
                                    info!("Picked `pair {chosen_pair_index}` to prove.");

                                    let chosen_pair = round.pairs.get_mut(chosen_pair_index).unwrap();                                    
                                    let compute_job = protocol::ComputeJob {
                                        job_id: job.id.clone(),
                                        job_type: protocol::JobType::Join(
                                            protocol::JoinDetails {
                                                pair_id: chosen_pair_index as u32,
                                                blob_pair: (
                                                    chosen_pair.left_blob.clone(),
                                                    chosen_pair.right_blob.clone()
                                                )
                                            }
                                        )
                                    };
                                    if let Err(err_msg) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send join pair for proving: `{err_msg:?}`")
                                    } else {
                                        chosen_pair.num_deals += 1;
                                    }
                                }

                                Stage::Snark => {
                                    let compute_job = protocol::ComputeJob {
                                        job_id: job.id.clone(),
                                        job_type: protocol::JobType::Groth16(
                                            protocol::Groth16Details {
                                                proof_blob: job.recursion.join_proof.clone().unwrap()
                                            }
                                        )
                                    };
                                    if let Err(err_msg) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send the STARK proof for Groth16 extraction: `{err_msg:?}`")
                                    }

                                },

                                _ => {}
                            }                            
                        },

                        // prover has finished its task
                        protocol::Request::ProofIsReady(new_proofs) => {
                            for new_proof in new_proofs {
                                if job.id != new_proof.job_id {
                                    warn!("Status update for an unknown job `{new_proof:?}`, ignored.");
                                    continue;
                                }
                                match new_proof.proof_type {
                                    protocol::ProofType::Prove(seg_id) => {
                                        if job.recursion.stage != Stage::Prove {                             
                                            continue;
                                        }
                                        if seg_id >= job.recursion.prove_and_lift.segments.len() as u32 {
                                            warn!(
                                                "Invalid segment id `{}`, valid range: `0 - {}`",
                                                seg_id,
                                                job.schema.prove.num_segments - 1
                                            );
                                            continue;
                                        }                                     
                                        let blob_hash = xxh3::xxh3_64(&new_proof.blob);
                                        let mut segment_already_proved = false;
                                        let mut proof_should_be_logged = true;
                                        let prover_id = prover_peer_id.to_string();
                                        job.recursion
                                        .prove_and_lift
                                        .proofs
                                        .entry(seg_id)
                                            .and_modify(|all_proofs| {
                                                segment_already_proved = true;                                                    
                                                all_proofs
                                                    .entry(prover_id.clone())
                                                    .and_modify(|prover_proofs| {
                                                        match prover_proofs
                                                            .iter()
                                                            .find(|p: &&recursion::Proof| p.blob_hash == blob_hash) {
                                                                None => {
                                                                    prover_proofs.push(
                                                                        recursion::Proof {                                                            
                                                                            blob: new_proof.blob.clone(),
                                                                            blob_hash: blob_hash,
                                                                            spent: false,
                                                                        }
                                                                    );
                                                                    info!("Another proof for `segment {seg_id:?}`.");
                                                                },

                                                                Some(_) => {
                                                                    // duplicate proof
                                                                    proof_should_be_logged = false;
                                                                }
                                                            }
                                                    });
                                            })                                            
                                            .or_insert_with(|| {
                                                let mut proofs = HashMap::new();
                                                proofs.insert(
                                                    prover_id.clone(),
                                                    vec![
                                                        recursion::Proof {
                                                            blob_hash: blob_hash,
                                                            blob: new_proof.blob.clone(),
                                                            spent: false,
                                                        }
                                                    ]
                                                );
                                                proofs
                                            });
                                        if false == segment_already_proved {                                            
                                            //@ need to validate received proof somehow
                                            job.recursion.prove_and_lift.progress_map.set(seg_id as usize, true);
                                            info!(
                                                "`Segment {}` is proved, progress_map: {:?}",
                                                seg_id,
                                                job.recursion.prove_and_lift.progress_map
                                            );
                                        
                                            if true == job.recursion.prove_and_lift.is_finished() {
                                                info!("Prove stage is finished.");
                                                db_update_futures.push(
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
                                            }
                                        }
                                        // db
                                        if true == proof_should_be_logged {
                                            db_insert_futures.push(
                                                col_segments.insert_one(
                                                    db::Segment {
                                                        id: seg_id,
                                                        job_id: db_job_oid.clone(),
                                                        proof: db::Proof {
                                                            prover: prover_id.clone(),
                                                            blob: new_proof.blob.clone(),
                                                            hash: blob_hash,
                                                        }
                                                    }
                                                )
                                                .into_future()
                                            );
                                        }                                        
                                    },

                                    protocol::ProofType::Join(pair_id) => {
                                        if job.recursion.stage != Stage::Join { 
                                            continue;
                                        }
                                        let round = job.recursion.join_rounds.last_mut().unwrap();
                                        let round_number = round.number;

                                        if pair_id >= round.pairs.len() as u32 {
                                            warn!(
                                                "Invalid pair id `{}`, valid range: `0 - {}`",
                                                pair_id,
                                                job.schema.prove.num_segments - 1
                                            );
                                            continue;
                                        }                                     
                                        let blob_hash = xxh3::xxh3_64(&new_proof.blob);
                                        let mut pair_already_proved = false;
                                        let mut proof_should_be_logged = true;
                                        let prover_id = prover_peer_id.to_string();                                        
                                        round.proofs
                                        .entry(pair_id)
                                            .and_modify(|all_proofs| {
                                                pair_already_proved = true;                                                    
                                                all_proofs
                                                .entry(prover_id.clone())
                                                    .and_modify(|prover_proofs| {
                                                        match prover_proofs
                                                            .iter()
                                                            .find(|p: &&recursion::Proof| p.blob_hash == blob_hash) {
                                                                None => {
                                                                    prover_proofs.push(
                                                                        recursion::Proof {                                                            
                                                                            blob: new_proof.blob.clone(),
                                                                            blob_hash: blob_hash,
                                                                            spent: false,
                                                                        }
                                                                    );
                                                                    info!("Another proof for `pair {pair_id:?}`.");
                                                                },

                                                                Some(_) => {
                                                                    // duplicate proof
                                                                    proof_should_be_logged = false;
                                                                }
                                                            }
                                                    });
                                            })                                            
                                            .or_insert_with(|| {
                                                let mut proofs = HashMap::new();
                                                proofs.insert(
                                                    prover_id.clone(),
                                                    vec![
                                                        recursion::Proof {
                                                            blob_hash: blob_hash,
                                                            blob: new_proof.blob.clone(),
                                                            spent: false,
                                                        }
                                                    ]
                                                );
                                                proofs
                                            });
                                        // db
                                        if true == proof_should_be_logged {
                                            db_insert_futures.push(
                                                col_joins.insert_one(
                                                    db::Join {
                                                        job_id: db_job_oid.clone(),
                                                        round: round_number,
                                                        pair_id: pair_id,
                                                        proof: db::Proof {
                                                            prover: prover_id.clone(),
                                                            blob: new_proof.blob.clone(),
                                                            hash: blob_hash,
                                                        }
                                                    }
                                                )
                                                .into_future()
                                            );
                                        }
                                        if false == pair_already_proved {
                                            if true == round.progress_map.all() {
                                                if true == job.recursion.begin_next_join_round() {
                                                    // verify the final join proof                                                
                                                    info!("Let's verify the final join proof.");                                                    
                                                    join_proof_verification_futures.push(
                                                        verify_join_proof(
                                                            new_proof.blob.clone(),
                                                            job.journal.clone(),
                                                            job.schema.verification.image_id.clone(),
                                                        )
                                                    );                                                    
                                                }                             
                                            }
                                        }
                                    },

                                    protocol::ProofType::Groth16 => {
                                        if job.recursion.stage != Stage::Snark { 
                                            continue;
                                        }                                        
                                        info!("A Groth16 proof has been extracted.");                                        
                                        let blob_hash = xxh3::xxh3_64(&new_proof.blob);
                                        let mut groth16_already_extracted = false;
                                        let mut proof_should_be_logged = true;
                                        let prover_id = prover_peer_id.to_string();                                        
                                        job.recursion
                                        .groth16_proofs
                                        .entry(prover_id.clone())                                            
                                            .and_modify(|prover_proofs| {
                                                groth16_already_extracted = true;                                                    
                                                match prover_proofs
                                                    .iter()
                                                    .find(|p: &&recursion::Proof| p.blob_hash == blob_hash) {
                                                        None => {
                                                            prover_proofs.push(
                                                                recursion::Proof {                                                            
                                                                    blob: new_proof.blob.clone(),
                                                                    blob_hash: blob_hash,
                                                                    spent: false,
                                                                }
                                                            );
                                                        },

                                                        Some(_) => {
                                                            // duplicate proof
                                                            proof_should_be_logged = false;
                                                        }
                                                    }
                                            })                                        
                                            .or_insert_with(|| {
                                                vec![
                                                    recursion::Proof {
                                                        blob_hash: blob_hash,
                                                        blob: new_proof.blob.clone(),
                                                        spent: false,
                                                    }
                                                ]
                                            });
                                        // db
                                        if true == proof_should_be_logged {
                                            db_insert_futures.push(
                                                col_groth16.insert_one(
                                                    db::Groth16 {
                                                        job_id: db_job_oid.clone(),                                                        
                                                        proof: db::Proof {
                                                            prover: prover_id.clone(),
                                                            blob: new_proof.blob.clone(),
                                                            hash: blob_hash,
                                                        }
                                                    }
                                                )
                                                .into_future()
                                            );
                                        }
                                        if false == groth16_already_extracted {                                            
                                            // verify the final join proof                                                
                                            info!("Let's verify the Groth16 proof");

                                        }
                                    },
                                };
                            }                                                    
                        },
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },

            },
        }
    }
}

fn i_need_update(
    gossipsub: &mut gossipsub::Behaviour,
    topic: &gossipsub::IdentTopic,
    job_id: &str,
) -> anyhow::Result<()> {
    if let Err(e) = gossipsub
    .publish(
        topic.clone(),
        bincode::serialize(&protocol::Need::UpdateMe(job_id.to_string()))?
    ) {                
        eprintln!("[warn] `status update` gossip failed, error: {e:?}");
    }        
    Ok(())
}

fn i_need_compute(
    gossipsub: &mut gossipsub::Behaviour,
    topic: &gossipsub::IdentTopic,
    job: &job::Job
) -> anyhow::Result<()> {
    let need_type = match job.recursion.stage {
        // request for prove
        Stage::Prove => protocol::NeedType::Prove,
        
        // request for join
        Stage::Join => protocol::NeedType::Join,

        // request for groth16/plonk
        Stage::Snark => protocol::NeedType::Groth16,

        _ => {
            return Ok(())
        },    
    };
    if let Err(err_msg) = gossipsub
        .publish(
            topic.clone(),
            bincode::serialize(&protocol::Need::Compute(0, need_type))?
        )
    {            
        warn!("Need compute gossip failed, error: `{err_msg:?}`");
    }
    Ok(())
}

fn get_home_dir() -> anyhow::Result<String> {
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
        "{}/.wholesum/client/jobs/{}",
        get_home_dir()?,
        generated_job_id
    );
    // create folders for residues
    for action in ["prove", "join", "groth16"] {
        fs::create_dir_all(
            format!("{working_dir}/{action}")
        )?;
    }
    if schema.prove.num_segments == 0 {
        eprintln!("[warn] Number of segments is 0.");
    }    
    let rec = recursion::Recursion::new(
        schema.prove.num_segments,
        &schema.prove.segment_path,
        &schema.prove.segment_filename_prefix,
    )?;
    let journal = fs::read(&schema.verification.journal_filepath)?;
    Ok(
        Job {
            id: generated_job_id.clone(),
            working_dir: working_dir,
            schema: schema,
            recursion: rec,
            journal: journal,
        }
    )
        
}

// async fn resume_job(
//     col_jobs_generic: &mongodb::Collection<bson::Document>,
//     col_segments: &mongodb::Collection<db::Segment>,
//     col_joins: &mongodb::Collection<db::Join>,
//     job_id: &str,
// ) -> anyhow::Result<(job::Job, Bson)> {
//     println!("[info] Resuming job `{job_id}`");
//     let doc = col_jobs_generic
//         .find_one(doc! {
//             "id": job_id.to_string()
//         })
//         .await?
//         .ok_or_else(|| anyhow::anyhow!("No such job in db."))?;        
//     let db_job_oid = 
//         doc.get("_id")
//         .ok_or_else(|| anyhow::anyhow!("`_id` is not available"))?
//         .clone();
//     println!("[info] Job is found, oid: `{db_job_oid:?}`");
//     let db_job: db::Job = bson::from_document(doc)?;
//     let working_dir = format!("{}/.wholesum/client/jobs/{}",
//         get_home_dir()?,
//         job_id
//     );
//     // create folders for residues
//     for action in ["prove", "join", "groth16"] {
//         fs::create_dir_all(
//             format!("{working_dir}/{action}")
//         )?;
//     }    
//     let mut job = Job {
//         id: job_id.to_string(),            
//         working_dir: working_dir.clone(),
//         schema: job::Schema { 
//             prove: job:: ProveConfig {
//                 po2: db_job.po2,
//                 num_segments: db_job.num_segments,
//                 segments_cid: db_job.segments_cid.clone(),
//             },
//             verification: job::VerificationConfig {       
//                 journal_file_path: String::from(""),
//                 image_id: db_job.verification.image_id,
//             },
//         },

//         recursion: recursion::Recursion::new(db_job.num_segments)
//     };
//     // prove stage(segments)
//     let db_proofs = retrieve_segments_from_db(
//         col_segments,
//         &db_job_oid,
//     ).await?;
//     if true == db_proofs.is_empty() {
//         println!("[warn] No proved segments to sync with.");
//         return Ok(
//             (
//                 job,
//                 db_job_oid
//             )
//         )
//     }
//     println!("[info] Number of proved segments found: `{}`", db_proofs.len());
//     for seg_id in db_proofs.keys() {
//         job.recursion.prove_and_lift.progress_map.set(*seg_id as usize, true);
//     }
//     job.recursion.prove_and_lift.proofs = db_proofs;
//     println!("[info] Prove stage is in sync with DB.");
//     if true == job.recursion.prove_and_lift.is_finished() {
//         println!("[info] Prove stage is finished.");
//         if true == job.recursion.begin_join_stage() {
//             println!("[info] Attempting to verify the final join proof...");
//             //@ todo: verify join proof
//             return Ok(
//                 (
//                     job,
//                     db_job_oid
//                 )
//             );
//         }
//     } else {
//         eprintln!("[warn] Prove stage is not finished yet so no need to retrieve join data from DB.");
//         return Ok(
//             (
//                 job,
//                 db_job_oid
//             )
//         );
//     }
//     // join
//     let join_btree = retrieve_joins_from_db(
//         col_joins,
//         &db_job_oid,
//     ).await?;
//     if join_btree.len() == 0 {
//         eprintln!("[warn] No joins to sync with.");
//         return Ok(
//             (
//                 job,
//                 db_job_oid
//             )
//         )
//     }
//     let max_allowed_join_rounds = job.schema.prove.num_segments.ilog2() + 1;
//     let available_join_rounds_on_db = join_btree.keys().last().unwrap();
//     if *available_join_rounds_on_db > max_allowed_join_rounds {
//         eprintln!("[warn] Too many join rounds. Max would be `{}`, but `{}` are available.",
//             max_allowed_join_rounds,
//             available_join_rounds_on_db
//         );
//         return Ok(
//             (
//                 job,
//                 db_job_oid
//             )
//         )
//     }
//     for (round_number, db_joins) in join_btree.into_iter() {
//         println!("[info] Checking join round {round_number}...");
//         let round = job.recursion.join_rounds.last_mut().unwrap();
//         for db_join in db_joins.iter() {
//             if let Some(pair_index) = round
//                 .pairs
//                 .iter()
//                 .position(|p| 
//                     p.0 == db_join.left_input_proof &&
//                     p.1 == db_join.right_input_proof
//                 )
//             {
//                 if true == round
//                     .progress_map
//                     .get(pair_index)
//                     .unwrap()
//                 {
//                     continue;
//                 }
//                 round.progress_map.set(pair_index, true);                
//                 round.proofs.entry(pair_index)
//                 .and_modify(|proofs| {
//                     match proofs
//                     .iter()
//                     .find(|p: &&recursion::Proof| p.cid == db_join.proof.cid) {
//                         None => {
//                             proofs.push(
//                                 recursion::Proof {
//                                     cid: db_join.proof.cid.clone(),
//                                     prover: db_join.proof.prover.clone(),
//                                     spent: false,
//                                 }
//                             )
//                         },

//                         Some(_) => ()
//                     }
//                 })
//                 .or_insert_with(|| {
//                     vec![
//                         recursion::Proof {
//                             cid: db_join.proof.cid.clone(),
//                             prover: db_join.proof.prover.clone(),
//                             spent: false,
//                         }
//                     ]
//                 });

//             } else {
//                 eprintln!("[warn] `({}-{})` is not found in the pairs. Sync may be incomplete.",
//                     db_join.left_input_proof,
//                     db_join.right_input_proof
//                 );              
//                 continue;
//             }
//         }
//         println!("[info] Sync is complete for round {}.", round.number);
//         if true == round.progress_map.all() {
//             if true == job.recursion.begin_next_join_round() {                
//             }
//         }
//     }

//     Ok(
//         (
//             job,
//             db_job_oid
//         )
//     )    
// }

// async fn retrieve_segments_from_db(
//     col_segments: &mongodb::Collection<db::Segment>,
//     db_job_oid: &Bson,
// ) -> anyhow::Result<BTreeMap<u32, Vec<recursion::Proof>>> {
//     // retrieve all proved segments
//     println!("[info] Retrieving segments from the db...");
//     let mut cursor = col_segments.find(
//         doc! {
//             "job_id": db_job_oid
//         }
//     )
//     .await?;
//     let mut proofs = BTreeMap::<u32, Vec<recursion::Proof>>::new();
//     while let Some(db_segment) = cursor.try_next().await? {        
//         proofs.entry(db_segment.id)
//         .and_modify(|v| {
//             v.push(
//                 recursion::Proof {
//                     cid: db_segment.proof.cid.clone(),
//                     prover: db_segment.proof.prover.clone(),
//                     spent: false
//                 }
//             );
//         })
//         .or_insert_with(|| {
//             vec![
//                 recursion::Proof {
//                     cid: db_segment.proof.cid,
//                     prover: db_segment.proof.prover,
//                     spent: false
//                 }
//             ]
//         });
//     }
//     Ok(proofs)
// }

// async fn retrieve_joins_from_db(
//     col_joins: &mongodb::Collection<db::Join>,
//     db_job_oid: &Bson,
// ) -> anyhow::Result<BTreeMap<u32, Vec<db::Join>>> {
//     println!("[info] Retrieving joins from the db...");
//     let mut cursor = col_joins.find(
//         doc! {
//             "job_id": db_job_oid
//         }
//     )
//     // .projection(
//     //     doc! {
//     //         "verified_blob": 0
//     //     }
//     // )
//     .sort(
//         doc! {
//             "round": 1 
//         }
//     )
//     .await?;
//     let mut join_btree = BTreeMap::<u32, Vec<db::Join>>::new();
//     while let Some(db_join) = cursor.try_next().await? {  
//         join_btree.entry(db_join.round as u32)
//             .and_modify(|j| j.push(db_join.clone()))
//             .or_insert(vec![db_join]);
//     }
//     Ok(join_btree)
// }

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

// verify the final join proof
async fn verify_join_proof(
    proof_blob: Vec<u8>,
    journal: Vec<u8>,
    image_id: String,
) -> anyhow::Result<()> {
    let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(&proof_blob)?;
    let journal: Journal = bincode::deserialize(&journal)?;
    let proof = Receipt::new(
        InnerReceipt::Succinct(sr),
        journal.bytes
    );        
    let now = Instant::now();
    proof.verify(
        <[u8; 32]>::from_hex(&image_id)?
    )?;
    let verification_dur = now.elapsed().as_millis();
    info!("Verification of join proof took `{verification_dur} msecs`.");
    Ok(())
}