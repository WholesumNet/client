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
};
use std::{
    fs,
    time::{
        Instant, 
        Duration,
    },
    future::IntoFuture,
};
use rand::Rng;
use bincode;
// use chrono::{Utc};

use toml;
use tracing_subscriber::EnvFilter;
use anyhow;
use reqwest;

use clap::{
    Parser, Subcommand
};

use comms::{
    p2p::{MyBehaviourEvent},
    protocol
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
    },
    options::{
        ClientOptions,
        ServerApi,
        ServerApiVersion
    },
};

use dstorage::lighthouse;

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
            println!("[warn] No keys were supplied, so one has been generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    println!("[info] Peer id: `{:?}`", PeerId::from_public_key(&local_key.public()));  
    
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
    
    // futures for mongodb progress saving 
    let mut db_insert_futures = FuturesUnordered::new();
    let mut db_update_futures = FuturesUnordered::new();

    // the job
    let db_job_oid;
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
                &db_client.database("wholesum_client").collection("jobs"),
                &col_segments,
                &col_joins,
                job_id
            )
            .await?;
            db_job_oid = joid;
            synced_job
        },

        _ => {
            panic!("[warn] Missing command, not sure what you meant.");
        },
    };     

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
            eprintln!("[warn] Failed to bootstrap Kademlia: `{:?}`", e);

        } else {
            println!("[info] Self-bootstraping is initiated.");
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
            
            // succinct receipt's verification has been finished
            // v_res = succinct_receipt_verification_futures.select_next_some() => {                
            //     if let Err(failed) = v_res {
            //         let failed: VerificationError = failed;
            //         eprintln!("[warn] Proved receipt(`{}`)'s verification has failed. error: `{}`", 
            //             failed.item_id,
            //             failed.err_msg
            //         );
            //     }
            // },

            v_res = join_proof_verification_futures.select_next_some() => {
                let receipt: Receipt = match v_res {
                    Err(failed) => {
                        //@ which segment/join/... to blame? 
                        eprintln!("[warn] Join proof verification has been failed: `{:#?}`", failed);
                        continue;                        
                    },

                    Ok(r) => r
                };
                println!("[info] Bingo! the final join proof has been verified, aggregation is complete.");
                job.recursion.stage = Stage::Groth16;
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
                    Err(e) => eprintln!("[warn] Failed to record progress(insert) to the db: `{:#?}`", e),

                    Ok(oid) => println!("[info] Progress recorded(insert) to the db: `{:?}`", oid)
                }                
            },

            res = db_update_futures.select_next_some() => {
                match res {
                    Err(e) => eprintln!("[warn] Failed to record progress(update) to the db: `{:#?}`", e), 

                    Ok(oid) => println!("[info] Progress recorded(update) to the db: `{:?}`", oid)
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
                        // channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        protocol::Request::ProofIsReady(new_proofs) => {
                            for new_proof in new_proofs {
                                if job.id != new_proof.job_id {
                                    println!("[warn] Status update for an unknown job `{new_proof:?}`, ignored.");
                                    continue;
                                }
                                match new_proof.proof_type {
                                    protocol::ProofType::ProveAndLift(seg_id) => {
                                        if job.recursion.stage != Stage::Prove {                             
                                            continue;
                                        }
                                        if seg_id >= job.schema.prove.num_segments {
                                            eprintln!(
                                                "[warn] Invalid segment id `{}`, valid range: `0 - {}`",
                                                seg_id,
                                                job.schema.prove.num_segments
                                            );
                                            continue;
                                        }
                                        if true == job.recursion.prove_and_lift
                                            .progress_map
                                            .get(seg_id as usize)
                                            .unwrap()
                                        {
                                            continue;
                                        }
                                        //@ need to validate received proof somehow
                                        job.recursion.prove_and_lift.progress_map.set(seg_id as usize, true);
                                        println!(
                                            "[info] `Segment {}` is proved, progress_map: {:?}",
                                            seg_id,
                                            job.recursion.prove_and_lift.progress_map
                                        );
                                        job.recursion.prove_and_lift
                                        .proofs
                                        .entry(seg_id)
                                        .and_modify(|proofs| {
                                            match proofs
                                            .iter()
                                            .find(|p: &&recursion::Proof| p.cid == new_proof.cid) {
                                                None => {
                                                    proofs.push(
                                                        recursion::Proof {
                                                            cid: new_proof.cid.clone(),
                                                            prover: prover_peer_id.to_string(),
                                                            spent: false,
                                                        }
                                                    )
                                                },

                                                Some(_) => ()
                                            }
                                        })
                                        .or_insert_with(|| {
                                            vec![
                                                recursion::Proof {
                                                    cid: new_proof.cid.clone(),
                                                    prover: prover_peer_id.to_string(),
                                                    spent: false,
                                                }
                                            ]
                                        });
                                    
                                        if true == job.recursion.prove_and_lift.is_finished() {
                                            println!("[info] Prove stage is finished.");
                                            if true == job.recursion.begin_join_stage() {
                                                // verify stark proof
                                                println!("[info] Attempting to verify the stark proof");
                                            }
                                        }
                                        // db
                                        db_insert_futures.push(
                                            col_segments.insert_one(
                                                db::Segment {
                                                    id: seg_id,
                                                    job_id: db_job_oid.clone(),
                                                    proof: db::Proof {
                                                        cid: new_proof.cid.clone(),
                                                        prover: prover_peer_id.to_string()
                                                    }
                                                }
                                            )
                                            .into_future()
                                        );                                               
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
                                    },

                                    protocol::ProofType::Join(left, right) => {
                                        if job.recursion.stage != Stage::Join { 
                                            continue;
                                        }
                                        let round = job.recursion.join_rounds.last_mut().unwrap();
                                        let round_number = round.number;
                                        //@ not efficient to do 100k comparisons.
                                        let index = match round.pairs.iter().position(|p| p.0 == left && p.1 == right) {
                                            Some(i) => i,
                                            None => {
                                                eprintln!("[warn] Unknown join pair, left: `{left:?}`, right: `{right:?}`");
                                                continue;
                                            }
                                        };
                                        round.progress_map.set(index, true);
                                        println!(
                                            "[info] `Pair {:?}` is joined, progress_map: {:?}",
                                            (&left, &right),
                                            round.progress_map
                                        );
                                        round.proofs.entry(index)
                                        .and_modify(|proofs| {
                                            match proofs
                                            .iter()
                                            .find(|p: &&recursion::Proof| p.cid == new_proof.cid) {
                                                None => {
                                                    proofs.push(
                                                        recursion::Proof {
                                                            cid: new_proof.cid.clone(),
                                                            prover: prover_peer_id.to_string(),
                                                            spent: false,
                                                        }
                                                    )
                                                },

                                                Some(_) => ()
                                            }
                                        })
                                        .or_insert_with(|| {
                                            vec![
                                                recursion::Proof {
                                                    cid: new_proof.cid.clone(),
                                                    prover: prover_peer_id.to_string(),
                                                    spent: false,
                                                }
                                            ]
                                        });
                                        // db
                                        //@ watch for duplicates
                                        db_insert_futures.push(
                                            col_joins.insert_one(
                                                db::Join {
                                                    job_id: db_job_oid.clone(),
                                                    round: round_number,
                                                    left_input_proof: left.clone(),
                                                    right_input_proof: right.clone(),
                                                    proof: db::Proof {
                                                        cid: new_proof.cid.clone(),
                                                        prover: prover_peer_id.to_string()
                                                    }
                                                }
                                            )
                                            .into_future()
                                        );                                                
                                        if true == round.progress_map.all() {
                                            if true == job.recursion.begin_next_join_round() {
                                                // verify the final join proof                                                
                                                println!("[info] Let's verify the final join proof.");
                                                let join_proof_cid = job.recursion.join_proof.clone().unwrap();
                                                let join_proof_filepath = format!("{}/join/proof", job.working_dir);
                                                if let Err(e) = lighthouse::download_file(
                                                    &ds_client,
                                                    &join_proof_cid,
                                                    join_proof_filepath.clone()
                                                ).await {
                                                    eprintln!("[warn] Proof download failed: `{e:?}`");
                                                } else {                                                
                                                    join_proof_verification_futures.push(
                                                        verify_join_proof(
                                                            join_proof_filepath.clone(),
                                                            job.schema.verification.journal_file_path.clone(),
                                                            job.schema.verification.image_id.clone(),
                                                        )
                                                    );                                                    
                                                }
                                            }                             
                                        }                                        
                                    },

                                    protocol::ProofType::Groth16 => {
                                        // if job.recursion.stage != Stage::Groth16 { 
                                        //     continue;
                                        // }
                                        // job.recursion.snark = Some(receipt_cid.to_string());
                                        // job.recursion.stage = Stage::Snark;
                                        // println!("[info] a SNARK proof has been extracted for the job `{job_id}`: {receipt_cid} ");
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
    let mut rng = rand::thread_rng();
    let nonce: u8 = rng.gen();
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
    let compute_job = match job.recursion.stage {
        Stage::Prove => {
            println!(
                "gossip progress_map: {:?}",
                job.recursion.prove_and_lift.progress_map
            );
            protocol::ComputeJob {
                job_id: job.id.clone(),
                budget: 0,
                job_type: protocol::JobType::ProveAndLift(
                    protocol::ProveAndLiftDetails {
                        segments_base_cid: job.schema.prove.segments_cid.clone(),
                        segment_prefix_str: String::from("segment-"),
                        po2: job.schema.prove.po2 as u8,
                        num_segments: job.schema.prove.num_segments,
                        progress_map: job.recursion.prove_and_lift.progress_map.to_bytes(),
                    }
                )
            }
        },

        Stage::Join => {
            let round = job.recursion.join_rounds.last().unwrap();
            protocol::ComputeJob {
                job_id: job.id.clone(),
                budget: 0,
                job_type: protocol::JobType::Join(
                    protocol::JoinDetails {
                        pairs: round.pairs.clone(),
                        progress_map: round.progress_map.to_bytes()
                    }
                )
            }
        },

        _ => {
            return Ok(())
        },    
    };
    if let Err(e) = gossipsub
    .publish(
        topic.clone(),
        bincode::serialize(&protocol::Need::Compute(compute_job))?
    ) {            
        eprintln!("[warn] Need compute gossip failed, error: `{e:?}`");
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
        schema.prove.num_segments
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
    let working_dir = format!("{}/.wholesum/client/jobs/{}",
        get_home_dir()?,
        job_id
    );
    // create folders for residues
    for action in ["prove", "join", "groth16"] {
        fs::create_dir_all(
            format!("{working_dir}/{action}")
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

        recursion: recursion::Recursion::new(db_job.num_segments)
    };
    // prove stage(segments)
    let segments = retrieve_segments_from_db(
        col_segments,
        &db_job_oid,
    ).await?;
    if segments.len() == 0 {
        println!("[warn] No segments to sync with.");
        return Ok(
            (
                job,
                db_job_oid
            )
        )
    }
    for seg_id in segments.keys() {
        job.recursion.prove_and_lift.progress_map.set(*seg_id as usize, true);
    }
    if true == job.recursion.prove_and_lift.is_finished() {
        // job.recursion.begin_join();
        // if true == job.recursion.join.begin_next_round() {
        //     println!("[info] Join is already finished.");
        //     return Ok(
        //         (
        //             job,
        //             db_job_oid
        //         )
        //     )            
        // }
    }
    // join
    // let join_btree = retrieve_verified_joins_from_db(
    //     col_joins,
    //     &verified_segments,
    //     &db_job_oid,
    //     &job.working_dir
    // ).await?;
    // if join_btree.len() == 0 {
    //     eprintln!("[warn] No verified joins to sync with.");
    //     return Ok(
    //         (
    //             job,
    //             db_job_oid
    //         )
    //     )
    // }
    // let db_cur_round = join_btree.keys().last().unwrap();
    // // println!("db round: {db_cur_round}");    
    // // println!("db rounds: {:?}", join_btree.keys());
    // // emulate a full join and sync with    
    // loop {
    //     let round_joins = match join_btree
    //         .get(&(job.recursion.join.round as u32))
    //     {
    //         Some(v) => v,
    //         None => {
    //             println!("no more joins is possible.");
    //             break;
    //         }
    //     };            
    //     // println!("r: {}, len: {}", job.recursion.join.round, round_joins.len());
    //     for db_join in round_joins {
    //         let joined_pair_index = job
    //             .recursion
    //             .join
    //             .pairs
    //             .iter()
    //             .position(|p| 
    //                 p.left == db_join.verified_blob.input_cid_left &&
    //                 p.right == db_join.verified_blob.input_cid_right
    //             );
    //         if true == joined_pair_index.is_none() {
    //             eprintln!("[warn] Missing join pair, sync may be incomplete.");
    //             continue;
    //         }
    //         // println!("removing pair {}-{}", db_join.verified_blob.input_cid_left,db_join.verified_blob.input_cid_right);
    //         let joined_pair = job.recursion.join.pairs.swap_remove(joined_pair_index.unwrap());
    //         job.recursion.join.joined.insert(
    //             joined_pair.position,
    //             db_join.verified_blob.cid.clone()
    //         );            
    //     }
    //     if true == job.recursion.join.begin_next_round() ||
    //        job.recursion.join.round as u32 == *db_cur_round
    //     {            
    //         break;
    //     }
    // }
    // println!("{:#?}", job.recursion.join);
    // println!("[info] Join is synced with the db.");
    Ok(
        (
            job,
            db_job_oid
        )
    )    
}

async fn retrieve_segments_from_db(
    col_segments: &mongodb::Collection<db::Segment>,
    db_job_oid: &Bson,
) -> anyhow::Result<HashMap<u32, HashMap<String, String>>> {
    // retrieve all proved segments
    println!("[info] Retrieving segments from the db...");
    let mut cursor = col_segments.find(
        doc! {
            "job_id": db_job_oid
        }
    )
    .await?;
    let mut segments = HashMap::<u32, HashMap<String, String>>::new();
    while let Some(db_segment) = cursor.try_next().await? {        
        segments.entry(db_segment.id)
        .and_modify(|v| {
            v.insert(db_segment.proof.cid.clone(), db_segment.proof.prover.clone());
        })
        .or_insert_with(|| {
            HashMap::from([
                (db_segment.proof.cid, db_segment.proof.prover)
            ])
        });
    }
    Ok(segments)
}

// async fn retrieve_verified_joins_from_db(
//     col_joins: &mongodb::Collection<db::Join>,
//     verified_segments: &Vec<recursion::Segment>,
//     db_job_oid: &Bson,
//     working_dir: &str,
// ) -> anyhow::Result<BTreeMap<u32, Vec<db::Join>>> {
//     println!("[info] Retrieving verified joins from the db...");
//     // println!("{db_job_oid:?}");
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
//         // println!("{}, {}", db_join.round, db_join.verified_blob.cid);      
//         join_btree.entry(db_join.round as u32)
//             .and_modify(|j| j.push(db_join.clone()))
//             .or_insert(vec![db_join]);
//     }
//     // emulate
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

// verify proved and lifted receipt 
async fn verify_succinct_receipt(
    ds_client: &reqwest::Client,
    residue_path: String,
    prover_id: String,
    receipt_cid: String,
    item_id: String
) -> Result<VerificationResult, VerificationError> {    
    let blob_file_path = format!("{residue_path}/{receipt_cid}");
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
    let now = Instant::now(); 
    match succinct_receipt.verify_integrity() {
        Err(e) => Err(VerificationError {
            receipt_cid: receipt_cid.clone(),
            item_id: item_id.clone(),
            err_msg: format!("Receipt verification error: `{}`",  e.to_string())
        }),

        Ok(_) => {
            let verification_dur = now.elapsed().as_millis();
            println!("verification took `{verification_dur} msecs`.");
            Ok(VerificationResult {
                prover_id: prover_id,
                receipt_cid: receipt_cid,
                item_id: item_id,
                receipt_file_path: blob_file_path,
                receipt_blob: blob,
            })
        }
    }
}

// verify the final join proof
async fn verify_join_proof(
    succinct_receipt_file_path: String,
    journal_file_path: String,
    image_id: String,
) -> anyhow::Result<Receipt> {
    let sr: SuccinctReceipt<ReceiptClaim> = bincode::deserialize(
        &std::fs::read(succinct_receipt_file_path)?
    )?;
    let journal: Journal = bincode::deserialize(
        &std::fs::read(journal_file_path)?
    )?;
    let stark_receipt = Receipt::new(
        InnerReceipt::Succinct(sr),
        journal.bytes
    );        
    let now = Instant::now();
    stark_receipt.verify(
        <[u8; 32]>::from_hex(&image_id)?
    )?;
    let verification_dur = now.elapsed().as_millis();
    println!("[info] STARK verification took `{verification_dur} msecs`.");
    Ok(stark_receipt)
}