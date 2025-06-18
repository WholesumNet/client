#![doc = include_str!("../README.md")]
use futures::{
    select,
    stream::{
        // FuturesUnordered,
        StreamExt,
    },
    channel::{mpsc},
    prelude::sink::SinkExt
};
use tokio::{
    task,
    time::interval
};
use tokio_stream::wrappers::IntervalStream;

use libp2p::{
    gossipsub, mdns, request_response,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};
use std::{
    fs,
    time::{
        // Instant, 
        Duration,
    },
    // future::IntoFuture,
};
use bincode;

use toml;
use anyhow;
// use reqwest;

use clap::{
    Parser, Subcommand
};

use uuid::Uuid;
use env_logger::Env;
use log::{info, warn};
use mongodb::{
    // bson,
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
use peyk::{
    p2p::{MyBehaviourEvent},
    protocol,
    protocol::{
        ProofKind
    }
};

mod job;
use job::Job;

mod pipeline;

mod db;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Client CLI for Wholesum")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Wholesum is a p2p prover network and \
                   this program is a CLI for client nodes.",
          long_about = None)
]
struct Cli {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .init();
    let cli = Cli::parse();
    info!("<-> Client agent for Wholesum network <->");
    info!("Operating mode: `{}` network",
        if false == cli.dev {"global"} else {"local(development)"}
    );

    // setup mongodb
    // let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // setup redis
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let redis_con = redis_client.get_multiplexed_async_connection().await?;    
    let mut zeth_segment_stream = subscribe_to_zeth_segment_stream(redis_con.clone()).await;
    let mut zeth_keccak_stream = subscribe_to_zeth_keccak_stream(redis_con.clone()).await;
    let mut zeth_zkr_stream = subscribe_to_zeth_zkr_stream(redis_con.clone()).await;


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
            warn!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    let my_peer_id = PeerId::from_public_key(&local_key.public());
    info!("My PeerId: `{:?}`", my_peer_id); 
    // futures for local verification of succinct proofs
    // let mut succinct_proof_verification_futures = FuturesUnordered::new();
    // let mut join_proof_verification_futures = FuturesUnordered::new();

    // let col_jobs = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Job>("jobs");
    // let col_segments = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Segment>("segments");
    // let col_joins = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Join>("joins");
    // let col_groth16 = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Groth16>("groth16");
    
    // futures for mongodb progress saving 
    // let mut db_insert_futures = FuturesUnordered::new();
    // let mut db_update_futures = FuturesUnordered::new();

    // the job
    let mut job = match &cli.job {
        Some(Commands::New{ job_file }) => {
            let job = new_job(job_file)?;            
            // let oid = col_jobs.insert_one(
            //     db::Job {
            //         id: job.id.clone(),
            //         verification: db::Verification {
            //             image_id: job.schema.image_id.clone(),
            //         },
            //         snark_proof: None,
            //     }
            // )
            // .await?
            // .inserted_id;
            // (job, oid)
            job
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
    // info!("Job's progress will be recorded to the DB with Id: `{db_job_oid:?}`");

    // swarm 
    let mut swarm = peyk::p2p::setup_swarm(&local_key).await?;
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
    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(5 * 60))
    )
    .fuse();
    // used for posting job needs
    let mut timer_post_job = IntervalStream::new(
        interval(Duration::from_secs(5))
    )
    .fuse();
    use rand::prelude::*;
    let mut rng = rand::rng();
    // use for retrieveing the stark proof
    let mut timer_retrieve_agg_proof = IntervalStream::new(
        interval(Duration::from_secs(5))
    )
    .fuse();   


    loop {
        select! {
            // try to discover new peers
            _i = timer_peer_discovery.select_next_some() => {
                if true == cli.dev {
                    continue;
                }
                let random_peer_id = PeerId::random();
                info!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);
            },

            // post need prove
            _i = timer_post_job.select_next_some() => {
                let need = match job.pipeline.stage {
                    // request for groth16 proving
                    pipeline::Stage::Groth16 => {
                        let nonce = rng.random::<u32>();
                        protocol::NeedKind::Groth16(nonce)
                    },

                    // request for other kinds of proving
                    _ => {
                        // let _outstanding_jobs = (
                        //     job.pipeline.num_outstanding_aggregate_items() +
                        //     job.pipeline.num_outstanding_resolve_items()
                        // ) as u32;
                        let nonce = rng.random::<u32>();
                        protocol::NeedKind::Prove(nonce)
                    },
                };
                if let Err(_e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(
                        topic.clone(),
                        bincode::serialize(&need)?
                    )
                {            
                    // warn!("Need compute gossip failed, error: `{err_msg:?}`");
                }                
            },

            //@ ask for the stark proof
            _i = timer_retrieve_agg_proof.select_next_some() => {
                if job.pipeline.stage == pipeline::Stage::Resolve &&
                   job.pipeline.agg_proof.is_none()
                {
                    let (prover, hash)  = job.pipeline.agg_proof_token();
                    let peer_id = PeerId::from_bytes(&prover).unwrap();
                    let _req_id = swarm
                        .behaviour_mut()
                        .req_resp
                        .send_request(
                            &peer_id,
                            protocol::Request::TransferBlob(hash),
                        );
                    info!(
                        "Requested transfer of the aggregated proof blob `{}` from `{}`.",
                        hash,
                        peer_id
                    );
                }
            },

            // zeth segments are ready
            value = zeth_segment_stream.select_next_some() => {
                for stream in value.as_sequence().unwrap() {
                    let data = stream.as_sequence().unwrap();
                    // let _stream_name = &data[0];
                    let contents = data[1].as_sequence().unwrap();
                    for object in contents { 
                        let segments = object.as_sequence().unwrap();                        
                        // object[1] is valkey generated id
                        let segment_data  = segments[1].as_sequence().unwrap();
                        let id = if let redis::Value::BulkString(bs) =  &segment_data[0] {
                            let str_id = String::from_utf8_lossy(&bs).to_string();
                            match u32::from_str_radix(&str_id, 10) {
                                Ok(id) => id,

                                Err(err_msg) => {
                                    if str_id.eq_ignore_ascii_case("<DONE>") {
                                        info!("All segments have been received from the ValKey server.");
                                        job.pipeline.stop_segment_feeding();
                                    } else {
                                        warn!("Invalid segment id `{str_id}` from the ValKey server: `{err_msg}");
                                    }
                                    continue
                                }
                            }
                        } else {
                            continue
                        };
                        let blob = if let redis::Value::BulkString(blob) = &segment_data[1] {
                            blob.to_owned()
                        } else {
                            continue
                        };
                        job.pipeline.feed_segment(id as usize, blob);                        
                    }
                }                
            },

            // zeth keccak items are ready
            value = zeth_keccak_stream.select_next_some() => {
                for stream in value.as_sequence().unwrap() {                    
                    let contents = stream.as_sequence().unwrap();
                    // let _stream_name = &contents[0];
                    let objects = &contents[1]
                        .as_sequence()
                        .unwrap()
                        [0]
                        .as_sequence()
                        .unwrap();
                    //let _last_id = &objects[0];
                    let assumptions = objects[1].as_sequence().unwrap();
                    let num_assumptions = objects[1].as_sequence().unwrap().len() / 2;
                    for i in 0..num_assumptions {
                        if let redis::Value::BulkString(bs) = &assumptions[i * 2] {                            
                            let cd_str = String::from_utf8_lossy(&bs).to_string();
                            if cd_str.eq_ignore_ascii_case("<DONE>") {
                                info!("All Keccak assumptions have been received from the ValKey server.");
                                continue
                            }
                        } else {
                            warn!("Invalid Keccak assumption from the ValKey server.");
                            continue
                        };
                        let blob = if let redis::Value::BulkString(blob) = &assumptions[i * 2 + 1] {
                            blob
                        } else {
                            continue
                        };
                        job.pipeline.feed_assumption(&blob);
                    }                    
                }                
            },

            value = zeth_zkr_stream.select_next_some() => {
                for stream in value.as_sequence().unwrap() {                    
                    let contents = stream.as_sequence().unwrap();
                    // let _stream_name = &contents[0];
                    let objects = &contents[1]
                        .as_sequence()
                        .unwrap()
                        [0]
                        .as_sequence()
                        .unwrap();
                    //let _last_id = &objects[0];
                    let assumptions = objects[1].as_sequence().unwrap();
                    let num_assumptions = objects[1].as_sequence().unwrap().len() / 2;
                    for i in 0..num_assumptions {
                        if let redis::Value::BulkString(bs) = &assumptions[i * 2] {                            
                            let cd_str = String::from_utf8_lossy(&bs).to_string();
                            if cd_str.eq_ignore_ascii_case("<DONE>") {
                                info!("All Zkr assumptions have been received from the ValKey server.");
                                continue
                            }
                        } else {
                            warn!("Invalid Zkr assumption from the ValKey server.");
                            continue
                        };
                        let blob = if let redis::Value::BulkString(blob) = &assumptions[i * 2 + 1] {
                            blob
                        } else {
                            continue
                        };
                        job.pipeline.feed_assumption(&blob);
                    }                                                
                }                
            },

            // res = db_insert_futures.select_next_some() => {
            //     if let Err(err_msg) = res {
            //         warn!("DB insert was failed: `{err_msg:?}`");
            //     }                
            // },

            // res = db_update_futures.select_next_some() => {
            //     if let Err(err_msg) = res {
            //         warn!("DB insert was failed: `{err_msg:?}`");
            //     } 
            // },

            // libp2p events
            event = swarm.select_next_some() => match event {
                
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Local node is listening on {address}");
                },

                // mdns events
                SwarmEvent::Behaviour(
                    MyBehaviourEvent::Mdns(
                        mdns::Event::Discovered(list)
                    )
                ) => {
                    for (peer_id, _multiaddr) in list {
                        info!("mDNS discovered a new peer: {peer_id}");
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
                        info!("mDNS discovered peer has expired: {peer_id}");
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
                    info!("received gossip message: {:#?}", message);                    
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
                        protocol::Request::WouldProve => {
                            let prover_id = prover_peer_id.to_bytes();
                            match job.pipeline.stage {
                                pipeline::Stage::Groth16 => {                                    
                                    let (batch_id, assignments) = match job
                                        .pipeline
                                        .assign_groth16_batch(&prover_id)
                                    {
                                        Some((i, a)) => (i, a),

                                        None => {                                    
                                            warn!("No Groth16 jobs for `{prover_peer_id:?}` at this time.");
                                            continue
                                        }
                                    };
                                    let compute_job = protocol::ComputeJob {
                                        id: job.id,
                                        kind: protocol::JobKind::Groth16(
                                            protocol::Groth16Details {
                                                batch: assignments
                                                    .into_iter()
                                                    .map(|ass| {
                                                        match ass {
                                                            pipeline::Input::Blob(blob) => 
                                                                (
                                                                    batch_id,
                                                                    protocol::InputBlob::Blob(blob)
                                                                ),

                                                            pipeline::Input::Token(prover, proof) => 
                                                                (
                                                                    batch_id,
                                                                    protocol::InputBlob::Token(
                                                                        proof.hash,
                                                                        prover,
                                                                    )
                                                                )
                                                        }
                                                    })
                                                .collect()
                                            }
                                        )
                                    };
                                    if let Err(_e) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(
                                                channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send the Groth16 job for proving.");
                                    } else {
                                        job.pipeline.confirm_groth16_assignment(&prover_id, batch_id);
                                        info!("Sent the Groth16 job to `{prover_peer_id}`.");
                                    }
                                },

                                pipeline::Stage::Resolve => {
                                    //@ ask for agg proof blob here too?
                                    // if job.pipeline.num_outstanding_resolve_items() == 0 {
                                    //     continue
                                    // }                                    
                                    let (batch_id, assignments) = match job
                                        .pipeline
                                        .assign_assumption_batch(&prover_id)
                                    {
                                        Some((i, a)) => (i, a),

                                        None => {                                    
                                            warn!("No assumption jobs for `{prover_peer_id:?}` at this time.");
                                            //@ send resolve jobs if possible
                                            continue
                                        }
                                    };
                                    let compute_job = protocol::ComputeJob {
                                        id: job.id,
                                        kind: protocol::JobKind::Assumption(
                                            protocol::AssumptionDetails {
                                                batch: assignments
                                                    .into_iter()
                                                    .map(|ass| {
                                                        match ass {
                                                            pipeline::Input::Blob(blob) => 
                                                                (
                                                                    batch_id,
                                                                    protocol::InputBlob::Blob(blob)
                                                                ),

                                                            pipeline::Input::Token(prover, proof) => 
                                                                (
                                                                    batch_id,
                                                                    protocol::InputBlob::Token(
                                                                        proof.hash,
                                                                        prover,
                                                                    )
                                                                )
                                                        }
                                                    })
                                                .collect()
                                            }
                                        )
                                    };
                                    if let Err(e) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(
                                                channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send the assumption job for proving: `{e:?}`");
                                    } else {
                                        job.pipeline.confirm_assumption_assignment(&prover_id, batch_id);
                                        info!("Sent the assumption job to `{prover_peer_id}`.");
                                    }
                                },

                                pipeline::Stage::Aggregate => {
                                    let (batch_id, assignments) = match job
                                        .pipeline
                                        .assign_agg_batch(&prover_id)
                                    {
                                        Some((i, a)) => (i, a),

                                        None => {                                    
                                            warn!("No aggregate jobs for `{prover_peer_id:?}` at this time.");
                                            //@ send resolve jobs if possible
                                            continue
                                        }
                                    };
                                    //@ unify segment and join into one job   
                                    let compute_job = protocol::ComputeJob {
                                        id: job.id,
                                        kind: protocol::JobKind::Aggregate(
                                            protocol::AggregateDetails {
                                                id: batch_id,
                                                blobs_are_segment: job.pipeline.num_agg_rounds() == 1,
                                                batch: assignments
                                                    .into_iter()
                                                    .map(|ass| {
                                                        match ass {
                                                            pipeline::Input::Blob(blob) => 
                                                                protocol::InputBlob::Blob(blob),

                                                            pipeline::Input::Token(prover, proof) => 
                                                                protocol::InputBlob::Token(
                                                                    proof.hash,
                                                                    prover,
                                                                )
                                                        }                                                        
                                                    })
                                                .collect()
                                            }
                                        )
                                    };
                                    if let Err(e) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(
                                                channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send the aggregate job for proving: `{e:?}`");
                                    } else {
                                        job.pipeline.confirm_agg_assignment(&prover_id, batch_id);
                                        info!("Sent the aggregate job to `{prover_peer_id}`.");
                                    }
                                }
                            };
                        },

                        // prover has finished its job
                        protocol::Request::ProofIsReady(token) => {
                            if job.id != token.job_id {
                                warn!("Ignored unknown proof token: `{token:?}`");
                                continue;
                            }

                            let prover_id = prover_peer_id.to_bytes();
                            match token.kind {
                                ProofKind::Assumption(batch_id, blob) => {
                                    info!(
                                        "Received assumption proof for batch index `{}` from `{}`",
                                        batch_id,
                                        prover_peer_id
                                    );
                                    job.pipeline.add_assumption_proof(
                                        batch_id,
                                        blob,
                                        prover_id
                                    );
                                },                                 

                                ProofKind::Aggregate(batch_id) => {
                                    info!(
                                        "Received agg proof for batch index `{}` from `{}`",
                                        batch_id,
                                        prover_peer_id
                                    );
                                    job.pipeline.add_agg_proof(
                                        batch_id,
                                        token.hash,
                                        prover_id
                                    );
                                    if job.pipeline.stage == pipeline::Stage::Resolve {
                                        let _req_id = swarm
                                            .behaviour_mut()
                                            .req_resp
                                            .send_request(
                                                &prover_peer_id,
                                                protocol::Request::TransferBlob(token.hash),
                                            );
                                        info!(
                                            "Requested transfer of the aggregated proof blob `{}` from `{}`.",
                                            token.hash,
                                            prover_peer_id
                                        );
                                    }
                                },                                    

                                ProofKind::Groth16(batch_id, blob) => {
                                    info!(
                                        "Received Groth16 proof for from `{}`",
                                        prover_peer_id,
                                    );
                                    job.pipeline.add_groth16_proof(batch_id, blob, prover_id);                              
                                },
                            };
                        },

                        protocol::Request::TransferBlob(_) => (),
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        protocol::Response::BlobIsReady(blob) => {
                            info!(
                                "Received the final aggregated proof from `{}`",
                                peer_id,
                            );
                            job.pipeline.add_final_agg_proof(
                                peer_id.to_bytes(),
                                blob
                            );
                        },

                        _ => {},
                    }
                },

                _ => {
                    // println!("{:#?}", event)
                },

            },
        }
    }
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

    let generated_job_id = Uuid::new_v4().as_u128();
    let working_dir = format!(
        "{}/.wholesum/client/jobs/{}",
        get_home_dir()?,
        generated_job_id.to_string()
    );
    // create folders for residues
    for action in ["prove", "join", "groth16"] {
        fs::create_dir_all(
            format!("{working_dir}/{action}")
        )?;
    }    
    let pipeline = pipeline::Pipeline::new(schema.image_id.as_bytes());
    Ok(
        Job {
            id: generated_job_id,
            working_dir: working_dir,
            schema: schema,
            pipeline: pipeline,
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

//         recursion: pipeline::pipeline::new(db_job.num_segments)
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
//         job.pipeline.prove_and_lift.progress_map.set(*seg_id as usize, true);
//     }
//     job.pipeline.prove_and_lift.proofs = db_proofs;
//     println!("[info] Prove stage is in sync with DB.");
//     if true == job.pipeline.prove_and_lift.is_finished() {
//         println!("[info] Prove stage is finished.");
//         if true == job.pipeline.begin_join_stage() {
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
//         let round = job.pipeline.join_rounds.last_mut().unwrap();
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
//                     .find(|p: &&pipeline::Proof| p.cid == db_join.proof.cid) {
//                         None => {
//                             proofs.push(
//                                 pipeline::Proof {
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
//                         pipeline::Proof {
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
//             if true == job.pipeline.begin_next_join_round() {                
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
// ) -> anyhow::Result<BTreeMap<u32, Vec<pipeline::Proof>>> {
//     // retrieve all proved segments
//     println!("[info] Retrieving segments from the db...");
//     let mut cursor = col_segments.find(
//         doc! {
//             "job_id": db_job_oid
//         }
//     )
//     .await?;
//     let mut proofs = BTreeMap::<u32, Vec<pipeline::Proof>>::new();
//     while let Some(db_segment) = cursor.try_next().await? {        
//         proofs.entry(db_segment.id)
//         .and_modify(|v| {
//             v.push(
//                 pipeline::Proof {
//                     cid: db_segment.proof.cid.clone(),
//                     prover: db_segment.proof.prover.clone(),
//                     spent: false
//                 }
//             );
//         })
//         .or_insert_with(|| {
//             vec![
//                 pipeline::Proof {
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

async fn subscribe_to_zeth_segment_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<redis::Value> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("zeth-segment-stream").arg(&last_id)
                .query_async(&mut redis_con)
                .await
                .unwrap();
            // update last_id
            for stream in result.as_sequence().unwrap(){
                let data = stream.as_sequence().unwrap();
                let contents = data[1].as_sequence().unwrap();
                let last_object = contents.last().unwrap();
                let objects = last_object.as_sequence().unwrap();
                last_id = if let redis::Value::BulkString(new_last_id) = &objects[0] {
                    String::from_utf8_lossy(&new_last_id).to_string() 
                } else {
                    continue
                };
                info!("Last segment item read from the ValKey server: `{last_id}`");
            }
            let _ = tx.send(result).await;
        }
    });
    rx
}


async fn subscribe_to_zeth_keccak_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<redis::Value> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("zeth-keccak-stream").arg(&last_id)
                .query_async(&mut redis_con)
                .await
                .unwrap();
            // update last_id
            for stream in result.as_sequence().unwrap(){
                let data = stream.as_sequence().unwrap();
                let contents = data[1].as_sequence().unwrap();
                let last_object = contents.last().unwrap();
                let objects = last_object.as_sequence().unwrap();
                last_id = if let redis::Value::BulkString(new_last_id) = &objects[0] {
                    String::from_utf8_lossy(&new_last_id).to_string() 
                } else {
                    continue
                };
                info!("Last Keccak item read from the ValKey server: `{last_id}`");
            }
            let _ = tx.send(result).await;
        }
    });
    rx
}

async fn subscribe_to_zeth_zkr_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<redis::Value> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("zeth-zkr-stream").arg(&last_id)
                .query_async(&mut redis_con)
                .await
                .unwrap();
            // update last_id
            for stream in result.as_sequence().unwrap(){
                let data = stream.as_sequence().unwrap();
                let contents = data[1].as_sequence().unwrap();
                let last_object = contents.last().unwrap();
                let objects = last_object.as_sequence().unwrap();
                last_id = if let redis::Value::BulkString(new_last_id) = &objects[0] {
                    String::from_utf8_lossy(&new_last_id).to_string() 
                } else {
                    continue
                };
                info!("Last Zkr item read from the ValKey server: `{last_id}`");
            }
            let _ = tx.send(result).await;
        }
    });
    rx
}