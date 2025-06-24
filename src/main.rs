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

use anyhow;
// use reqwest;

use clap::{
    Parser, Subcommand
};

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

mod pipeline;
use pipeline::Pipeline;

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
    let _db_client = mongodb_setup("mongodb://localhost:27017").await?;

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

    let mut pipeline = match &cli.job {
        Some(Commands::New{ job_file }) => {
            let pipeline = Pipeline::new(job_file)?;
            // let oid = col_jobs.insert_one(
            //     db::Job {
            //         id: pipeline.id.clone(),
            //         verification: db::Verification {
            //             image_id: job.schema.image_id.clone(),
            //         },
            //         snark_proof: None,
            //     }
            // )
            // .await?
            // .inserted_id;
            // (job, oid)
            pipeline
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
                let need = match pipeline.stage {
                    // request for groth16 proving
                    pipeline::Stage::Groth16 => {
                        let nonce = rng.random::<u32>();
                        protocol::NeedKind::Groth16(nonce)
                    },

                    // request for other kinds of proving
                    _ => {
                        // let _outstanding_jobs = (
                        //     pipeline.num_outstanding_aggregate_items() +
                        //     pipeline.num_outstanding_resolve_items()
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
                if pipeline.stage == pipeline::Stage::Resolve &&
                   pipeline.agg_proof.is_none()
                {
                    let (prover, hash)  = pipeline.agg_proof_token();
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
                                        pipeline.stop_segment_feeding();
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
                        pipeline.feed_segment(id as usize, blob);                        
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
                        pipeline.feed_assumption(&blob);
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
                        pipeline.feed_assumption(&blob);
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
                    // message,
                    ..
                })) => {
                    // let msg_str = String::from_utf8_lossy(&message.data);
                    // println!("Got message: '{}' with id: {id} from peer: {peer_id}",
                    //          msg_str);
                    // info!("received gossip message: {:#?}", message);                    
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
                            match pipeline.stage {
                                pipeline::Stage::Groth16 => {                                    
                                    let (batch_id, assignments) = match pipeline
                                        .assign_groth16_batch(&prover_id)
                                    {
                                        Some((i, a)) => (i, a),

                                        None => {                                    
                                            warn!("No Groth16 jobs for `{prover_peer_id:?}` at this time.");
                                            continue
                                        }
                                    };
                                    let compute_job = protocol::ComputeJob {
                                        id: pipeline.id,
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
                                        pipeline.confirm_groth16_assignment(&prover_id, batch_id);
                                        info!("Sent the Groth16 job to `{prover_peer_id}`.");
                                    }
                                },

                                pipeline::Stage::Resolve => {
                                    //@ ask for agg proof blob here too?
                                    // if pipeline.num_outstanding_resolve_items() == 0 {
                                    //     continue
                                    // }                                    
                                    let (batch_id, assignments) = match pipeline
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
                                        id: pipeline.id,
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
                                        pipeline.confirm_assumption_assignment(&prover_id, batch_id);
                                        info!("Sent the assumption job to `{prover_peer_id}`.");
                                    }
                                },

                                pipeline::Stage::Aggregate => {
                                    let (batch_id, assignments) = match pipeline
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
                                        id: pipeline.id,
                                        kind: protocol::JobKind::Aggregate(
                                            protocol::AggregateDetails {
                                                id: batch_id,
                                                blobs_are_segment: pipeline.num_agg_rounds() == 1,
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
                                        pipeline.confirm_agg_assignment(&prover_id, batch_id);
                                        info!("Sent the aggregate job to `{prover_peer_id}`.");
                                    }
                                }
                            };
                        },

                        // prover has finished its job
                        protocol::Request::ProofIsReady(token) => {
                            if pipeline.id != token.job_id {
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
                                    pipeline.add_assumption_proof(
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
                                    pipeline.add_agg_proof(
                                        batch_id,
                                        token.hash,
                                        prover_id
                                    );
                                    if pipeline.stage == pipeline::Stage::Resolve {
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
                                    pipeline.add_groth16_proof(batch_id, blob, prover_id);                              
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
                            pipeline.add_final_agg_proof(
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