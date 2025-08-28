use futures::{
    select,
    stream::{
        FuturesUnordered,
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
    future::IntoFuture,
    collections::BTreeMap,
};
use bincode;

use anyhow;
// use reqwest;

use clap::{
    Parser, Subcommand
};
use xxhash_rust::xxh3::xxh3_128;
use env_logger::Env;
use log::{info, warn};
// use mongodb::{
//     // bson,
//     bson::{
//         // Bson,
//         doc,
//     },
//     options::{
//         ClientOptions,
//         ServerApi,
//         ServerApiVersion
//     },
// };
use peyk::{
    p2p::{MyBehaviourEvent},
    protocol,
    protocol::{
        ProofKind
    }
};

use pipeline::{
    sp1_subblock::Pipeline,
    sp1_subblock::Stage,
};

// mod db;

// CLI
#[derive(Parser, Debug)]
#[command(name = "Client CLI for Wholesum(subblock)")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Wholesum is a p2p prover network for ETH L1 block proving. \
                   This program is a CLI for the subblock client node.",
          long_about = None
)]
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
    info!("Proving blocks in a subblock fasion.");

    // setup mongodb
    // let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // setup redis
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let redis_con = redis_client.get_multiplexed_async_connection().await?;    
    let mut rsp_subblock_stdin_stream = subscribe_to_rsp_subblock_stdin_stream(redis_con.clone()).await;
    let mut rsp_agg_stdin_stream = subscribe_to_rsp_agg_stdin_stream(redis_con.clone()).await;

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
    info!("My PeerId: `{my_peer_id}`");     

    // let col_jobs = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Job>("jobs");
    // let col_proofs = db_client
    //     .database("wholesum_client")
    //     .collection::<db::Proof>("proofs");    
    
    // futures for mongodb progress saving 
    // let mut db_insert_futures = FuturesUnordered::new();
    // let mut db_update_futures = FuturesUnordered::new();

    let mut pipeline = Pipeline::new()?;
    // let (mut pipeline, db_job_oid) = match &cli.job {
    //     Some(Commands::New{ job_file }) => {
    //         let pipeline = Pipeline::new(job_file)?;
    //         let oid = col_jobs.insert_one(
    //             db::Job {
    //                 id: pipeline.id.to_string(),
    //                 image_id: pipeline.image_id.into()
    //             }
    //         )
    //         .await?
    //         .inserted_id;
    //         (pipeline, oid)            
    //     },

    //     Some(Commands::Resume{ job_id }) => {
    //         resume_job(
    //             &db_client.database("wholesum_client").collection("jobs"),
    //             &col_segments,
    //             &col_joins,
    //             job_id
    //         )
    //         .await?
    //     },

    //     _ => {
    //         panic!("Missing command, not sure what you meant.");
    //     },
    // };
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
        interval(Duration::from_secs(5 * 60))
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

            // post needs
            _i = timer_post_job.select_next_some() => {
                let need = match pipeline.stage {
                    // request for groth16 proving
                    Stage::ExecuteSubblock => {
                        let nonce = rng.random::<u32>();
                        protocol::NeedKind::Execute(nonce)
                    },

                    // request for other kinds of proving
                    // _ => {
                    //     // let _outstanding_jobs = (
                    //     //     pipeline.num_outstanding_aggregate_items() +
                    //     //     pipeline.num_outstanding_resolve_items()
                    //     // ) as u32;
                    //     let nonce = rng.random::<u32>();
                    //     protocol::NeedKind::Prove(nonce)
                    // },
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

            // request transfer of final proofs periodically
            _i = timer_retrieve_agg_proof.select_next_some() => {                
            },

            // subblock stdins are ready
            value = rsp_subblock_stdin_stream.select_next_some() => {
                for stream in value.as_sequence().unwrap() {
                    let data = stream.as_sequence().unwrap();
                    // let _stream_name = &data[0];
                    let contents = data[1].as_sequence().unwrap();
                    let mut blob_lens = vec![];
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
                                        continue
                                    } else {
                                        warn!("Invalid subblock stdin id `{str_id}` from the ValKey server: `{err_msg}");
                                    }
                                    continue
                                }
                            }
                        } else {
                            continue
                        };
                        let blob = if let redis::Value::BulkString(blob) = &segment_data[1] {
                            blob_lens.push(blob.len());
                            blob.to_owned()
                        } else {
                            continue
                        };
                        pipeline.feed_subblock_stdin(id as usize, blob);                        
                    }
                    info!("All subblock stdins have been read from the ValKey server.");
                    info!(
                        "Largest blob size: `{}`, smallest blob size: `{}`",
                        blob_lens.iter().max().unwrap(),
                        blob_lens.iter().min().unwrap()
                    );
                    pipeline.stop_subblock_stdin_feeding();
                }
            },

            // agg stdin is ready
            value = rsp_agg_stdin_stream.select_next_some() => {
                for stream in value.as_sequence().unwrap() {
                    let data = stream.as_sequence().unwrap();
                    // let _stream_name = &data[0];
                    let contents = data[1].as_sequence().unwrap();
                    let mut blob_lens = vec![];
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
                                        continue
                                    } else {
                                        warn!("Invalid agg stdin id `{str_id}` from the ValKey server: `{err_msg}");
                                    }
                                    continue
                                }
                            }
                        } else {
                            continue
                        };
                        let blob = if let redis::Value::BulkString(blob) = &segment_data[1] {
                            blob_lens.push(blob.len());
                            blob.to_owned()
                        } else {
                            continue
                        };
                        // pipeline.feed_aggregate_stdin(id as usize, blob);  
                    }
                    info!("Agg stdin has been read from the ValKey server.");
                    info!(
                        "Largest blob size: `{}`, smallest blob size: `{}`",
                        blob_lens.iter().max().unwrap(),
                        blob_lens.iter().min().unwrap()
                    );
                    // pipeline.stop_aggregate_stdin_feeding();
                }
            },

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
                        protocol::Request::Would => {
                            let prover_id = prover_peer_id.to_bytes();
                            match pipeline.stage {                                
                                
                                Stage::ExecuteSubblock => {
                                    let (batch_id, assignments) = match pipeline
                                        .assign_execute_subblock_batch(&prover_id)
                                    {
                                        Some((i, a)) => (i, a),

                                        None => {                                    
                                            // warn!("No execute jobs for `{prover_peer_id:?}` at this time.");
                                            continue
                                        }
                                    };  
                                    let compute_job = protocol::ComputeJob {
                                        id: pipeline.id,
                                        kind: protocol::JobKind::SP1(protocol::SP1Op::Execute(
                                            protocol::ExecuteDetails {
                                                id: batch_id,
                                                batch: assignments
                                                    .into_iter()
                                                    .map(|ass| {
                                                        match ass {
                                                            pipeline::sp1_subblock::Input::Blob(blob) => 
                                                                protocol::InputBlob::Blob(blob),

                                                            pipeline::sp1_subblock::Input::Token(prover, proof) => 
                                                                protocol::InputBlob::Token(
                                                                    proof.hash,
                                                                    prover,
                                                                )
                                                        }                                                        
                                                    })
                                                .collect()
                                            }
                                        ))
                                    };
                                    if let Err(e) = swarm
                                        .behaviour_mut()
                                        .req_resp
                                            .send_response(
                                                channel,
                                                protocol::Response::Job(compute_job)
                                            )
                                    {
                                        warn!("Failed to send the subblock job for execution: `{e:?}`");
                                    } else {
                                        pipeline.confirm_execute_subblock_batch_assignment(
                                            &prover_id,
                                            batch_id
                                        );
                                        info!("Sent the subblock execute job to `{prover_peer_id}`.");
                                    }
                                },
                            };
                        },

                        // prover has finished its job
                        protocol::Request::ProofIsReady(token) => {
                            if pipeline.id != token.job_id {
                                warn!("Ignored unknown proof token: `{token:?}`");
                                continue;
                            }

                            // let prover_id = prover_peer_id.to_bytes();
                            // match token.kind {
                                
                            // };
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
                            match pipeline.stage {
                                // Stage::Assumption => {
                                //     info!(
                                //         "Received the final aggregated proof from `{}`",
                                //         peer_id,
                                //     );
                                //     pipeline.add_final_agg_proof(
                                //         peer_id.to_bytes(),
                                //         blob.clone()
                                //     );
                                //     db_insert_futures.push(
                                //         col_proofs.insert_one(
                                //             db::Proof {
                                //                 job_id: db_job_oid.clone(),
                                //                 prover: peer_id.to_bytes(),
                                //                 hash: xxh3_128(&blob).to_string(),
                                //                 blob: Some(blob),
                                //                 round_number: Some(pipeline.cur_agg_round_number() as u32),
                                //                 kind: db::Kind::Aggregate,
                                //                 batch_id: 0u128.to_string()
                                //             }
                                //         )
                                //         .into_future()
                                //     );
                                // },                               

                                _ => {}
                            }

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

async fn subscribe_to_rsp_subblock_stdin_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<redis::Value> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("rsp-subblock-stdin-stream").arg(&last_id)
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
                info!("Last rsp subblock stdin item read from the ValKey server: `{last_id}`");
            }
            let _ = tx.send(result).await;
        }
    });
    rx
}

async fn subscribe_to_rsp_agg_stdin_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<redis::Value> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("rsp-agg-stdin-stream").arg(&last_id)
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
                info!("Last rsp subblock stdin item read from the ValKey server: `{last_id}`");
            }
            let _ = tx.send(result).await;
        }
    });
    rx
}