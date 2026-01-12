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
use redis::Value::BulkString;
use libp2p::{
    identity,
    identify,  
    gossipsub,
    kad,
    request_response,
    swarm::{
        SwarmEvent
    },
    PeerId,
    multiaddr::Protocol
};
use std::{
    env,
    time::{
        Instant, 
        Duration,
    },
    // future::IntoFuture,
    collections::{
        HashSet,
    },
};
use bincode;
use rand::prelude::*;
use anyhow::{
    Context
};
use clap::{
    Parser
};
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
    p2p::{
        GlobalBehaviourEvent
    },
    protocol,
    blob_transfer
};

// use anbar;

use pipeline::{
    pipeline::{
        Pipeline,
        Stage,
    },
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
    #[arg(short, long)]
    key_file: Option<String>,
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .init();
    let cli = Cli::parse();
    info!("<-> Client agent for Wholesum network <->");    
    info!("Proving blocks using block division logic.");

    // setup mongodb
    // let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // setup redis
    let redis_client = redis::Client::open("redis://:redispassword@localhost:6379/0")?;
    let redis_con = redis_client.get_multiplexed_async_connection().await?;    

    let mut active_provers = HashSet::<PeerId>::new();
    
    let mut block_stream = subscribe_to_block_stream(redis_con.clone()).await;    

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

    // Libp2p swarm 
    // peer id
    let local_key = {
        if let Some(key_file) = cli.key_file {
            let bytes = std::fs::read(key_file).unwrap();
            identity::Keypair::from_protobuf_encoding(&bytes)?
        } else {
            // Create a random key for ourselves
            let new_key = identity::Keypair::generate_ed25519();
            let bytes = new_key.to_protobuf_encoding().unwrap();
            let _bw = std::fs::write("./key.secret", bytes);
            warn!("No keys were supplied, so one is generated for you and saved to `./key.secret` file.");
            new_key
        }
    };    
    let my_peer_id = PeerId::from_public_key(&local_key.public());    
    info!(
        "My peer id: `{}`",
        my_peer_id
    );  
    let mut swarm = peyk::p2p::setup_global_swarm(&local_key)?;
    // listen on all interfaces
    // ipv4
    swarm.listen_on(
        "/ip4/0.0.0.0/udp/20201/quic-v1".parse()?
    )?;
    swarm.listen_on(
        "/ip4/0.0.0.0/tcp/20201".parse()?
    )?;
    // ipv6
    // ipv6
    // swarm.listen_on(
    //     "/ip6/::/udp/20201/quic-v1".parse()?
    // )?;
    // swarm.listen_on(
    //     "/ip6/::/tcp/20201".parse()?
    // )?;
    // init gossip
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic);
    
    // init kademlia: get to know bootnode(s)
    let bootnode_peer_id = env::var("BOOTNODE_PEER_ID")
        .context("`BOOTNODE_PEER_ID` environment variable does not exist.")?;
    let bootnode_ip_addr = env::var("BOOTNODE_IP_ADDR")
        .context("`BOOTNODE_IP_ADDR` environment variable does not exist.")?;
    swarm.behaviour_mut()
        .kademlia
        .add_address(
            &bootnode_peer_id.parse()?,
            format!(
                "/ip4/{}/tcp/20201",
                bootnode_ip_addr
            )
            .parse()?
        );
    // initiate bootstrapping
    match swarm.behaviour_mut().kademlia.bootstrap() {
        Ok(query_id) => {            
            info!(
                "Bootstrap is initiated, query id: {:?}",
                query_id
            );
        },
        Err(e) => {
            info!(
                "Bootstrap failed: {:?}",
                e
            );
        }
    };
    // specify the external address
    let external_ip_addr = env::var("EXTERNAL_IP_ADDR")
        .context("`EXTERNAL_IP_ADDR` environment variable does not exist.")?;
    let external_port = env::var("EXTERNAL_PORT")
        .context("`EXTERNAL_PORT` environment variable does not exist.")?;
    swarm.add_external_address(
        format!(
            "/ip4/{}/tcp/{}",
            external_ip_addr,
            external_port
        )
        .parse()?
    );
    swarm.add_external_address(
        format!(
            "/ip4/{}/udp/{}/quic-v1",
            external_ip_addr,
            external_port
        )
        .parse()?
    );

    // to update kademlia tables
    let mut timer_peer_discovery = IntervalStream::new(
        interval(Duration::from_secs(60))
    )
    .fuse();
    // to update prover list
    let mut timer_update_prover_list = IntervalStream::new(
        interval(Duration::from_secs(5))
    )
    .fuse();
    // to revoke stale assignments
    let mut timer_revoke_stale_assignments = IntervalStream::new(
        interval(Duration::from_secs(2))
    )
    .fuse();

    let mut rng = rand::rng();

    let mut recent_insufficient_peers_cry_time = Instant::now();

    loop {
        select! {
            // try to discover new peers
            _i = timer_peer_discovery.select_next_some() => {                
                let random_peer_id = PeerId::random();
                // info!("Searching for the closest peers to `{random_peer_id}`");
                swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(random_peer_id);                
            },

            // update prover list
            _i = timer_update_prover_list.select_next_some() => {
                let nonce = rng.random::<u32>();
                let need = bincode::serialize(
                    &protocol::NeedKind::Prove(nonce)
                )
                .unwrap();
                if let Err(e) = swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), need)
                {
                    let now = Instant::now();
                    if now.duration_since(recent_insufficient_peers_cry_time).as_secs() > 120u64 {
                        warn!("Need compute gossip failed, error: `{e:?}`");
                        recent_insufficient_peers_cry_time = now;
                    }
                }
            },

            // to revoke stale assignments
            _i = timer_revoke_stale_assignments.select_next_some() => {
                pipeline.revoke_stale_assignments();                                
            },

            // p = pipeline_init_future.select_next_some() => {
            //     self.pipeline = Some(p?);
            // }

            // new blocks inbound from Redis
            (block_number, stdins) = block_stream.select_next_some() => {                
                pipeline.add_block(block_number, stdins);                
            },

            // libp2p events
            event = swarm.select_next_some() => match event {
                // general events
                SwarmEvent::NewListenAddr { address, .. } => {
                    info!("Local node is listening on {address}");
                },

                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    ..
                } => {
                    info!(
                        "A connection has been established to {} via {:?}",
                        peer_id,
                        endpoint
                    );                    
                },

                // identify events
                SwarmEvent::Behaviour(GlobalBehaviourEvent::Identify(identify::Event::Received {
                    // peer_id,
                    // info,
                    ..
                })) => {
                    // info!(
                    //     "Received identify from {}: {:#?}`",
                    //     peer_id,
                    //     info
                    // );                        
                },

                SwarmEvent::NewExternalAddrOfPeer {
                    peer_id,
                    address
                } => {
                    let is_public = address.iter()
                        .filter_map(|c| 
                            if let Protocol::Ip4(ip4_addr) = c {
                                Some(ip4_addr)
                            } else {
                                None
                            }
                        )
                        .all(|a| !a.is_private() && !a.is_loopback());
                    if is_public {                        
                        info!(
                            "Added public address of the peer to the DHT: {}",
                            address
                        );
                        swarm.behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, address);
                    }                      
                },



                // gossipsub events
                SwarmEvent::Behaviour(GlobalBehaviourEvent::Gossipsub(gossipsub::Event::Message{..})) => {},

                // kademlia events
                SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result: kad::QueryResult::GetClosestPeers(Ok(_ok)),
                    ..
                })) => {
                    // info!("Query finished with closest peers: {:#?}", ok.peers);
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                    result:
                        kad::QueryResult::GetClosestPeers(Err(kad::GetClosestPeersError::Timeout {
                            ..
                        })),
                    ..
                })) => {
                    // warn!("Query for closest peers timed out");
                },

                // SwarmEvent::Behaviour(GlobalBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed {
                //     result: kad::QueryResult::GetProviders(
                //         Ok(
                //             kad::GetProvidersOk::FoundProviders{ mut providers, .. }
                //         )
                //     ),
                //     ..
                // })) => {
                //     providers.remove(&my_peer_id);
                //     info!("providers: {:?}", providers);
                //     for peer_id in providers {
                //         let res = swarm.dial(peer_id);
                //         info!("dial result: {:?}", res);
                //     }
                // },

                // requests
                SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: prover,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    },
                    ..
                })) => {                
                    match request {
                        // prover indicates her interest to prove                        
                        protocol::Request::Would => {
                            active_provers.insert(prover.clone());                            
                            if pipeline.stage == Stage::Verify {
                                continue;
                            }                            
                            let ass = pipeline.assign(&prover);
                            if ass.is_none() {
                                // warn!(
                                //     "No assignments for `{:?}` at this time.",
                                //     prover
                                // );
                                continue;
                            }                 
                            let (batch_id, tokens) = ass.unwrap();
                            let elf_kind = if pipeline.stage == Stage::Subblock {
                                protocol::ELFKind::Subblock
                            } else {
                                protocol::ELFKind::Agg
                            };                                
                            let compute_job = protocol::ComputeJob {
                                id: pipeline.id,
                                kind: protocol::JobKind::SP1(protocol::SP1Op::Prove(
                                    protocol::ProveDetails {
                                        id: batch_id,
                                        elf_kind: elf_kind,
                                        tokens: tokens.into_iter()
                                            .map(|t| protocol::InputToken {
                                                hash: t.hash,
                                                owner: t.owner.unwrap_or_else(|| my_peer_id.clone()).to_bytes(),
                                            })
                                            .collect::<Vec<protocol::InputToken>>(),
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
                                warn!(
                                    "Failed to send job(`{:?}`) to prover (`{:?}`): `{:?}`",
                                    batch_id,
                                    prover,
                                    e
                                );
                            }                            
                        },

                        // prover has finished its job
                        protocol::Request::ProofIsReady(token) => {
                            if pipeline.id != token.job_id {
                                warn!("Ignored unknown proof token: `{token:?}`");
                                continue;
                            }
                            match token.kind {                          
                                protocol::ProofKind::Subblock(batch_id) => {
                                    info!(
                                        "Subblock: a new proof for batch(`{:?}`).",
                                        batch_id
                                    );
                                    pipeline.add_subblock_proof(
                                        batch_id,
                                        token.hash,
                                        prover.clone()
                                    );                                    
                                },

                                protocol::ProofKind::Agg(batch_id) => {
                                    info!(
                                        "Agg: a new proof for batch(`{:?}`).",
                                        batch_id
                                    );
                                    pipeline.add_agg_proof(
                                        batch_id,
                                        token.hash,
                                        prover.clone()
                                    );
                                    
                                    let _req_id = swarm.behaviour_mut()
                                        .blob_transfer
                                        .send_request(
                                            &prover,
                                            blob_transfer::Request(token.hash.to_string())
                                        );
                                    info!(
                                        "Requested agg proof blob(`{:?}`) from `{}`",
                                        token.hash,
                                        prover
                                    );                                    
                                },                          
                            };
                        },
                    }
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    },
                    ..
                })) => {                
                    match response {
                        _ => {},
                    };
                },

                // <blob transfer>
                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Request {
                        request: blob_transfer::Request(blob_hash),
                        channel,
                        //request_id,
                        ..
                    },
                    ..
                })) => {                
                    let blob_hash = blob_hash.parse::<u128>().unwrap();
                    if let Some(blob) = pipeline.get_blob(&blob_hash) {
                        if let Err(e) = swarm
                            .behaviour_mut()
                            .blob_transfer
                                .send_response(
                                    channel,
                                    blob_transfer::Response(blob.clone())
                                )
                        {
                            warn!(
                                "Failed to initiate the requested blob(`{}`)'s transmission: `{:?}`.",
                                blob_hash,
                                e
                            );
                        } else {
                            info!(
                                "The requested blob(`{}`)'s transmission to `{}` is initiated: {:.2} KB",
                                blob_hash,
                                peer_id,
                                blob.len() as f64 / 1024.0f64
                            );
                        }                            
                    } else {
                        warn!(
                            "The requested blob(`{}`) does not exist.",
                            blob_hash,
                        );
                    }
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Response {
                        response: blob_transfer::Response(blob),
                        //response_id,
                        ..
                    },
                    ..
                })) => {                                    
                    pipeline.verify_agg_proof(blob);                    
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::InboundFailure {
                    peer: peer_id,
                    connection_id,
                    request_id,
                    error,
                })) => {
                    warn!(
                        "Blob transfer `inbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                        peer_id,
                        connection_id,
                        request_id,
                        error
                    );
                },

                SwarmEvent::Behaviour(GlobalBehaviourEvent::BlobTransfer(request_response::Event::OutboundFailure {
                    peer: peer_id,
                    connection_id,
                    request_id,
                    error,
                })) => {
                    warn!(
                        "Blob transfer `outbound failure`: peer `{}`, con_id: {:?}, req_id: {:?}, e: {:?} ",
                        peer_id,
                        connection_id,
                        request_id,
                        error
                    );
                },

                _ => {
                    // info!("{:#?}", event);
                },

            },
        }
    }
}

async fn subscribe_to_block_stream(
    mut redis_con: redis::aio::MultiplexedConnection
) -> mpsc::Receiver<(u64, Vec<Vec<u8>>)> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
    let mut current_block = 0u64;
    // 8 subblocks + 1 agg stdin at max
    let mut stdins = Vec::<Vec<u8>>::with_capacity(9);
    task::spawn(async move {
        loop {
            let result: redis::Value = redis::cmd("XREAD")
                .arg("BLOCK").arg(0)
                .arg("STREAMS").arg("blocks").arg(&last_id)
                .query_async(&mut redis_con)
                .await
                .unwrap();
            let streams = result.as_sequence().unwrap();
            let contents = streams[0].as_sequence().unwrap();
            let _stream_name = &contents[0];
            for entry in contents[1].as_sequence().unwrap() {
                let items = &entry.as_sequence().unwrap();
                if let BulkString(bs) = &items[0] {
                    last_id = String::from_utf8_lossy(&bs).into_owned();
                }
                let block_data = items[1].as_sequence().unwrap();
                // 0: block number
                if let BulkString(bs) = &block_data[0] {
                    let maybe_block_number = String::from_utf8_lossy(&bs);
                    if maybe_block_number.eq_ignore_ascii_case("<EOB>") {
                        let _ = tx.send((current_block, stdins.clone())).await;
                        stdins.clear();
                        current_block = 0u64;
                    } else {
                        if current_block == 0u64 {
                            match maybe_block_number.parse::<u64>() {
                                Ok(block_number) => {
                                    current_block = block_number;
                                },

                                Err(e) => {
                                    warn!(
                                        "Failed to parse block number: {:?}",
                                        e
                                    );
                                }
                            };
                        }
                    }
                } else {
                    warn!("Expected block number but got something else.");
                    continue
                }
                // 1: stdin
                if let BulkString(blob) = &block_data[1] {
                    stdins.push(blob.to_owned());
                } else {
                    warn!("Expected stdin blob but got something else.");
                    continue
                }
            }   
        }
    });
    rx
}

