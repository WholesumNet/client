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
    gossipsub, mdns, request_response,
    identity, identify,  
    swarm::{SwarmEvent},
    PeerId,
};
use std::{
    fs,
    env,
    time::{
        Instant, 
        Duration,
    },
    // future::IntoFuture,
    collections::{
        HashSet,
        BTreeMap,
        VecDeque
    },
};
use bincode;
use rand::prelude::*;
use anyhow::{
    Context
};
// use reqwest;

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
    p2p::{MyBehaviourEvent},
    protocol,
    blob_transfer
};

use anbar;

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
    #[arg(long, action)]
    dev: bool,

    #[arg(short, long)]
    key_file: Option<String>,
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
    info!("Proving blocks using block division logic.");

    // setup mongodb
    // let db_client = mongodb_setup("mongodb://localhost:27017").await?;

    // setup redis
    let redis_client = redis::Client::open("redis://:redispassword@localhost:6379/0")?;
    let redis_con = redis_client.get_multiplexed_async_connection().await?;    

    let mut active_provers = HashSet::<PeerId>::new();

    // <block number, [stdins]>
    let mut blocks = BTreeMap::<u32, Vec<u128>>::new();
    let mut current_block: Option<u32> = None;
    let mut outstanding_blocks = VecDeque::<u32>::new();
    let mut block_stream = subscribe_to_block_stream(redis_con.clone()).await;    

    // blob store
    let mut blob_store = anbar::BlobStore::new();

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

    let mut pipeline = tokio::task::spawn_blocking(||
        Pipeline::new().unwrap()
    )
    .await?;    

    // swarm 
    let mut swarm = peyk::p2p::setup_swarm(&local_key)?;
    let topic = gossipsub::IdentTopic::new("<-- Wholesum p2p prover bazaar -->");
    let _ = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic);     

    // bootstrap kademlia
    if !cli.dev {
        let bootnode_peer_id = env::var("BOOTNODE_PEER_ID")
            .context("`BOOTNODE_PEER_ID` environment variable does not exist.")?;
        let bootnode_ip_addr = env::var("BOOTNODE_IP_ADDR")
            .context("`BOOTNODE_IP_ADDR` environment variable does not exist.")?;
        // get to know bootnodes        
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
    }
    
    // listen on all interfaces and whatever port the os assigns
    //@ should read from the config file
    swarm.listen_on("/ip4/0.0.0.0/udp/20201/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/tcp/20201".parse()?)?;
    swarm.listen_on("/ip6/::/udp/20201/quic-v1".parse()?)?;

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

    let mut rng = rand::rng();

    let mut recent_insufficient_peers_cry_time = Instant::now();

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

            // p = pipeline_init_future.select_next_some() => {
            //     self.pipeline = Some(p?);
            // }

            // new blocks inbound
            new_blocks = block_stream.select_next_some() => {                
                info!(
                    "Received a new batch of blocks to prove: `{:?}`",
                    new_blocks.keys()
                );
                // prove blocks fifo
                outstanding_blocks.extend(new_blocks.keys().cloned());
                if outstanding_blocks.is_empty() {
                    warn!("Detected empty block stream.");
                    continue;
                }
                for (block_number, stdin_map) in new_blocks.into_iter() {
                    let mut blob_hashes = 
                        Vec::<u128>::with_capacity(stdin_map.len());
                    for (_i, blob) in stdin_map.into_iter() {
                        blob_hashes.push(blob_store.store(blob));
                    }                    
                    blocks.insert(block_number, blob_hashes);
                }
                if current_block.is_some() {
                    // wait until the current block is proved
                    continue;
                }
                current_block = outstanding_blocks.pop_front();
                if let Some(block_number) = current_block {
                    let blob_hashes = blocks.get(&block_number).unwrap();                    
                    pipeline.begin_next_block(
                        block_number,
                        &blob_hashes,
                        &my_peer_id,
                    );
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
                    info!(
                        "Inbound identify event from {:?}: {:?}`",
                        peer_id,
                        info
                    );
                    if !cli.dev {
                        for addr in info.listen_addrs {
                            swarm
                                .behaviour_mut()
                                .kademlia
                                .add_address(&peer_id, addr);
                        }
                    }
                },

                // gossipsub events
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message{..})) => {},

                // requests
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: prover,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        // prover indicates her interest to prove                        
                        protocol::Request::Would => {
                            active_provers.insert(prover.clone());
                            if current_block.is_none() {
                                continue;
                            }
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
                                                owner: t.owner.to_bytes(),
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

                                    blob_store.add_incomplete_blob(token.hash);
                                    let _req_id = swarm
                                        .behaviour_mut()
                                        .blob_transfer
                                        .send_request(
                                            &prover,
                                            blob_transfer::Request::GetInfo(token.hash)
                                        );
                                    info!(
                                        "Requested info of agg proof blob(`{:?}`) from `{:?}`",
                                        token.hash,
                                        prover
                                    );                                    
                                },                          
                            };
                        },
                    }
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        _ => {},
                    };
                },

                // blob transfer requests
                SwarmEvent::Behaviour(MyBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: _peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        //request_id,
                        ..
                    }
                })) => {                
                    match request {
                        blob_transfer::Request::GetInfo(hash) => {
                            if let Some(num_chunks) = blob_store.get_blob_info(hash) {
                                if let Err(e) = swarm
                                    .behaviour_mut()
                                    .blob_transfer
                                        .send_response(
                                            channel,
                                            blob_transfer::Response::Info(blob_transfer::BlobInfo {
                                                hash: hash,                                            
                                                num_chunks: num_chunks,
                                            })
                                        )
                                {
                                    warn!("Failed to send back blob info: `{e:?}`");
                                }
                            }
                        },

                        blob_transfer::Request::GetChunk(blob_hash, req_chunk_index) => {
                            if let Some((data, chunk_hash)) = blob_store.get_chunk(
                                blob_hash, 
                                req_chunk_index
                            ) {
                                if let Err(e) = swarm
                                    .behaviour_mut()
                                    .blob_transfer
                                        .send_response(
                                            channel,
                                            blob_transfer::Response::Chunk(
                                                blob_transfer::BlobChunk {
                                                    blob_hash: blob_hash,
                                                    index: req_chunk_index,
                                                    data: data,
                                                    chunk_hash: chunk_hash,
                                                }
                                            )
                                        )
                                {
                                    warn!("Failed to send back the blob chunk: `{e:?}`");
                                    //@ wtdh
                                }
                            }
                        },
                    }
                },

                // blob transfer responses
                SwarmEvent::Behaviour(MyBehaviourEvent::BlobTransfer(request_response::Event::Message {
                    peer: peer_id,
                    message: request_response::Message::Response {
                        response,
                        //response_id,
                        ..
                    }
                })) => {                
                    match response {
                        blob_transfer::Response::Info(blob_info) => {
                            blob_store.add_blob_info(blob_info.hash, blob_info.num_chunks);
                            // request first chunk
                            let _req_id = swarm
                                .behaviour_mut()
                                .blob_transfer
                                .send_request(
                                    &peer_id,
                                    blob_transfer::Request::GetChunk(blob_info.hash, 0)
                                );
                            info!(
                                "Requested the first chunk of the blob(`{}`) from `{}`",
                                blob_info.hash,
                                peer_id
                            );
                        },

                        blob_transfer::Response::Chunk(blob_chunk) => {                        
                            //@ assumed owner === chunk sender
                            blob_store.add_blob_chunk(
                                blob_chunk.blob_hash,
                                blob_chunk.index,
                                blob_chunk.data,
                                blob_chunk.chunk_hash
                            );
                            if blob_store.is_blob_complete(blob_chunk.blob_hash) {
                                if pipeline.stage == Stage::Agg {
                                    let proof = blob_store.get_blob(blob_chunk.blob_hash).unwrap();
                                    if let Ok(()) = pipeline.verify_agg_proof(&proof) {
                                        info!(
                                            "Block proof(`{:?}`) is verified.",
                                            pipeline.block_number
                                        );
                                        // being the next one
                                        current_block = outstanding_blocks.pop_front();
                                        if let Some(block_number) = current_block {
                                            let blob_hashes = blocks.get(&block_number).unwrap();                    
                                            pipeline.begin_next_block(
                                                block_number,
                                                &blob_hashes,
                                                &my_peer_id,
                                            );
                                        } else {
                                            info!("All blocks are processed, waiting for the next batch.");
                                        }
                                    } else {
                                        warn!(
                                            "Proof verification failed for block(`{:?}`).",
                                            pipeline.block_number
                                        );
                                        //@ wtd?
                                    }
                                }                                
                            } else {
                                // request next chunk
                                if let Some(next_chunk_index) = blob_store.get_next_blob_chunk_index(blob_chunk.blob_hash) {
                                    let _req_id = swarm
                                        .behaviour_mut()
                                        .blob_transfer
                                        .send_request(
                                            &peer_id,
                                            blob_transfer::Request::GetChunk(blob_chunk.blob_hash, next_chunk_index)
                                        );
                                }                             
                            }                            
                        }
                    }
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
) -> mpsc::Receiver<BTreeMap<u32, BTreeMap<u32, Vec<u8>>>> {
    let (mut tx, rx) = mpsc::channel(32);
    let mut last_id = "0".to_string();
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
            let entries = contents[1].as_sequence().unwrap();
            let mut blocks: BTreeMap<u32, BTreeMap<u32, Vec<u8>>> = BTreeMap::new();            
            for entry in entries {
                let items = &entry.as_sequence().unwrap();
                if let BulkString(bs) = &items[0] {
                    last_id = String::from_utf8_lossy(&bs).into_owned();
                }
                let (block_number, stdin_id) = 
                    if let BulkString(bs) = &items[1].as_sequence().unwrap()[0] {
                        let blob_id = String::from_utf8_lossy(&bs).into_owned();
                        let tokens: Vec<_> = blob_id.split("-")
                            .filter_map(|e| e.parse::<u32>().ok())
                            .collect();
                        (tokens[0], tokens[1])
                } else {
                    warn!("Failed to parse block number and subblock id.");
                    continue;
                };
                if let BulkString(blob) = &items[1].as_sequence().unwrap()[1] {
                    blocks.entry(block_number)
                        .and_modify(|stdins| {
                            if stdins.contains_key(&stdin_id) {
                                warn!(
                                    "Ignnored duplicate subblock for blob({}-{}).",
                                    block_number,
                                    stdin_id
                                );
                            } else {
                                stdins.insert(stdin_id, blob.to_owned());
                            }
                        })
                        .or_insert_with(||
                            BTreeMap::from([
                                (stdin_id, blob.to_owned())
                            ])
                        );
                } else {
                    warn!(
                        "Failed to retrieve subblock blob(`{}-{}`) from Redis.",
                        block_number,
                        stdin_id
                    );
                    continue;
                }
            }   
            let _ = tx.send(blocks).await;
        }
    });
    rx
}

