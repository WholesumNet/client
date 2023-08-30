#![doc = include_str!("../README.md")]

use futures::{future::Either, prelude::*, select};
use async_std::stream;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::OrTransport, upgrade},
    gossipsub, identity, mdns, noise, request_response,
    swarm::NetworkBehaviour,
    swarm::{StreamProtocol, SwarmBuilder, SwarmEvent},
    tcp, yamux, PeerId, Transport,
};
use libp2p_quic as quic;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use std::fs;
use toml;
use clap::Parser;
use uuid::Uuid;
use comms::{notice, compute};

// combine Gossipsub, mDNS, and RequestResponse
#[derive(NetworkBehaviour)]
struct MyBehaviour {
    req_resp: request_response::cbor::Behaviour<notice::Request, notice::Response>,
    gossipsub: gossipsub::Behaviour,
    mdns: mdns::async_io::Behaviour,
}

// CLI
#[derive(Parser, Debug)]
#[command(name = "Client CLI for Wholesum: p2p verifiable computing marketplace.")]
#[command(author = "Wholesum team")]
#[command(version = "1.0")]
#[command(about = "Yet another verifiable compute marketplace.", long_about = None)]
struct Cli {
    #[arg(short, long)]
    job: Option<String>,
}

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let cli = Cli::parse();
    println!("Started client agent for Wholesum.");
    // any jobs for the client?
    let mut raw_jobs = Vec::<compute::JobDetails>::new();

    if let Some(job_filename) = cli.job {
        println!("Loading job from: `{job_filename}`...");
        match fs::read_to_string(job_filename) {
            Ok(ss) => {
                match toml::from_str(ss.as_str()) {
                    Ok(j) => raw_jobs.push(j),
                    Err(e) => println!("Job parse error: {:#?}", e),
                }                
            },
            Err(e) => println!("Job load error: {:?}", e),
        };
    }    

    let mut pending_jobs = HashMap::<u128, compute::Job>::new();

    // get a random peer_id
    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());
    println!("PeerId: {local_peer_id}");
    // setup an encrypted dns-enabled transport over yamux
    let tcp_transport = tcp::async_io::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::Config::new(&id_keys).expect("signing libp2p static keypair"))
        .multiplex(yamux::Config::default())
        .timeout(std::time::Duration::from_secs(30))
        .boxed();
    let quic_transport = quic::async_std::Transport::new(quic::Config::new(&id_keys));
    let transport = OrTransport::new(quic_transport, tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    // to content-address message, take the hash of message and use it as an id
    let message_id_fn = |message: &gossipsub::Message| {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        gossipsub::MessageId::from(s.finish().to_string())
    };

    // set a custom Gossipsub configuration
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // aid debugging by not cluttering log space
        .validation_mode(gossipsub::ValidationMode::Strict) // enforce message signing
        .message_id_fn(message_id_fn) // content-address messages
        .build()
        .expect("Invalid gossipsub config.");

    // build a Gossipsub network behaviour
    let mut gossipsub = gossipsub::Behaviour::new(
        gossipsub::MessageAuthenticity::Signed(id_keys),
        gossipsub_config,
    )
    .expect("Invalid behaviour configuration.");

    // subscribe to our topic
    const TOPIC_OF_INTEREST: &str = "<-- Compute Bazaar -->";
    println!("topic of interest: `{TOPIC_OF_INTEREST}`");
    // @ use topic_hash config for auto hash(topic)
    let topic = gossipsub::IdentTopic::new(TOPIC_OF_INTEREST);
    let _ = gossipsub.subscribe(&topic);

    // create a swarm to manage events and peers
    let mut swarm = {
        let mdns = mdns::async_io::Behaviour::new(mdns::Config::default(), local_peer_id)?;
        let req_resp = request_response::cbor::Behaviour::<notice::Request, notice::Response>::new(
            [(
                StreamProtocol::new("/p2pcompute"),
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );
        let behaviour = MyBehaviour {
            req_resp: req_resp,
            gossipsub: gossipsub,
            mdns: mdns,
        };
        SwarmBuilder::with_async_std_executor(transport, behaviour, local_peer_id).build()
    };

    // read full lines from stdin
    // let mut input = io::BufReader::new(io::stdin()).lines().fuse();

    // listen on all interfaces and whatever port the os assigns
    swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // setup a timer to post jobs
    let mut need_compute_timer = stream::interval(Duration::from_secs(5)).fuse();
    let mut idle_timer = stream::interval(Duration::from_secs(5 * 60)).fuse();
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
            () = idle_timer.select_next_some() => {
                if raw_jobs.len() == 0 {
                    println!(
                        "Since you got no more jobs, how about creating one by typing in right here?"
                    );
                }                
            },
            // post jobs 
            () = need_compute_timer.select_next_some() => {
                if raw_jobs.len() == 0 {
                    println!("No jobs to post.");
                    continue;
                }
                let need_compute_msg = vec![notice::NeedRequest::Computation.into()];
                if let Err(e) = swarm
                    .behaviour_mut().gossipsub
                    .publish(topic.clone(), need_compute_msg) {
            
                    println!("Publish error: {e:?}")
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered a new peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }                     
                },

                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _multiaddr) in list {
                        println!("mDNS discovered peer has expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                },

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

                // incoming compute/verify request(interest actually)
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: sender_peer_id,
                    message: request_response::Message::Request {
                        request,
                        channel,
                        ..//request_id,
                    }
                })) => {                
                    // println!("request from: `{sender_peer_id}`, req_id: {:#?}, chan: {:#?}",
                    //     request_id, channel);
                    match request {
                        notice::Request::Compute(compute_offer) => {                            
                            println!("received `compute offer` from server: {}, offer: {:#?}", 
                                sender_peer_id, compute_offer);
                            //@ examine offer
                            if raw_jobs.len() == 0 {
                                println!("No more jobs left to be sent.");
                                continue;
                            }
                            let job_details = raw_jobs.pop().unwrap();
                            // match and send job
                            let job_id = Uuid::new_v4();
                            let compute_job = compute::Job {
                                id: job_id.to_string(),
                                details: job_details,
                            };
                            pending_jobs.insert(job_id.to_u128_le(), compute_job.clone());

                            let _ = swarm
                                .behaviour_mut().req_resp
                                .send_response(
                                    channel,
                                    notice::Response::Compute(compute_job),
                                );
                        },

                        notice::Request::Verify => {
                            println!("received `verification offer` from server: {}", 
                                sender_peer_id);
                        },
                    }
                },

                // incoming response to an earlier compute/verify offer
                SwarmEvent::Behaviour(MyBehaviourEvent::ReqResp(request_response::Event::Message{
                    peer: sender_peer_id,
                    message: request_response::Message::Response {
                        request_id,
                        ..//response,
                    }
                })) => {                                
                    println!("response came from {}, req_id: {:#?}",
                        sender_peer_id, request_id);
                },

                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Local node is listening on {address}");
                },

                _ => {}

            },
        }
    }
}
