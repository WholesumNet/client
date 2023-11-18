
# Wholesum network `Client` CLI

## Overview

Wholesum network is a p2p verifiable computing network. It builds on top of [Risc0](https://risczero.com/), [Swarm](https://ethswarm.org), [FairOS-dfs](https://github.com/fairDataSociety/fairOS-dfs), and [Libp2p](https://libp2p.io) to facilitate verifiable computing at scale.  

## How to run

To run a client agent, you would first need to fork the [comms](https://github.com/WholesumNet/comms) library and put it in the parent(aka ..) directory of the client directory. Next, you would need a job file, in case you were interested to run a tiny job, with the following look in TOML:

<pre>

# schema of a tyipcal job

title = "yet another job"
timeout = 30

[compute]

# min ram capacity(in GB) for an offer to be accepted
min_memory_capacity = 4

# docker image run by the server
docker_image = "test-risc0"

# command to run, will run as: "sh -c command"
command = "/root/risc0-0.17.0/examples/target/release/factors 1>/root/residue/stdout 2>/root/residue/stderr"

[verification]

# Risc0 image_id for the factors examples
image_id = "af1b4fd024acd5f8756263d3c73e66d816a86ca285bb4addc2a3ee8a14bb87c2"

# minimum number of independent successful verifications
min_required = 1

[harvest]

# minimum number of verified traces to make the whole job verified and done
min_verified_traces = 1

</pre>

Save the above content in a file, named `simple_job.toml` and then run it to connect with other nodes and get your job done:
`cargo run -- --job my_awesome_job.toml` <br>

Please note that the sample job file requires the presence of [Server](https://github.com/WholesumNet/server) and [Verifier](https://github.com/WholesumNet/verifier) nodes. 

## Caveat
Please be mindful that as of now the docker image `test-risczero` is not available to the public.
