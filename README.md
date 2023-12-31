
# Wholesum network `Client` CLI

## Overview

Wholesum network is a p2p verifiable computing network. It builds on top of [Risc0](https://risczero.com/), [Swarm](https://ethswarm.org), [FairOS-dfs](https://github.com/fairDataSociety/fairOS-dfs), and [Libp2p](https://libp2p.io) to facilitate verifiable computing at scale.  

## How to run

To run a client agent, you would first need to fork the [comms](https://github.com/WholesumNet/comms) library and put it in the parent("..") directory of the client directory. Next, you would need a job file, in case you were interested to run a tiny job, with the following look in TOML:

<pre>
# schema of a tyipcal job

title = "yet another job"
timeout = 30

[compute]

# min ram capacity(in GB) for an offer to be accepted
min_memory_capacity = 4

# docker image run by the server
docker_image = "rezahsnz/risc0-sha"

# command to run, will run as: "sh -c command"
command = "/home/prince/sha 1>/home/prince/residue/stdout 2>/home/prince/residue/stderr"

[verification]

# Risc0 image_id of the "sha example"
image_id = "c0e787dad721d314cc4823f2e0cd5e7caa405575cc55947217bd5663c076ad6a"

# minimum number of independent successful verifications
min_required = 1

[harvest]

# minimum number of verified traces to make the whole job verified and done
min_verified_traces = 1
</pre>

Save the above content to a file named `simple_job.toml`, and then run the client  get your job done:<br>
`cargo run -- --job simple_job.toml`

Please note that the sample job file requires the presence of [Server](https://github.com/WholesumNet/server) and [Verifier](https://github.com/WholesumNet/verifier) nodes over the network which at moment is local only. 

## USAGE

<pre>
Usage: client [OPTIONS]

Options:
  -j, --job <JOB>  
  -h, --help       Print help
  -V, --version    Print version
</pre>
