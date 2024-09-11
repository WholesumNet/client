
# Wholesum network `Client` CLI

## Overview

Wholesum network is a p2p verifiable computing network `tailored for ETH L2 sequencer proving`. It builds on top of [Risc0](https://risczero.com/), [Libp2p](https://libp2p.io), and decentralized storage options like [Swarm](https://ethswarm.org) and Filecoin to facilitate verifiable computing at scale. The design of the network follows a p2p parallel proving scheme where Risc0 jobs are passed around, proved, and finally combined into a final proof ready for L1 verification.

### Prerequisites

You would need to get certain environments ready for the client to function properly.

#### 1- Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

### 2- Lighthouse@Filecoin/Swarmy@Swarm

- Lighthouse:  
  You would need a subscription plan from [Lighthouse](https://docs.lighthouse.storage/lighthouse-1/quick-start) to run the client. Please obtain an api key and specify it with `-d` flag when running the client.
  
- Swarmy:
  Still under development.
  

### 3- Dependencies

To run a client agent, you would first need to fork the following libraries and put them in the parent("..") directory of the client:

- [comms](https://github.com/WholesumNet/comms)
- [dStorage](https://github.com/WholesumNet/dStorage)
- [jocker](https://github.com/WholesumNet/jocker)
- [benchmark](https://github.com/WholesumNet/benchmark)

### The job file
You would need a job file to engage with the network. Here's a sample job file for the SHA example:
<pre>
# schema of a tyipcal job

title = "yet another job"
timeout = 30

# matching criteria
[criteria]

# min memory capacity(in gb)
memory_capacity = 4

# benchmark score/duration(secs)
benchmark_duration_secs = 10000

# benchmark expiry from now(secs)
benchmark_expiry_secs = 864000

[compute]

# docker image run by the server
docker_image = "rezahsnz/r0-sha:1.0.1"

# command to run, will run as: "sh -c command"
command = "/home/prince/sha 1>/home/prince/residue/stdout 2>/home/prince/residue/stderr"

[verification]

# Risc0 image_id of the "sha example"
image_id = "BgXFTkDBZcx8VYUJJvRcU1jQDsW9SZddNgFQEdSUwGXihuhhd" 

# minimum number of independent successful verifications
min_required = 1

[harvest]

# minimum number of verified traces to make the whole job verified and done
min_verified_traces = 1
</pre>

Save the above content to a file named `simple_job.toml`, and feed it to the CLI with the `-j` flag.

## USAGE

<pre>
Wholesum is a P2P verifiable computing marketplace and this program is a CLI for client nodes.

Usage: client [OPTIONS]

Options:
  -d, --dstorage-key-file <DSTORAGE_KEY_FILE>  
  -j, --job <JOB>                              
      --dev                                    
  -k, --key-file <KEY_FILE>                    
  -h, --help                                   Print help
  -V, --version                                Print version

</pre>
