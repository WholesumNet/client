
# Wholesum network `Client` CLI

## Overview

Wholesum is a p2p prover network `tailored for ETH L1 block proving`. It builds on top of [Risc0](https://risczero.com/) and [Libp2p](https://libp2p.io). The design of the network follows a p2p distributed proving scheme where Risc0 jobs are passed around, proved, and finally combined into a final Groth16 proof ready for L1 verification.

### Prerequisites

You would need to get certain environments ready for the client to function properly.

#### Risc0 

To install Risc0, please follow the following [guide](https://github.com/risc0/risc0?tab=readme-ov-file#getting-started).

#### Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

#### MongoDB

Install the MongoDB from [here](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-community-with-docker/). Make sure a Docker container runs and is listenning on `localhost:27017`

### Library dependencies

To run a client agent, you would first need to fork the following libraries and put them in the parent("..") directory of the client:

- [peyk](https://github.com/WholesumNet/peyk)

### The job file

You would need a job file to engage with the network. Here's a sample job file for the SHA example:
<pre>
# schema of a typical L1 block proving job

redis_url = "redis://127.0.0.1"

image_id = "foobarbaz" 

</pre>

Save the above content to a file named `simple_job.toml`, and feed it to the CLI with the `-j` flag.

## USAGE

<pre>
Wholesum is a P2P verifiable computing marketplace and this program is a CLI for client nodes.

Usage: client [OPTIONS] [COMMAND]

Commands:
  new     Start a new job
  resume  Resume the job
  help    Print this message or the help of the given subcommand(s)

Options:
      --dev                  
  -k, --key-file <KEY_FILE>  
  -h, --help                 Print help
  -V, --version              Print version
</pre>
