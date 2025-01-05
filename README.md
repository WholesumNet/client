
# Wholesum network `Client` CLI

## Overview

Wholesum network is a p2p verifiable computing network `tailored for ETH L2 sequencer proving`. It builds on top of [Risc0](https://risczero.com/), [Libp2p](https://libp2p.io), and decentralized storage options like [Swarm](https://ethswarm.org) and Filecoin to facilitate verifiable computing at scale. The design of the network follows a p2p parallel proving scheme where Risc0 jobs are passed around, proved, and finally combined into a final proof ready for L1 verification.

### Prerequisites

You would need to get certain environments ready for the client to function properly.

#### Risc0 

To install Risc0, please follow the following [guide](https://github.com/risc0/risc0?tab=readme-ov-file#getting-started).

#### Docker

Docker runtime is needed as it is used to run `Risc0` containers. This awesome [guide](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04) from DigitalOcean is helpful in this regard.

#### MongoDB

Install the MongoDB from [here](https://www.mongodb.com/docs/manual/tutorial/install-mongodb-community-with-docker/). Make sure a Docker container runs and is listenning on `localhost:27017`

#### Decentralized storage

- Lighthouse:  
  You would need a subscription plan from [Lighthouse](https://docs.lighthouse.storage/lighthouse-1/quick-start). Please obtain an API key, put it into a file with the following look:

  <pre>
    apiKey = 'your key'
  </pre>
  
- Swarm:
  Still under develepment.
  

### Library dependencies

To run a client agent, you would first need to fork the following libraries and put them in the parent("..") directory of the client:

- [comms](https://github.com/WholesumNet/comms)
- [dStorage](https://github.com/WholesumNet/dStorage)
- [jocker](https://github.com/WholesumNet/jocker)

### The job file

You would need a job file to engage with the network. Here's a sample job file for the SHA example:
<pre>
# schema of a tyipcal job

[prove]

# segments' cid
segments_cid = "bafybeigzlwqexnsbr2euhpgcivtx3ak7dejud6tg7dyiyja522qscdsovi"

#po2 limit
po2 = 19

# number of segments
num_segments = 5

[verification]

journal_file_path = "/foo/journal"

# Risc0 image_id of the "sha example"
image_id = "f3877c67f872cd6225e9d6038a5b7af0de2c3b5f3b2f27f76a8b09e2230a4f5c"
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
  -d, --dstorage-key-file &lt;DSTORAGE_KEY_FILE&gt;  
      --dev                                    
  -k, --key-file &lt;KEY_FILE&gt;                    
  -h, --help                                   Print help
  -V, --version                                Print version
</pre>
