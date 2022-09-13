# raptor-simulation

```bash
$ raptorq_simulation -h
nsim 0.1.0

UDP network simulation using RaptorQ for erasure chunks

USAGE:
    raptorq_simulation [OPTIONS]

OPTIONS:
    -b, --num-batches <num-batches>
            number of batches to send [default: 1]

        --batch-parallelism <batch-parallelism>
            number of batches to send in parallel [default: 1]

        --erasure-count <erasure-count>
            number of erasure packets [default: 3000]

    -f, --file <file>
            File name to get data from

    -h, --help
            Print help information


    --num-packet-blast <num-packet-blast>
            number of packets sender should send at once [default: 32]

    -p, --listening-port <listening-port>
            UDP port on which receiver listens on packets [default: 19845]

    -s, --send
            Send raw bytes from file

    -V, --version
            Print version information
```

The application works in pairs:
- Sender
- Receiver

## Sender

Start the app with `-s` or `--send`.

Sender _must_ provide the payload (e.g. `--file transactions.json.txt`) which would be transmitted to all receivers.

```bash
target/release/raptorq_simulation -p 1234 -s -f src/transactions.json  --num-batches 3 --batch-parallelism 2 --erasure-count 3000 --num-packet-blast 40
```

## Receiver

Without any params the app would start as a receiver and listen for UDP packets on port 19845 default.

```bash
target/release/raptorq_simulation -p 1235
target/release/raptorq_simulation -p 1236

```

Receivers forward received packets to each other (except self). 
On successfully receiving all packets for each batch, they reassemble the file and store it under the current directory. [batch_id].BATCH being the file names.
Each of these .BATCH files should be identical to transactions.json


## NOTES
* linear encoding - we can do this on the fly into an accumulator as each packet is received. we do not need to wait until we have all the data
* raptor codes doesn't care about ordering of packets
* also using raptor codes  makes it easier since we don't need to reassemble the file manually first - it's actually easier to encode and decode as opposed to buffering all the packets and then constructing the file
* RaptorQ encoder configuration information need to be propagated over to committee of nodes for building Decoder configuration

