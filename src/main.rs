mod utils;
use std::collections::{HashMap, HashSet};
use std::{io, str, sync::Arc, thread};

use anyhow::Result;
use clap::Parser;
use crossbeam_channel::{unbounded, Receiver};
//use futures::executor::ThreadPool;
use crate::utils::{file_writer, generate_46b_batch_id, read_file, reassemble_packets, MTU_SIZE};
use futures::future::try_join_all;
use futures::prelude::*;
use raptorq::Decoder;
use serde::{Deserialize, Serialize};
use std::borrow::{Borrow, BorrowMut};
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use threadpool::ThreadPool;
use tokio::net::UdpSocket;

use futures::stream::FuturesUnordered;

use crate::utils::split_into_packets;

// using random 46 bytes - lets say ipfs Hash
const BATCH_ID_SIZE: usize = 46;

//How many packets to recieve from socket in single system call
pub const NUM_RCVMMSGS: usize = 32;

#[tokio::main]
async fn broadcast_to_peers(
    batch_id: [u8; 46],
    receivers: Vec<NodeAddress>,
    packet_list: Vec<Vec<u8>>,
    num_packet_blast: usize,
) -> Result<()> {
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
    let mut futs = FuturesUnordered::new();
    println!("Packet list {}", packet_list.len());
    let now = SystemTime::now();
    for (packet_index, packet) in packet_list.iter().enumerate() {
        // Sharding/Distribution of packets as per no of nodes
        let address = receivers[packet_index % receivers.len()].clone();
        let packet = packet.clone();
        let sock = udp_socket.clone();

        // Sending a packet to a given address.
        futs.push(tokio::spawn(async move {
            let _ = sock
                .send_to(&packet, (&address.ip_addr[..], address.port))
                .await;
        }));

        if futs.len() >= num_packet_blast {
            let _ = futs.next().await.unwrap();
        }
    }

    while (futs.next().await).is_some() {}

    println!(
        "Batch: {}: Packets sent to recipients: {:?}",
        str::from_utf8(&batch_id).unwrap(),
        SystemTime::now().duration_since(now).unwrap()
    );

    Ok(())
}

/// It takes a file, splits it into chunks, encodes the chunks, and sends the encoded chunks to the
/// receiver nodes
///
/// Arguments:
///
/// * `filename`: Path to the file to be sent
/// * `number_of_chunks`: The number of packets to be sent to the receiver nodes.
/// * `receivers`: A list of nodes to send the file to.
/// * `num_batches`: Number of batches to send
/// * `batch_parallelism`: The number of batches to send in parallel.
/// * `erasure_count`: This is the number of packets that will be sent to each receiver.
/// * `num_packet_blast`: This is the number of packets that will be sent to each receiver node in a
/// single UDP packet.
///
/// Returns:
///
/// a Result<(), io::Error>
async fn broadcast_file_in_chunks_to_peers(
    filename: PathBuf,
    receivers: Vec<NodeAddress>,
    num_batches: usize,
    batch_parallelism: usize,
    erasure_count: u32,
    num_packet_blast: usize,
) -> io::Result<()> {
    let raw_contents = read_file(filename);
    println!("Bytes in file: {}", raw_contents.len());

    let batch_thread_pool = ThreadPool::new(batch_parallelism);

    for _ in 0..num_batches {
        let batch_id = generate_46b_batch_id();
        let batch_name = str::from_utf8(&batch_id).unwrap();
        println!("Batch ID {:?}", batch_id);
        println!(
            "Batch: {} : Encoding packets: {:?}",
            batch_name,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        );

        let chunked_content = split_into_packets(&raw_contents, batch_id, erasure_count);
        println!(
            "Batch: {} : Length of each packet: {}",
            batch_name,
            chunked_content[1].len()
        );

        let rip = receivers.clone();
        batch_thread_pool.execute(move || {
            let _ = broadcast_to_peers(batch_id, rip, chunked_content, num_packet_blast);
        });

        println!(
            "Batch: {} : Sending packets to recipients: {:?}",
            batch_name,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
        );
    }

    batch_thread_pool.join();

    Ok(())
}

#[tokio::main]
async fn forward_handler(
    forwarder_channel_receive: Receiver<Vec<u8>>,
    nodes_ips_except_self: Vec<NodeAddress>,
) -> Result<()> {
    // let _ = udp_socket.set_nonblocking(true);
    let udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
    loop {
        match forwarder_channel_receive.recv() {
            Ok(packet) => {
                let mut broadcast_futures: Vec<_> = vec![];
                for addr in &nodes_ips_except_self {
                    let address = addr.clone();
                    let pack = packet.clone();
                    let sock = udp_socket.clone();
                    broadcast_futures.push(tokio::task::spawn(async move {
                        sock.send_to(&pack, (&address.ip_addr[..], address.port))
                            .await
                    }))
                }
                let _ = try_join_all(broadcast_futures).await?;
            }
            Err(e) => {
                println!("Error occurred while receiving packet: {:?}", e)
            }
        }
    }
}

async fn process_received_packets(receivers: Vec<NodeAddress>, port: u16) -> io::Result<()> {
    let sock_recv = &UdpSocket::bind(("0.0.0.0", port)).await.unwrap();
    println!("Listening on {}", port);

    let buf = [0; MTU_SIZE];

    let (reassembler_channel_send, reassembler_channel_receive) = unbounded();
    let (forwarder_channel_send, forwarder_channel_receive) = unbounded();
    let (file_creator_send_channel, file_creator_channel_receive) = unbounded();

    let mut batch_id_store: HashSet<[u8; BATCH_ID_SIZE]> = HashSet::new();
    let mut decoder_hash: HashMap<[u8; BATCH_ID_SIZE], (usize, Decoder)> = HashMap::new();

    thread::spawn({
        let assemble_send = reassembler_channel_send.clone();
        let fwd_send = forwarder_channel_send.clone();
        let f_send = file_creator_send_channel.clone();

        move || {
            reassemble_packets(
                reassembler_channel_receive,
                &mut batch_id_store,
                &mut decoder_hash,
                fwd_send.clone(),
                f_send.clone(),
            );
            drop(assemble_send);
            drop(fwd_send);
            drop(f_send);
        }
    });

    thread::spawn(move || file_writer(file_creator_channel_receive));

    let nodes_ips_except_self = receivers
        .iter()
        .filter(|n| n.role == NodeType::Receiver)
        .cloned()
        .collect::<Vec<NodeAddress>>();

    thread::spawn(move || forward_handler(forwarder_channel_receive, nodes_ips_except_self));

    loop {
        let mut receive_buffers = [buf; NUM_RCVMMSGS];

        // Receiving a batch of packets from the socket.
        let res = recv_mmsg(sock_recv, receive_buffers.borrow_mut())
            .await
            .unwrap();

        if res.len() > 0 {
            let mut i = 0;
            for buf in &receive_buffers {
                let packets_info = res.get(i).unwrap();
                let _ = reassembler_channel_send.send((buf.clone(), packets_info.1));
                i = i + 1;
            }
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cmd_options = Arguments::parse();
    let receivers = vec![
        NodeAddress {
            ip_addr: "0.0.0.0".to_string(),
            port: 1235,
            role: NodeType::Receiver,
        },
        NodeAddress {
            ip_addr: "0.0.0.0".to_string(),
            port: 1236,
            role: NodeType::Receiver,
        },
        NodeAddress {
            ip_addr: "0.0.0.0".to_string(),
            port: 1237,
            role: NodeType::Receiver,
        },
    ];

    if cmd_options.send {
        let filename = match cmd_options.input_file {
            None => {
                println!("missing file name argument");
                return Ok(());
            }
            Some(path) => {
                if !Path::new(&path).exists() {
                    println!("{:?} does not exit", path);
                    return Ok(());
                }
                path
            }
        };

        println!(
            "Number of batches: {:?}: batches parallelism: {:?}: erasure chunks: {}: Number of packet blasts: {}: Number of receivers: {}",
            cmd_options.num_batches, cmd_options.batch_parallelism, cmd_options.erasure_count, cmd_options.num_packet_blast, receivers.len()
        );

        broadcast_file_in_chunks_to_peers(
            filename,
            receivers,
            cmd_options.num_batches,
            cmd_options.batch_parallelism,
            cmd_options.erasure_count,
            cmd_options.num_packet_blast,
        )
        .await
    } else {
        process_received_packets(receivers, cmd_options.port).await
    }
}

#[derive(Parser, Debug)]
#[clap(
    name = "raptor_simulation",
    version = "0.1.0",
    about = "Network Simulation Using RaptorQ",
    author = "vrrb"
)]
pub struct Arguments {
    #[clap(short = 's', long = "send", help = "Send raw bytes from file")]
    pub send: bool,

    #[clap(
        value_name = "file",
        short = 'f',
        long = "file",
        help = "File name to get data from",
        parse(from_os_str)
    )]
    pub input_file: Option<PathBuf>,

    #[clap(
        value_name = "listening-port",
        short = 'p',
        long = "listening-port",
        help = "UDP port on which receiver listens on packets",
        default_value_t = 19845
    )]
    pub port: u16,

    #[clap(
        value_name = "num-batches",
        short = 'b',
        long = "num-batches",
        help = "number of batches to send",
        default_value_t = 1
    )]
    pub num_batches: usize,

    #[clap(
        value_name = "batch-parallelism",
        long = "batch-parallelism",
        help = "number of batches to send in parallel",
        default_value_t = 1
    )]
    pub batch_parallelism: usize,

    #[clap(
        value_name = "erasure-count",
        long = "erasure-count",
        help = "number of erasure packets",
        default_value_t = 3000
    )]
    pub erasure_count: u32,

    #[clap(
        value_name = "num-packet-blast",
        long = "num-packet-blast",
        help = "number of packets sender should send at once w/ async",
        default_value_t = 32
    )]
    pub num_packet_blast: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename = "node_list")]
pub(crate) struct NodeAddress {
    /// IP address of the node
    #[serde(rename = "ip")]
    pub ip_addr: String,
    /// Port on the node listening on udp connections
    pub port: u16,
    /// Leader, Normal
    pub role: NodeType,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    Sender,
    Receiver,
}

impl Default for NodeType {
    fn default() -> Self {
        NodeType::Receiver
    }
}

#[derive(Debug)]
pub enum SendPktsError {
    /// IO Error during send: first error, num failed packets
    IoError(io::Error, usize),
}

//Unused (For linux we have to use libc::send_mmsg for writing multiple packets on to socket at at time)
//Not used since this doesnt add any optimization
#[cfg(not(target_os = "linux"))]
pub async fn batch_send<S, T>(sock: &UdpSocket, packets: &[(T, S)]) -> Result<(), SendPktsError>
where
    S: Borrow<SocketAddr>,
    T: AsRef<[u8]>,
{
    let mut num_failed = 0;
    let mut erropt = None;
    for (p, a) in packets {
        if let Err(e) = sock.send_to(p.as_ref(), a.borrow()).await {
            num_failed += 1;
            if erropt.is_none() {
                erropt = Some(e);
            }
        }
    }

    if let Some(err) = erropt {
        Err(SendPktsError::IoError(err, num_failed))
    } else {
        Ok(())
    }
}

//For Linux we can use system call from libc::recv_mmsg
/// It receives a UDP packet from a socket, and
/// returns the index of the packet in the array, the number of bytes received, and the address of the
/// sender
///
/// Arguments:
///
/// * `socket`: The UDP socket to receive from.
/// * `packets`: a mutable array of byte arrays, each of which is the size of the largest packet you
/// want to receive.
///
#[cfg(not(target_os = "linux"))]
pub async fn recv_mmsg(
    socket: &UdpSocket,
    packets: &mut [[u8; 1280]; NUM_RCVMMSGS],
) -> io::Result<Vec<(usize, usize, SocketAddr)>> {
    let mut received = Vec::new();
    let count = std::cmp::min(NUM_RCVMMSGS, packets.len());
    let mut i = 0;
    for packt in packets.iter_mut().take(count) {
        match socket.recv_from(packt).await {
            Err(e) => {
                return Err(e);
            }
            Ok((nrecv, from)) => {
                received.push((i, nrecv, from));
            }
        }
        i += 1;
    }
    Ok(received)
}
