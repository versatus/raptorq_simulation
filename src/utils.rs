use crate::BATCH_ID_SIZE;
use crossbeam_channel::{Receiver, Sender};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng, RngCore};
use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, str};

/// Maximum over-the-wire size of a Transaction
///   1280 is IPv6 minimum MTU
///   40 bytes is the size of the IPv6 header
///   8 bytes is the size of the fragment header

pub const MTU_SIZE: usize = 1280;

const PACKET_SNO: usize = 4;

const FLAGS: usize = 1;

///   40 bytes is the size of the IPv6 header
///   8 bytes is the size of the fragment header
const PAYLOAD_SIZE: usize = MTU_SIZE - PACKET_SNO - BATCH_ID_SIZE - FLAGS - 40 - 8;

/// It takes a batch id, a sequence number, and a payload, and returns a packet
///
/// Arguments:
///
/// * `batch_id`: This is the batch id that we're sending.
/// * `payload`: the data to be sent
///
/// Returns:
///
/// A vector of bytes
pub fn create_packet(batch_id: [u8; BATCH_ID_SIZE], payload: Vec<u8>) -> Vec<u8> {
    let mut mtu: Vec<u8> = vec![];

    // empty byte for raptor coding length
    // doing the plus one since raptor is returning minus 1 length.

    mtu.push(0_u8);
    // forward-flag at the beginning
    mtu.push(1_u8);

    for i in 0..BATCH_ID_SIZE {
        mtu.push(batch_id[i]);
    }
    mtu.extend_from_slice(&payload);

    mtu
}

/// `split_into_packets` takes a `full_list` of bytes, a `batch_id` and an `erasure_count` and returns a
/// `Vec<Vec<u8>>` of packets
///
/// Arguments:
///
/// * `full_list`: The list of bytes to be split into packets
/// * `batch_id`: This is a unique identifier for the batch of packets.
/// * `erasure_count`: The number of packets that can be lost and still be able to recover the original data.
pub fn split_into_packets(
    full_list: &[u8],
    batch_id: [u8; BATCH_ID_SIZE],
    erasure_count: u32,
) -> Vec<Vec<u8>> {
    let packet_holder = encode_into_packets(full_list, erasure_count);

    let mut headered_packets: Vec<Vec<u8>> = vec![];
    for (_, ep) in packet_holder.into_iter().enumerate() {
        headered_packets.push(create_packet(batch_id, ep))
    }
    println!("Packets len {:?}", headered_packets.len());
    headered_packets
}

/// It takes a list of bytes and an erasure count, and returns a list of packets
///
/// Arguments:
///
/// * `unencoded_packet_list`: This is the list of packets that we want to encode.
/// * `erasure_count`: The number of packets that can be lost and still be able to recover the original
/// data.
///
/// Returns:
///
/// A vector of vectors of bytes.
pub fn encode_into_packets(unencoded_packet_list: &[u8], erasure_count: u32) -> Vec<Vec<u8>> {
    let encoder = Encoder::with_defaults(unencoded_packet_list, (PAYLOAD_SIZE) as u16);
    let packets: Vec<Vec<u8>> = encoder
        .get_encoded_packets(erasure_count)
        .iter()
        .map(|packet| packet.serialize())
        .collect();

    //Have to be transmitter to peers
    //println!("encoder config {:?}", encoder.get_config());
    println!("Packet size after raptor: {}", packets[0].len());
    packets
}

/// It takes a packet and returns the batch id
///
/// Arguments:
///
/// * `packet`: The packet that we want to extract the batch id from.
///
/// Returns:
///
/// The batch_id is being returned.
pub fn get_batch_id(packet: &[u8; 1280]) -> [u8; BATCH_ID_SIZE] {
    let mut batch_id: [u8; BATCH_ID_SIZE] = [0; BATCH_ID_SIZE];
    let mut chunk_no: usize = 0;
    for i in 2..(BATCH_ID_SIZE + 2) {
        batch_id[chunk_no] = packet[i];
        chunk_no += 1;
    }
    batch_id
}

/// > Generate a random 46 byte batch id
pub fn generate_46b_batch_id() -> [u8; BATCH_ID_SIZE] {
    let mut x = [0_u8; BATCH_ID_SIZE];
    thread_rng().fill_bytes(&mut x);
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(BATCH_ID_SIZE)
        .map(char::from)
        .collect();
    x.copy_from_slice(s.as_bytes());
    x
}

/// It reads the contents of a file into a byte array
///
/// Arguments:
///
/// * `file_path`: The path to the file you want to read.
pub fn read_file(file_path: PathBuf) -> Vec<u8> {
    let metadata = fs::metadata(&file_path).expect("unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];
    File::open(file_path)
        .unwrap()
        .read_exact(&mut buffer)
        .expect("buffer overflow");
    buffer
}

/// It receives a tuple of a string and a vector of bytes from a channel, and writes the vector of bytes
/// to a file whose name is the string
///
/// Arguments:
///
/// * `file_recv`: Receiver<(String, Vec<u8>)>
pub fn file_writer(file_recv: Receiver<(String, Vec<u8>)>) {
    loop {
        match file_recv.recv() {
            Ok((batch_id, contents)) => {
                let batch_fname = format!("{}.BATCH", batch_id);
                fs::write(batch_fname, &contents).unwrap();
            }
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };
    }
}

/// It receives packets from the `receiver` channel, checks if the packet is a duplicate, and if not, it
/// checks if the packet is a forwarder packet. If it is, it forwards the packet to the `forwarder`
/// channel. If it is not, it checks if the packet is a new batch. If it is, it creates a new decoder
/// for the batch. If it is not, it adds the packet to the decoder. If the decoder is complete, it sends
/// the decoded file to the `file_send` channel
///
/// Arguments:
///
/// * `receiver`: Receiver<([u8; 1280], usize)>
/// * `batch_id_hashset`: A hashset that contains the batch_ids of all the batches that have been
/// reassembled.
/// * `decoder_hash`: A hashmap that stores the batch_id as the key and a tuple of the number of packets
/// received and the decoder as the value.
/// * `forwarder`: Sender<Vec<u8>>
/// * `file_send`: Sender<(String, Vec<u8>)>
pub fn reassemble_packets(
    receiver: Receiver<([u8; 1280], usize)>,
    batch_id_hashset: &mut HashSet<[u8; BATCH_ID_SIZE]>,
    decoder_hash: &mut HashMap<[u8; BATCH_ID_SIZE], (usize, Decoder)>,
    forwarder: Sender<Vec<u8>>,
    file_send: Sender<(String, Vec<u8>)>,
) {
    loop {
        let mut received_packet = match receiver.recv() {
            Ok(pr) => pr,
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        let batch_id = get_batch_id(&received_packet.0);
        if batch_id_hashset.contains(&batch_id) {
            continue;
        }

        // This is to check if the packet is a forwarder packet. If it is, it forwards the packet to the `forwarder` channel.
        // Since packet is shared across nodes with forward flag as 1
        let forward_flag = received_packet.0.get_mut(1).unwrap();
        if *forward_flag == 1 {
            *forward_flag = 0;
            let _ = forwarder.try_send(received_packet.0[0..received_packet.1].to_vec());
        }

        match decoder_hash.get_mut(&batch_id) {
            Some((num_packets, decoder)) => {
                *num_packets += 1;
                // Decoding the packet.
                let result = decoder.decode(EncodingPacket::deserialize(
                    &received_packet.0[48_usize..received_packet.1].to_vec(),
                ));
                if !result.is_none() {
                    batch_id_hashset.insert(batch_id);

                    println!(
                        "Batch: {}: Generating reassembled file: {:?}: Number of packets received: {}",
                        str::from_utf8(&batch_id).unwrap(),
                        SystemTime::now().duration_since(UNIX_EPOCH).unwrap(), *num_packets
                    );
                    // This is the part of the code that is sending the reassembled file to the `file_send` channel.
                    let result_bytes = result.unwrap();
                    let batch_id_str = String::from(str::from_utf8(&batch_id).unwrap());
                    let msg = (batch_id_str, result_bytes);
                    let _ = file_send.send(msg);
                    decoder_hash.remove(&batch_id);
                }
            }
            None => {
                // This is creating a new decoder for a new batch.
                decoder_hash.insert(
                    batch_id,
                    (
                        1_usize,
                        Decoder::new(ObjectTransmissionInformation::new(7038895, 1176, 1, 1, 8)),
                    ),
                );
            }
        }
    }
}
