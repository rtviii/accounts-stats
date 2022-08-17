#![allow(unused_variables)]
#![allow(unused_imports)]
use clap::Parser;
use crossbeam::thread::{self, Scope};
use log::{info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::{self, BaseConsumer};
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::ThreadedProducer;
use rdkafka::producer::{DeliveryResult, ProducerContext};
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use sb3_sqlite::{
    block_ops, create_statistics_tables, insert_block_stat, merge_btree_maps, merge_hmaps,
    process_tx, upsert_account, AccountProfile,
};

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::process::exit;

use std::{
    sync::mpsc::{self, Sender},
    time::Duration,
};

const BATCH_SIZE:usize = 20;
pub fn parse_tuple(tup: &str) -> Result<(u64, u64), std::string::ParseError> {
    let tup = tup.replace("(", "");
    let tup = tup.replace(")", "");
    let startend: Vec<_> = tup
        .split(",")
        .into_iter()
        .map(|x| x.parse::<u64>().unwrap())
        .collect();
    Ok((startend[0], startend[1]))
}

#[derive(Debug, Parser)]
#[clap(author, version, long_about=None)]
pub struct Args {
    #[clap(takes_value = false, long)]
    producer: bool,

    #[clap(takes_value = false, long)]
    consumer: bool,

    #[clap(short = 'g', long)]
    consumer_group: Option<String>,

    #[clap(short = 'p', long)]
    broker_port: usize,

    #[clap(short = 't', long)]
    topic_name: String,

    #[clap(short, long)]
    output_db: String,

    #[clap(long)]
    n_threads: u64,

    #[clap( long , short='s', value_parser=parse_tuple)]
    start_end: (u64, u64),
}

pub fn baseconsumer_init(
    port: usize,
    consumer_group: &str,
    offset_range: &[u64],
    topic: &str,
) -> Result<BaseConsumer, KafkaError> {
    let bconsumer = {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", format!("127.0.0.1:{}", port))
            .set("enable.partition.eof", "false")
            .set("socket.send.buffer.bytes", "5048576")
            .set("socket.receive.buffer.bytes", "5048576")
            .set("queued.max.messages.kbytes", "2096151")
            .set("session.timeout.ms", "60000")
            .set("enable.auto.commit", "true")
            .set("fetch.wait.max.ms", "10000")
            // .set("batch.num.messages", "1")
            .set("group.id", consumer_group)
            .create()
            .expect("Failed to create consumer");
        c
    };

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, rdkafka::Offset::Offset(offset_range[0] as i64))?;
    bconsumer.assign(&tpl)?;
    Ok(bconsumer)
}

fn main() {
    let args       = Args::parse();
    let flag_cons  = args.consumer;
    let flag_prod  = args.producer;
    let port       = &args.broker_port;
    let topic_name = &args.topic_name;
    let nthreads   = args.n_threads;
    let outputdb   = args.output_db;
    let startend   = args.start_end;
    let timer      = timer::Timer::new();

    let (mpsc_sx, mpsc_rx) = mpsc::channel::<(
        BTreeMap<String, AccountProfile>,
        HashMap<(String, u64), u64>,
    )>();

    // -------------------------------------------------------------------------------
    // create n_threads equal chunks ranging from start_end[0] to start_end[1]
    let offsets = (startend.0..startend.1).collect::<Vec<u64>>();
    let chunks = offsets.chunks(offsets.len() / nthreads as usize);
    let consumer_group = &args
        .consumer_group
        .unwrap_or("default_consumer_group".to_string());
    // -------------------------------------------------------------------------------
    let (mpsc_send, mpsc_receive) = mpsc::channel();

    let _ = thread::scope(|s| -> Result<(), KafkaError> {
        s.spawn(move |_| {
            let dbconn = create_statistics_tables(&outputdb)
                .expect(&format!("Could not created sqlite file {}.", &outputdb));
            loop {
                match mpsc_receive.recv() {
                    Ok(msg) => {
                        let (accounts_map, blockstats_map): (
                            BTreeMap<String, AccountProfile>,
                            HashMap<(String, u64), u64>,
                        ) = msg;

                        for (address, profile) in accounts_map.iter() {
                            upsert_account(&dbconn, &address, &profile).unwrap();
                        }

                        for ((bhash, bheight), txcount) in blockstats_map.iter() {
                            insert_block_stat(&dbconn, bhash, bheight, txcount).unwrap();
                        }
                    }
                    Err(e) => {
                        println!("{}", e);
                        std::thread::sleep(Duration::from_secs(2));
                    }
                }
            }
        });

        for chunk in chunks {
            let bconsumer = baseconsumer_init(*port, consumer_group, chunk, &topic_name)?;
            println!("Watermarks : {:?}", bconsumer.fetch_watermarks(&topic_name, 0, Duration::from_secs(10))?);
            let _ = spawn_consumer_thread_in_scope(
                bconsumer,
                chunk[chunk.len() - 1],
                s,
                mpsc_send.clone(),
            );
        }
        Ok(())
    })
    .unwrap();

}

pub fn threcho(msg: &str) {
    println!("[{:?}]: {}", std::thread::current().id(), msg);
}
pub enum BlockProcessingError {
    KafkaError(),
    SerdeError(),
    SqliteError(),
    OtherError(),
}

// fn count_tx__ix_per_tx(transactions: &Vec<Value>) -> (usize, f64) {
//     let (txnum, mut ix_num) = (transactions.len(), 0);
//     for tx in transactions.iter() {
//         let tx_ixs = tx["transaction"]["message"]["instructions"]
//             .as_array()
//             .unwrap();
//         ix_num += tx_ixs.len() as usize;
//     }
//     (txnum, ix_num as f64 / txnum as f64)
// }
fn count_tx(transactions: &Vec<Value>) -> (usize, f64) {
    let (txnum, mut ix_num) = (transactions.len(), 0);
    for tx in transactions.iter() {
        let tx_ixs = tx["transaction"]["message"]["instructions"]
            .as_array()
            .unwrap();
        ix_num += tx_ixs.len() as usize;
    }
    (txnum, ix_num as f64 / txnum as f64)
}

#[derive(Debug)]
pub struct BlockStatsRow {
    blockhash: String,
    blockheight: u64,
    txnum: u64,
}
pub fn block_extract_statistics(
    block: Value,
) -> Result<(BTreeMap<String, AccountProfile>, BlockStatsRow), BlockProcessingError> {
    let mut block_map = BTreeMap::new();
    let transactions = block["transactions"]
        .as_array()
        .expect("Didn't find transactions");
    let blockheight = block["blockHeight"]
        .as_u64()
        .expect("Didn't find blockheight");
    let blockhash = (block["blockhash"])
        .as_str()
        .expect("Didn't find blockhhash")
        .to_string();
    let tx = transactions.len();

    for tx in transactions.iter() {
        let _ = process_tx(&tx["transaction"], &mut block_map);
    }
    Ok((
        block_map,
        BlockStatsRow {
            blockhash,
            blockheight: blockheight,
            txnum: tx as u64,
        },
    ))
}

pub fn spawn_consumer_thread_in_scope<'a>(
    consumer       : BaseConsumer,
    halt_at_offset : u64,
    crossbeam_scope: &Scope<'a>,
    send_to_master: Sender<(
        BTreeMap<String, AccountProfile>,
        HashMap<(String, u64), u64>,
    )>,
) -> Result<(), KafkaError> {
    crossbeam_scope.spawn(move |_| -> Result<(), BlockProcessingError> {

        println!("Spawned consumer with halt_at_offset {:?}" , halt_at_offset);

        let mut threadwide_account_stats: BTreeMap<String,      AccountProfile> = BTreeMap::new();
        let mut threadwide_blocks_stats: HashMap<(String, u64), u64>            = HashMap::new();
        let mut processed_blocks                               = 1;

        
        loop {
            threcho("Polling.");
            match consumer.poll(Duration::from_millis(10000)) {
                
                Some(m) => {
                    println!("Got Some");
                    let message = match m {
                        Ok(bm) => {

                            let parsedval: Value =
                                serde_json::from_slice::<Value>(&bm.payload().unwrap()).unwrap();
                                println!("Got block {:?}", parsedval);
                            let (accounts_stats, block_stats_row) =
                                block_extract_statistics(parsedval)?;
                            threadwide_account_stats =
                                merge_btree_maps(threadwide_account_stats, accounts_stats);
                            threadwide_blocks_stats.insert(
                                (block_stats_row.blockhash, block_stats_row.blockheight),
                                block_stats_row.txnum,
                            );
                            processed_blocks += 1;
                            println!(
                                "[{:?}] Processed blocks {}",
                                std::thread::current().id(),
                                processed_blocks
                            );
                            bm
                        }
                        Err(e) => {
                            
                            panic!("Message receive error! {}", e);
                        }
                    };
                    if message.offset() == halt_at_offset as i64 {
                        println!("Consumer reached halt offset {}", halt_at_offset);
                        break;
                    }
                }
                None => {
                    println!(".");
                }
            }

            if processed_blocks % BATCH_SIZE == 0 {
                send_to_master
                    .send((threadwide_account_stats, threadwide_blocks_stats))
                    .unwrap();
                println!(
                    "[{:?}] Processed {} blocks. Sending batch to master.",
                    std::thread::current().id(),
                    BATCH_SIZE
                );
                threadwide_account_stats = BTreeMap::new();
                threadwide_blocks_stats = HashMap::new();
                processed_blocks = 1;
            }
        }
        Ok(())
    });
    Ok(())
}
