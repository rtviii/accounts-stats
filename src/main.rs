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
use sb3_sqlite::{create_statistics_tables, insert_block_stat, upsert_account, block_ops,merge_btree_maps,merge_hmaps, AccountProfile, process_tx};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::process::exit;

use std::{
    sync::mpsc::{self, Sender},
    time::Duration,
};

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
            .set("session.timeout.ms", "60000")
            .set("enable.auto.commit", "true")
            .set("fetch.wait.max.ms", "1000")
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
    // let workload       = &args.process_n_messages;
    let startend = args.start_end;
    let timer = timer::Timer::new();


    // TODO: Implement timer and associated stats 
    // timer.schedule_repeating(chrono::Duration::seconds(2), move || {
    //     let mut totalbytes = 0;
    //     loop {
    //         match rx.recv(){
    //             Ok(message)=>{
    //                 totalbytes += message;
    //             },
    //             Err(e)=>{
    //                 println!("Timer failed")
    //             }
    //         }
    //     }
    // });


    let ( mpsc_sx, mpsc_rx )= mpsc::channel::<(BTreeMap<String, AccountProfile>,HashMap<( String,u64 ), (u64,f64)> )>();
    let dbconn = create_statistics_tables(&outputdb).expect(&format!("Could not created sqlite file {}.", &outputdb));
    // -------------------------------------------------------------------------------
    // create n_threads equal chunks ranging from start_end[0] to start_end[1]
    let offsets = (startend.0..startend.1).collect::<Vec<u64>>();
    let chunks  = offsets.chunks(offsets.len() / nthreads as usize);
    let consumer_group = &args
        .consumer_group
        .unwrap_or("default_consumer_group".to_string());

    let (tx, rx) = mpsc::channel();
    let _ = thread::scope(|s| -> Result<(), KafkaError> {
        for chunk in chunks {
            let bconsumer = baseconsumer_init(*port, consumer_group, chunk, &topic_name)?;
            let _ = spawn_consumer_thread_in_scope(
                bconsumer,
                chunk[chunk.len()-1],
                s,
                tx.clone()
            );
        }
        Ok(())
    }).unwrap();

    // -------------------------------------------------------------------------------
    // loop {
    //     match rx.recv() {
    //         Ok(msg) => {
    //             threcho(&format!("Got {}", msg));
    //         }
    //         Err(e) => {
    //             println!("{}", e);
    //             std::thread::sleep(Duration::from_secs(2));
    //         }
    //     }
    // }

    // -------------------------------------------------------------------------------
}



pub fn threcho(msg: &str) {
    println!("[{:?}]: {}", std::thread::current().id(), msg);
}


pub enum BlockProcessingError{
    KafkaError(),
    SerdeError(),
    SqliteError(),
    OtherError(),
}

fn count_tx__ix_per_tx(transactions:&Vec<Value>)->( usize,f64 ){
    let ( txnum, mut ix_num )      = (transactions.len(),0);
    for tx in transactions.iter() {
        let tx_ixs = tx["transaction"]["message"]["instructions"].as_array().unwrap();
        ix_num += tx_ixs.len() as usize;
    }
    (txnum,  ix_num as f64 /txnum as f64 )
}


#[derive(Debug)]
pub struct BlockStatsRow{
    blockhash  : String,
    blockheight: u64,
    txnum      : u64,
    ixpertx    : f64,
}
pub fn block_extract_statistics(
    block             : Value
)->Result<(BTreeMap<String,AccountProfile>,BlockStatsRow), BlockProcessingError>{
    let mut block_map = BTreeMap::new();
    let transactions  = block["transactions"].as_array().expect("Didn't find transactions");
    let blockheight   = block["blockHeight"].as_u64().expect("Didn't find blockheight");
    let blockhash     = (block["blockhash"]).as_str().expect("Didn't find blockhhash").to_string();
    let (tx,ixpertx)  = count_tx__ix_per_tx(transactions);
    for tx in transactions.iter() {
        let _ = process_tx(&tx["transaction"], &mut block_map);
    }
    Ok((block_map, BlockStatsRow{blockhash, blockheight: blockheight, txnum: tx as u64, ixpertx: ixpertx}))
}


pub fn spawn_consumer_thread_in_scope<'a>(
    consumer       : BaseConsumer,
    halt_at_offset : u64,
    crossbeam_scope: &Scope<'a>,
    send_to_master: Sender<i64>,
) -> Result<(), KafkaError> {
    crossbeam_scope.spawn(move |_| -> Result<(), BlockProcessingError> {

        let mut per_thread_map:BTreeMap<String,      AccountProfile> = BTreeMap::new();
        let mut blocks_stats:  HashMap<( String,u64 ), (u64,f64)>      = HashMap::new();

        loop {
            match consumer.poll(Duration::from_millis(2000)) {
                Some(m) => {
                    let message = match m {
                        Ok (bm) =>{
                            let parsedval:Value = serde_json::from_slice::<Value>(&bm.payload().unwrap()).unwrap();
                            // println!("{:?}", parsedval);
                            block_extract_statistics(parsedval)?;
                            bm
                        },
                        Err(e ) =>{panic!("Message receive error! {}", e);}
                    };

                    let off = message.offset();
                    // println!("{:?}", off);
                    if off == halt_at_offset as i64{
                        println!("Consumer reached halt offset {}", off);
                        break;
                    }
                    let sz  = std::mem::size_of_val(message.payload().unwrap() );
                    println!("Thread {:?} message with offset :{:?} size = {:?}", std::thread::current().id(),off,sz);
                    send_to_master.send(off).unwrap();
                }
                None => {
                    println!("No message.")
                }
            }
        }
        println!("Exited loop");
        Ok(())
    });
    println!("returned thread");
    Ok(())
}

// struct ProducerLogger {}
// impl ClientContext for ProducerLogger {}
// impl ProducerContext for ProducerLogger {
//     type DeliveryOpaque = ();
//     fn delivery(
//         &self,
//         delivery_result: &DeliveryResult<'_>,
//         delivery_opaque: Self::DeliveryOpaque,
//     ) {
//         let delivery_res = delivery_result.as_ref();
//         match delivery_res {
//             Ok(message) => {
//                 let key: &str = message.key_view().unwrap().unwrap();
//                 println!(
//                     "Produced message {} successfully. Offset: {}. Partition :{}",
//                     key,
//                     message.offset(),
//                     message.partition()
//                 )
//                 // This would probably be a good place to commit offsets
//             }

//             Err(producer_err) => {
//                 let key: &str = producer_err.1.key_view().unwrap().unwrap();
//                 println!(
//                     "Failed to produce message {}. Error: {}",
//                     key, producer_err.0
//                 )
//             }
//         }
//     }
// }
// pub fn producer() {
//     let producer: ThreadedProducer<ProducerLogger> = ClientConfig::new()
//         .set("bootstrap.servers", "localhost:9095")
//         .create_with_context(ProducerLogger {})
//         .expect("Failed to create producer");
// }

// pub fn consumer() {
//     let mut baseconsumer: BaseConsumer = ClientConfig::new()
//         .set("bootstrap.servers", "127.0.0.1:9095")
//         // .set("enable.auto.commit", "false")
//         .set("group.id", "rt0")
//         .create()
//         .expect("Failed to create consumer");
// }
