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
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("fetch.wait.max.ms", "100")
            .set("batch.num.messages", "1")
            // .set("enable.auto.commit", "false")
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
    let args = Args::parse();
    let flag_cons = args.consumer;
    let flag_prod = args.producer;
    let port = &args.broker_port;
    let topic_name = &args.topic_name;
    let nthreads = args.n_threads;
    // let workload       = &args.process_n_messages;
    let startend = args.start_end;

    // create n_threads equal chunks ranging from start_end[0] to start_end[1]
    let offsets = (startend.0..startend.1).collect::<Vec<u64>>();
    let chunks = offsets.chunks(offsets.len() / nthreads as usize);

    let consumer_group = &args
        .consumer_group
        .unwrap_or("default_consumer_group".to_string());

    let (tx, rx) = mpsc::channel();

    let _ = thread::scope(|s| -> Result<(), KafkaError> {
        for c in chunks {
            let bconsumer = baseconsumer_init(*port, consumer_group, c, &topic_name)?;
            let _ = spawn_consumer_in_scope(
                bconsumer,
                c[c.len()-1],
                s,
                tx.clone()
            );
        }
        Ok(())
    })
    .unwrap();

    loop {
        match rx.recv() {
            Ok(msg) => {
                threcho(&format!("Got {}", msg));
            }
            Err(e) => {
                println!("{}", e);
                std::thread::sleep(Duration::from_secs(2));
            }
        }
    }
}

pub fn threcho(msg: &str) {
    println!("[{:?}]: {}", std::thread::current().id(), msg);
}

pub fn spawn_consumer_in_scope<'a>(
    consumer: BaseConsumer,
    halt_at_offset:u64,
    crossbeam_scope: &Scope<'a>,
    send_to_master: Sender<i64>,
) -> Result<(), KafkaError> {
    crossbeam_scope.spawn(move |_| -> Result<(), KafkaError> {
        loop {
            match consumer.poll(Duration::from_millis(1000)) {
                Some(m) => {
                    let message = m?;
                    let off = message.offset();
                    let sz = std::mem::size_of_val(message.payload().unwrap() );
                    println!("Thread {:?} message with offset :{:?} size = {:?}", std::thread::current().id(),off,sz);

                    send_to_master.send(off).unwrap();
                }
                None => {
                    // println!("No message");
                }
            }
        }
    });
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
