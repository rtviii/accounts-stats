use crossbeam::thread::Scope;
use crossbeam_utils::thread;
use log::{info, warn};
use std::{
    fmt::format,
    sync::mpsc::{self, Receiver, Sender},
    time::Duration,
};
use futures::{Stream, StreamExt};
use rdkafka::{producer::{ BaseProducer, BaseRecord , DeliveryResult, ProducerContext}, statistics::Topic, consumer::{StreamConsumer, MessageStream}};
use rand::RngCore;
use rdkafka::consumer::base_consumer::{self, BaseConsumer};
use rdkafka::{config::FromClientConfig, TopicPartitionList};
use serde_json::Value;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::thread::JoinHandle;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::producer::ThreadedProducer;
use rdkafka::Message;
use clap::Parser;

struct ProducerLogger {}
impl ClientContext for ProducerLogger {}
impl ProducerContext for ProducerLogger {
    type DeliveryOpaque = ();
    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        let delivery_res = delivery_result.as_ref();
        match delivery_res {
            Ok(message) => {
                let key: &str = message.key_view().unwrap().unwrap();
                println!(
                    "Produced message {} successfully. Offset: {}. Partition :{}",
                    key,
                    message.offset(),
                    message.partition()
                )
                // This would probably be a good place to commit offsets
            }

            Err(producer_err) => {
                let key: &str = producer_err.1.key_view().unwrap().unwrap();
                println!(
                    "Failed to produce message {}. Error: {}",
                    key, producer_err.0
                )
            }
        }
    }
}
pub fn producer() {
    let producer: ThreadedProducer<ProducerLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9095")
        .create_with_context(ProducerLogger {})
        .expect("Failed to create producer");

}

pub fn consumer(){
    let mut baseconsumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9095")
        // .set("enable.auto.commit", "false")
        .set("group.id", "rt0")
        .create()
        .expect("Failed to create consumer");

}



#[derive(Debug, Parser)]
#[clap(author, version, long_about=None, about = "Prototype of a tool to extract data from a set of blocks + threading experiments")]
pub struct Args {

    #[clap(takes_value = false,  long)]
    producer: bool,

    #[clap(takes_value = false,  long)]
    consumer: bool,

    #[clap(short='g', long)]
    consumer_group: Option<String>,

    #[clap(short='p', long)]
    broker_port: usize
}

#[tokio::main]
async fn main() {

    let args      = Args::parse();

    let flag_cons = args.consumer;
    let flag_prod = args.producer;

    let port           = &args.broker_port;
    let consumer_group = &args.consumer_group.unwrap_or("default_consumer_group".to_string());

    let mut baseconsumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{}",port))
        // .set("enable.auto.commit", "false")
        .set("group.id", consumer_group)
        .create()
        .expect("Failed to create consumer");


        let mut stream :StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", format!("127.0.0.1:{}",port))
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        // .set("enable.auto.commit", "false")
        .set("group.id", consumer_group)
        .create()
        .expect("Failed to create consumer");




    
    let mut tpl = TopicPartitionList::new();
    const TOPIC :&'static str  =  "historic_blocks_json";
    tpl.add_partition_offset(TOPIC, 0, rdkafka::Offset::Beginning);
    println!("{:?}", tpl);
    stream.assign(&tpl).unwrap();

    stream.seek(TOPIC, 0, rdkafka::Offset::Offset(16000), Duration::from_secs(2)).unwrap();

    let mut counter  = 0;
    loop{
        println!("Counter :{ }", counter);
        match stream.stream().next().await{
                None => warn!("Kafka none"),
           Some(m)   => {
            counter +=1;
            println!("m: {:?}", m.unwrap().payload())
           }
                // if let Some(headers) = m.headers() {
                //     for header in headers.iter() {
                //         info!("  Header {:#?}: {:?}", header.key, header.value);
                //     }
                // }
                // consumer.commit_message(&m, CommitMode::Async).unwrap();

    }

    }
    // while let msg =  stream.stream(){
    //     let msg      = msgres.unwrap();
    //     let key:&str = msg.key_view().unwrap().unwrap();
    //     let value    = msg.payload().unwrap();
    //     println!("Received message {} with value {:?}. Offset :{} | Partition :{}", key, value, msg.offset(),msg.partition());

    // }
    // baseconsumer.assign(&tpl).unwrap();
    // // baseconsumer.seek("historic_blocks_json", 0, rdkafka::Offset::Offset(100), Duration::from_secs(3)).unwrap();
    // for msgres in baseconsumer.iter(){
    //     let msg      = msgres.unwrap();
    //     let key:&str = msg.key_view().unwrap().unwrap();
    //     let value    = msg.payload().unwrap();
    //     println!("Received message {} with value {:?}. Offset :{} | Partition :{}", key, value, msg.offset(),msg.partition());
    // }
    
    

    // // MASTER THREAD
    // let data = (1..10_000).collect::<Vec<u64>>();
    // let (sd, rx): (Sender<u64>, Receiver<u64>) = mpsc::channel();
    // let sender = sd.clone();
    
    // thread::scope(|s| {
    //     spawn_worker_in_scope(s, &data[0..10], sd.clone(), 10);
    //     spawn_worker_in_scope(s, &data[10..20], sd.clone(), 10);
    //     spawn_worker_in_scope(s, &data[20..30], sd.clone(), 10);
    // })
    // .unwrap();

    // loop {
    //     let got = rx.recv().unwrap();
    //     threcho(&format!("Got {}", got));
    // }

    // for i in 1..10_000_000 {
    //     let mut data = [0u8; 8];
    //     rand::thread_rng().fill_bytes(&mut data);
    //     println!("Sedndg msg with data {:x?}", data);
    //     producer
    //         .send(
    //             BaseRecord::to("rt-test")
    //                 .key(&format!("key-{}", i))
    //                 .payload(&format!("payload-{}", i)),
    //         )
    //         .expect("couldn't send message");
    // }

    // baseconsumer.subscribe(&[&"rt-test"]).expect("Couldnbt subscribe t topic.");
    // return thread::spawn(move || loop{
    // });

    if flag_cons {
        println!("Starting consumer");
        let x = consumer();
    } else if flag_prod {
        producer();
    }


    // tokio_ex::main()
}



pub fn threcho(msg: &str) {
    println!("[{:?}]: {}", std::thread::current().id(), msg);
}

pub fn spawn_worker_in_scope<'a>(s:&Scope<'a>, data: &'a[u64], send_to_master: Sender<u64>, chunksize: u64) {
    s.spawn(move|_| {
        println!(" {:?} <-", std::thread::current().id());
            println!("A child thread borrowing `var`: {:?}", &data[0]);
            send_to_master.send(data[0]).expect("msdasdag");
    });
}


