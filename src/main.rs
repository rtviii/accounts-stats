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
    sync::mpsc::{self, Receiver, Sender},
    time::Duration,
};

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

pub fn consumer() {
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
    #[clap(takes_value = false, long)]
    producer: bool,

    #[clap(takes_value = false, long)]
    consumer: bool,

    #[clap(short = 'g', long)]
    consumer_group: Option<String>,

    #[clap(short = 'p', long)]
    broker_port: usize,
}

const TOPIC: &'static str = "historic_blocks_json";
pub async fn process_region(
    port          : usize,
    consumer_group: &str,
    offset_start  : usize,
    offset_end    : usize,
    topic         : &str,
    chl_sender    : Sender<i64>,
) {
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let flag_cons = args.consumer;
    let flag_prod = args.producer;
    let port = &args.broker_port;
    let consumer_group = &args
        .consumer_group
        .unwrap_or("default_consumer_group".to_string());

    let (tx, rx) = mpsc::channel();



    thread::scope(|s| {
        spawn_consumer_in_scope(s, tx.clone(), 29092, "rt".to_string(), 5400, 5300);
        spawn_consumer_in_scope(s, tx.clone(), 29092, "rt".to_string(), 5100, 5000);
        spawn_consumer_in_scope(s, tx.clone(), 29092, "rt".to_string(), 5200, 5100);
        spawn_consumer_in_scope(s, tx.clone(), 29092, "rt".to_string(), 5700, 5600);
    })
    .unwrap();

    loop {
        match rx.recv(){
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
    crossbeam_scope: &Scope<'a>,
    send_to_master: Sender<i64>,
    port: usize,
    consumer_group: String,
    offset_end: usize,
    offset_start: usize,
)->Result<(),KafkaError> {
    crossbeam_scope.spawn(move |_| -> Result<(),KafkaError> {
        let bconsumer = {
            let mut bconsumer: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", format!("127.0.0.1:{}", port))
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "true")
                // .set("enable.auto.commit", "false")
                .set("group.id", consumer_group)
                .create()
                .expect("Failed to create consumer");

            bconsumer
        };

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(TOPIC, 0, rdkafka::Offset::Offset(offset_start as i64))?;
        println!("{:?}", tpl);
        bconsumer.assign(&tpl)?;

        
        println!("Base consumer is at {:?}", bconsumer.assignment()?);
        let mut range = offset_end - offset_start;
        loop {
            match bconsumer.poll(Duration::from_secs(2)) {
                    Some(m) => {
                        range -=1;
                        let off= m?.offset();
                        println!("Got message with offset :{:?}", off);
                        send_to_master.send(off).unwrap();
                    }
                    None => {
                        range -=1;
                        println!("No message");
                    }
            }
            if range < 1 {
                println!("Stream completed.");
                break;
            };
        }
        Ok(())
    });
    Ok(())
}
