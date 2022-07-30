use crossbeam::thread::Scope;
use crossbeam_utils::thread;
use std::{
    fmt::format,
    sync::mpsc::{self, Receiver, Sender},
    time::Duration,
};

pub fn threcho(msg: &str) {
    println!("[{:?}]: {}", std::thread::current().id(), msg);
}

// WORKER thread
// process a batch of 100, return.
// for chunk in data.into_iter(){
//     let mut sum = 0;
//     // for i in chunk.iter(){
//     //     sum +=i;
//     //
//     threcho(&format!("sending sum({}) to master", sum));
//     send_to_master.send(sum).expect("Could not send.");
// pub fn spawn_worker_for_chunk(data: &[u64], send_to_master: Sender<u64>, chunksize: u64) {
// }


pub fn spawn_worker_in_scope<'a>(s:&Scope<'a>, data: &'a[u64], send_to_master: Sender<u64>, chunksize: u64) {
    s.spawn(move|_| {
        println!(" {:?} <-", std::thread::current().id());
            println!("A child thread borrowing `var`: {:?}", &data[0]);
            send_to_master.send(data[0]).expect("msdasdag");
    });
}

fn main() {
    // MASTER THREAD
    let data = (1..10_000).collect::<Vec<u64>>();
    let (sd, rx): (Sender<u64>, Receiver<u64>) = mpsc::channel();
    let sender = sd.clone();



    thread::scope(|s| {


        spawn_worker_in_scope(s, &data[0..10], sd.clone(), 10);
        spawn_worker_in_scope(s, &data[10..20], sd.clone(), 10);
        spawn_worker_in_scope(s, &data[20..30], sd.clone(), 10);


        // let jh1 = s.spawn(|_| {
        //     println!("Going to sleep for 3 sec.");
        //     std::thread::sleep(Duration::from_secs(3));
        //     println!("woke up");
        // });
        // // jh1.join();

        // let jh2 = s.spawn(|_| {
        //     println!("A child thread borrowing `var`: {:?}", &data[0]);
        // });

    })
    .unwrap();

    // let worker = thread::spawn(move|| {
    //     // WORKER thread
    //     // process a batch of 10000, return.

    //     for chunk in data.chunks(100){
    //         let mut sum = 0;
    //         for i in chunk.iter(){
    //             sum +=i;
    //         }
    //         threcho(&format!("sending sum({}) to master", sum));
    //         sender.send(sum).expect("Could not send.");
    //     }
    // });

    // worker.join();

    while true {
        let got = rx.recv().unwrap();
        threcho(&format!("Got {}", got));
    }
}
