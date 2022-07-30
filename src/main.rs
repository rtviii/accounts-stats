use std::{thread, sync::mpsc::{self, Sender, Receiver}, fmt::format};


pub fn threcho(msg:&str){
    println!("[{:?}]: {}",thread::current().id(), msg);
}



fn main() {

    // MASTER THREAD
    let data = (1..10_000).collect::<Vec<u64>>();
    let (sd, rx):( Sender<u64>, Receiver<u64> ) = mpsc::channel();
    let sender = sd.clone();
    let worker = thread::spawn(move|| {
        // WORKER thread
        // process a batch of 10000, return.
    
        for chunk in data.chunks(100){
            let mut sum = 0;
            for i in chunk.iter(){
                sum +=i;
            }
            threcho(&format!("sending sum({}) to master", sum));
            sender.send(sum).expect("Could not send.");
        }
    });


    // worker.join();

    
    while true {
        let got = rx.recv().unwrap();
        threcho(&format!("Got {}", got));
    };


    
}
