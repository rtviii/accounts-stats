use std::{thread, sync::mpsc::{self, Sender, Receiver}};


pub fn threcho(msg:&str){
    println!("[{:?}]: {}",thread::current().id(), msg);
}

fn main() {

    let me = std::thread::current().id();
    

    let (sd, rx):( Sender<u64>, Receiver<u64> ) = mpsc::channel();


    let y = sd.clone();
    let x = thread::spawn(move|| {

        let me = std::thread::current().id();
        // println!("[ {:?} ] sending 1.", me);
        threcho("sending 1");
        y.send(1);
        println!("Hello from a thread!");

    });



    x.join();



    
    let got = rx.recv().unwrap();
    println!();

    
    threcho("received 1");

    
}
