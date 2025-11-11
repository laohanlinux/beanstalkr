#![recursion_limit = "512"]

#[macro_use]
extern crate lazy_static;
extern crate strum;
#[macro_use]
extern crate strum_macros;
#[macro_use]
extern crate log;

mod architecture;
mod backend;
mod backup;
mod channel;
mod operation;

use structopt::StructOpt;

use async_std::io;
use async_std::net::TcpListener;
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;

use failure;

use crate::operation::dispatch::Dispatch;
use crate::operation::ClientHandler;

use std::process;

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    #[structopt(short, long, default_value = "0.0.0.0:11300")]
    addr: String,
}

fn main() -> io::Result<()> {
    pretty_env_logger::init_timed();
    _ = ctrlc::set_handler(move || {
        info!("beanstalkr exit");
        process::exit(0);
    });

    let opt: Opt = Opt::from_args();
    task::block_on(async move {
        let listener = TcpListener::bind(opt.addr).await?;
        let mut incoming = listener.incoming();
        info!("Listening on {}", listener.local_addr()?);
        let dispatch: Arc<Mutex<Dispatch>> = Arc::new(Mutex::new(Dispatch::new()));
        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            let dispatch = dispatch.clone();
            task::spawn(async move {
                let mut client = ClientHandler::new(Arc::new(stream), dispatch.clone());
                if let Err(err) = client.spawn_start().await {
                    error!("spawn start: {}", err);
                }
            });
        }
        Ok(())
    })
}
