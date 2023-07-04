#![feature(const_if_match)]
#![feature(associated_type_bounds)]
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
mod types;

use async_std::io;
use async_std::net::{TcpListener, TcpStream};
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use chrono::prelude::*;
use env_logger::fmt::Target;
use failure::{self, err_msg, Error, Fail};
use structopt::StructOpt;

use crate::architecture::cmd::Command;
use crate::architecture::tube::Tube;
use crate::operation::dispatch::Dispatch;
use crate::operation::ClientHandler;

use std::fs::File;
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
    ctrlc::set_handler(move || {
        info!("beanstalkr exit");
        process::exit(0);
    })
    .expect("TODO: panic message");

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
