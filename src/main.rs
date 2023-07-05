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
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio_util::codec::{Framed, LinesCodec};

/// A basic example
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    #[structopt(short, long, default_value = "0.0.0.0:11300")]
    addr: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init_timed();
    ctrlc::set_handler(move || {
        info!("beanstalkr exit");
        process::exit(0);
    })
        .expect("TODO: panic message");

    let opt: Opt = Opt::from_args();
    let listener = tokio::net::TcpListener::bind(opt.addr).await?;
    info!("Listening on {}", listener.local_addr()?);

    let dispatch: Arc<Mutex<Dispatch>> = Arc::new(Mutex::new(Dispatch::new()));

    while let Ok((stream, addr)) = listener.accept().await {
        let dispatch = dispatch.clone();
        spawn(async move {
            let mut client =
                ClientHandler::new(Framed::new(stream, LinesCodec::new()), dispatch.clone());
            if let Err(err) = client.spawn_start().await {
                error!("spawn start: {}", err);
            }
        });
    }

    Ok(())
}
