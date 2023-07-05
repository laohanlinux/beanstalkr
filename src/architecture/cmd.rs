use strum::*;
use failure::{Fail, bail, Error, err_msg};
use uuid::Uuid;

use std::str::FromStr;
use std::collections::HashMap;
use crate::architecture::job::{Job, random_factory};
use crate::architecture::tube::PriorityQueueItem;
use crate::architecture::protocol_config::{CommandParseOptions, CommandReplyOptions, CMD_PARSE_OPTIONS, CMD_REPLY_OPTIONS};
use crate::architecture::error::ProtocolError;

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, EnumString, Display, EnumCount, EnumDiscriminants)]
pub enum CMD {
    #[strum(to_string = "use")]
    Use,
    #[strum(to_string = "put")]
    Put,
    #[strum(to_string = "watch")]
    Watch,
    #[strum(to_string = "ignore")]
    Ignore,
    #[strum(to_string = "reserve")]
    Reserve,
    #[strum(to_string = "reserve-with-timeout")]
    ReserveWithTimeout,
    #[strum(to_string = "delete")]
    Delete,
    #[strum(to_string = "release")]
    Release,
    #[strum(to_string = "bury")]
    Bury,
    #[strum(to_string = "touch")]
    Touch,
    #[strum(to_string = "quit")]
    Quit,
    #[strum(to_string = "kick")]
    Kick,
    #[strum(to_string = "kick-job")]
    KickJob,

    #[strum(to_string = "peek")]
    Peek,
    #[strum(to_string = "peek-ready")]
    PeekReady,
    #[strum(to_string = "peek-delayed")]
    PeekDelayed,
    #[strum(to_string = "peek_buried")]
    PeekBuried,

    #[strum(to_string = "pause-tube")]
    PauseTube,
    #[strum(to_string = "list-tubes-watched")]
    ListTubesWatched,
    #[strum(to_string = "list-tubes")]
    ListTubes,
    #[strum(to_string = "list-tube-used")]
    ListTubeUsed,
}

impl CMD {}

impl Clone for CMD {
    fn clone(&self) -> Self {
        match self {
            CMD::Use => CMD::Use,
            CMD::Put => CMD::Put,
            CMD::Watch => CMD::Watch,
            CMD::Ignore => CMD::Ignore,
            CMD::Reserve => CMD::Reserve,
            CMD::ReserveWithTimeout => CMD::ReserveWithTimeout,
            CMD::Delete => CMD::Delete,
            CMD::Release => CMD::Release,
            CMD::Bury => CMD::Bury,
            CMD::Touch => CMD::Touch,
            CMD::Quit => CMD::Quit,
            CMD::Kick => CMD::Kick,
            CMD::KickJob => CMD::KickJob,
            CMD::Peek => CMD::Peek,
            CMD::PeekReady => CMD::PeekReady,
            CMD::PeekDelayed => CMD::PeekDelayed,
            CMD::PeekBuried => CMD::PeekBuried,
            CMD::PauseTube => CMD::PauseTube,
            CMD::ListTubesWatched => CMD::ListTubesWatched,
            CMD::ListTubes => CMD::ListTubes,
            CMD::ListTubeUsed => CMD::ListTubeUsed,
        }
    }
}

const MaxJobSize: i64 = 65536;

#[derive(Debug, Clone)]
pub struct Command {
    // eg: default
    pub(crate) name: String,
    // eg: use default
    pub(crate) raw_command: String,
    pub params: HashMap<String, String>,
    pub(crate) not_complete_received: bool,
    pub(crate) not_complete_send: bool,
    pub(crate) job: Job,
    pub(crate) yaml: Option<String>,
    pub err: Result<(), ProtocolError>,
}

/// Create a default Command of 'use default'
impl Default for Command {
    fn default() -> Self {
        let mut params = HashMap::new();
        params.insert("tube".to_owned(), "default".to_owned());
        Command {
            name: CMD::Use.to_string(),
            raw_command: "use default".to_string(),
            params,
            not_complete_received: false,
            not_complete_send: false,
            job: Default::default(),
            yaml: None,
            err: Ok(()),
        }
    }
}

impl Command {
    pub fn create_job_from_params(&mut self) -> Result<(), Error> {
        let pri = self.params.get("pri").unwrap().parse::<i64>()?;
        let delay = self.params.get("delay").unwrap().parse::<i64>()?;
        let mut ttr = self.params.get("ttr").unwrap().parse::<i64>()?;
        let bytes = self.params.get("bytes").unwrap().parse::<i64>()?;
        let id = random_factory();
        let data = self.params.get("data").unwrap();

        if ttr <= 0 {
            ttr = 1;
        }
        if bytes > MaxJobSize {
            return Err(ProtocolError::JobTooBig.into());
        }
        if bytes != data.len() as i64 {
            return Err(err_msg(ProtocolError::BadFormat));
        }
        self.job = Job::new(id, pri, delay, ttr, bytes, data.clone());
        debug!("create new job from params: {:?}", self.job.id());
        Ok(())
    }
    pub fn wrap_result(mut self, err: Result<(), ProtocolError>) -> Self {
        self.err = err;
        self
    }

    pub fn parse(&mut self, raw_command: &str) -> Result<bool, Error> {
        // 有些命令命令只接收一次即可；有些命令需要接收两次
        if !self.not_complete_received {
            let parts: Vec<&str> = raw_command.split_ascii_whitespace().collect();
            if parts.is_empty() {
                return Err(err_msg(ProtocolError::BadFormat));
            }
            let name = parts.first().unwrap().to_lowercase();

            let opts: &CommandParseOptions = CMD_PARSE_OPTIONS.get(&name).ok_or(err_msg(ProtocolError::UnknownCommand))?;
            // 解析出命令名
            self.name = name;

            // 判断参数个数是够一致
            if parts.len() != opts.expected_length {
                return Err(err_msg(ProtocolError::BadFormat));
            }

            // 解析命令信息
            self.raw_command = raw_command.clone().to_string();
            self.not_complete_received = opts.waiting_for_more;

            for (i, param_name) in opts.params.iter().enumerate() {
                self.params.insert(param_name.to_string(), parts[i + 1].clone().to_string());
            }

//            debug!("PROTOCOL command after parsing {:?}", self);
            return Ok(!self.not_complete_received);
        }

        // 解析第2轮命令
//        debug!("GOT MORE {:?}", self);
        if self.name == CMD::Put.to_string() {
            self.params.insert("data".to_owned(), raw_command.to_owned());
            self.raw_command = raw_command.to_owned() + "\r\n";
            self.create_job_from_params()?;
            self.not_complete_received = false;
        }
        Ok(true)
    }

    // 如果命令处理有问题，则立刻回应
    pub async fn reply(&mut self) -> (bool, String) {
        let cmd = CMD::from_str(self.name.as_ref()).unwrap();
        if self.err.is_err() {
            return (false, format!("{}", self.err.as_ref().unwrap_err()));
        }
        if let Some(opts) = CMD_REPLY_OPTIONS.get(&self.name) {
            if !opts.use_job_id {
                // DELETE, RELEASE, BURY
                if opts.param.is_empty() {
                    return (false, opts.message.clone());
                }
                return (false, vec![opts.message.clone(), self.params.get(&opts.param).unwrap().clone()].join(" "));
            }

            return (false, vec![opts.message.clone(), self.job.id().to_string()].join(" "));
        }

        if !self.not_complete_send {
            self.not_complete_send = true;
            match cmd {
                CMD::Peek | CMD::PeekReady | CMD::PeekDelayed | CMD::PeekBuried => {
                    return (true, format!("FOUND {} {}", self.job.id(), self.job.bytes));
                }
                CMD::Reserve | CMD::ReserveWithTimeout => {
                    return (true, format!("RESERVED {} {}", self.job.id(), self.job.bytes));
                }
                CMD::ListTubes => {
                    return (true, format!("OK {}", self.yaml.as_ref().unwrap().len()));
                }
                CMD::ListTubesWatched => {
                    return (true, format!("OK {}", self.yaml.as_ref().unwrap().len()));
                }
                _ => unreachable!()
            }
        }
        if self.yaml.is_some() {
            let yaml = self.yaml.as_ref().unwrap();
            return (false, yaml.clone());
        }
        (false, format!("{}", self.job.data))
    }
}
