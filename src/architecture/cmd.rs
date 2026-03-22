use crate::architecture::error::ProtocolError;
use crate::architecture::job::{next_job_id, Job};
use crate::architecture::protocol_config::{
    CommandParseOptions, COMMAND_PARSE_OPTIONS, COMMAND_REPLY_OPTIONS,
};
use crate::architecture::stats::GLOBAL_STATS;
use std::sync::atomic::Ordering;

use anyhow::{anyhow, Error};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use tracing::debug;

#[allow(dead_code)]
#[derive(Debug, Clone, Eq, PartialEq, EnumString, EnumCount, EnumDiscriminants)]
pub enum CommandKind {
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
    #[strum(to_string = "reserve-job")]
    ReserveJob,
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
    #[strum(to_string = "peek-buried")]
    PeekBuried,

    #[strum(to_string = "pause-tube")]
    PauseTube,
    #[strum(to_string = "list-tubes-watched")]
    ListTubesWatched,
    #[strum(to_string = "list-tubes")]
    ListTubes,
    #[strum(to_string = "list-tube-used")]
    ListTubeUsed,
    
    // Stats 命令
    #[strum(to_string = "stats")]
    Stats,
    #[strum(to_string = "stats-job")]
    StatsJob,
    #[strum(to_string = "stats-tube")]
    StatsTube,
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 使用 strum 的 to_string 来获取字符串表示
        // 注意：EnumString 会生成 FromStr，我们可以通过 match 来实现 Display
        let s = match self {
            CommandKind::Use => "use",
            CommandKind::Put => "put",
            CommandKind::Watch => "watch",
            CommandKind::Ignore => "ignore",
            CommandKind::Reserve => "reserve",
            CommandKind::ReserveWithTimeout => "reserve-with-timeout",
            CommandKind::ReserveJob => "reserve-job",
            CommandKind::Delete => "delete",
            CommandKind::Release => "release",
            CommandKind::Bury => "bury",
            CommandKind::Touch => "touch",
            CommandKind::Quit => "quit",
            CommandKind::Kick => "kick",
            CommandKind::KickJob => "kick-job",
            CommandKind::Peek => "peek",
            CommandKind::PeekReady => "peek-ready",
            CommandKind::PeekDelayed => "peek-delayed",
            CommandKind::PeekBuried => "peek-buried",
            CommandKind::PauseTube => "pause-tube",
            CommandKind::ListTubesWatched => "list-tubes-watched",
            CommandKind::ListTubes => "list-tubes",
            CommandKind::ListTubeUsed => "list-tube-used",
            CommandKind::Stats => "stats",
            CommandKind::StatsJob => "stats-job",
            CommandKind::StatsTube => "stats-tube",
        };
        write!(f, "{}", s)
    }
}

// MAX_JOB_SIZE 现在通过 GLOBAL_STATS.max_job_size 配置

#[derive(Debug, Clone)]
pub struct Command {
    pub(crate) name: String,
    pub(crate) raw_command: String,
    pub params: HashMap<String, String>,
    pub(crate) not_complete_received: bool,
    pub(crate) not_complete_send: bool,
    pub(crate) job: Job,
    pub(crate) yaml: Option<String>,
    pub err: Result<(), ProtocolError>,
}

impl Default for Command {
    fn default() -> Self {
        let mut params = HashMap::new();
        params.insert("tube".to_owned(), "default".to_owned());
        Command {
            name: CommandKind::Use.to_string(),
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
        let id = next_job_id();
        let data = self.params.get("data").unwrap();

        if ttr <= 0 {
            ttr = 1;
        }
        let max_job_size = GLOBAL_STATS.max_job_size.load(Ordering::SeqCst) as i64;
        if bytes > max_job_size {
            return Err(ProtocolError::JobTooBig.into());
        }
        // 验证数据长度（注意：body 包含 \r\n，所以 bytes 应该等于 data.len()）
        if bytes != data.len() as i64 {
            return Err(anyhow!(ProtocolError::BadFormat));
        }
        // 验证数据以 \r\n 结尾
        if !data.ends_with("\r\n") {
            return Err(ProtocolError::ExpectedCrlf.into());
        }
        self.job = Job::new(id, pri, delay, ttr, bytes, data.clone());
        debug!("create new job from params: {:?}", self.job.id());
        Ok(())
    }
    pub fn wrap_result(mut self, err: Result<(), ProtocolError>) -> Self {
        self.err = err;
        self
    }

    /// 验证 tube 名称是否合法
    /// 根据协议，名称可以包含：letters, numerals, hyphen, plus, slash, semicolon,
    /// dot, dollar-sign, underscore, parentheses，但不能以连字符开头
    fn validate_tube_name(name: &str) -> Result<(), ProtocolError> {
        if name.len() > 200 {
            return Err(ProtocolError::BadFormat);
        }
        if name.starts_with('-') {
            return Err(ProtocolError::BadFormat);
        }
        // 验证每个字符是否合法
        for c in name.chars() {
            if !c.is_ascii_alphanumeric() 
                && c != '-'
                && c != '+'
                && c != '/'
                && c != ';'
                && c != '.'
                && c != '$'
                && c != '_'
                && c != '('
                && c != ')' {
                return Err(ProtocolError::BadFormat);
            }
        }
        Ok(())
    }

    /// 最大命令行长度（包括 \r\n）
    const MAX_COMMAND_LINE_LENGTH: usize = 224;

    pub fn parse(&mut self, raw_command: &str) -> Result<bool, Error> {
        // 有些命令命令只接收一次即可；有些命令需要接收两次
        if !self.not_complete_received {
            // 检查命令行长度
            if raw_command.len() > Self::MAX_COMMAND_LINE_LENGTH {
                return Err(anyhow!(ProtocolError::BadFormat));
            }
            
            let parts: Vec<&str> = raw_command.split_ascii_whitespace().collect();
            if parts.is_empty() {
                return Err(anyhow!(ProtocolError::BadFormat));
            }
            let name = parts.first().unwrap().to_lowercase();

            let opts: &CommandParseOptions = COMMAND_PARSE_OPTIONS
                .get(&name)
                .ok_or(anyhow!(ProtocolError::UnknownCommand))?;
            // 解析出命令名
            self.name = name;

            // 判断参数个数是够一致
            if parts.len() != opts.expected_length {
                return Err(anyhow!(ProtocolError::BadFormat));
            }

            // 解析命令信息
            self.raw_command = raw_command.to_string();
            self.not_complete_received = opts.waiting_for_more;

            for (i, param_name) in opts.params.iter().enumerate() {
                self.params
                    .insert(param_name.to_string(), parts[i + 1].to_string());
            }

            // 验证 tube 名称（如果存在）
            if let Some(tube) = self.params.get("tube") {
                Self::validate_tube_name(tube)?;
            }

            //            debug!("PROTOCOL command after parsing {:?}", self);
            return Ok(!self.not_complete_received);
        }

        // 解析第2轮命令
        //        debug!("GOT MORE {:?}", self);
        if self.name == CommandKind::Put.to_string() {
            self.params
                .insert("data".to_owned(), raw_command.to_owned());
            self.raw_command = raw_command.to_owned() + "\r\n";
            self.create_job_from_params()?;
            self.not_complete_received = false;
        }
        Ok(true)
    }

    // 如果命令处理有问题，则立刻回应
    pub async fn reply(&mut self) -> (bool, String) {
        let cmd = CommandKind::from_str(self.name.as_ref()).unwrap();
        if let Err(ref err) = self.err {
            let err_str = format!("{}", err);
            // 特殊处理 BURIED 错误 - 需要返回 BURIED <id>
            if err_str == "BURIED" && matches!(cmd, CommandKind::Put | CommandKind::Release) {
                return (false, format!("BURIED {}", self.job.id()));
            }
            // 特殊处理 OUT_OF_MEMORY 错误 - 需要返回 OUT_OF_MEMORY
            if err_str == "OUT_OF_MEMORY" {
                return (false, "OUT_OF_MEMORY".to_string());
            }
            // 特殊处理 EXPECTED_CRLF 错误
            if err_str == "EXPECTED_CRLF" {
                return (false, "EXPECTED_CRLF".to_string());
            }
            return (false, err_str);
        }
        if let Some(opts) = COMMAND_REPLY_OPTIONS.get(&self.name) {
            if !opts.use_job_id {
                // DELETE, RELEASE, BURY
                if opts.param.is_empty() {
                    return (false, opts.message.clone());
                }
                return (
                    false,
                    [opts.message.clone(),
                        self.params.get(&opts.param).unwrap().clone()]
                    .join(" "),
                );
            }

            return (
                false,
                [opts.message.clone(), self.job.id().to_string()].join(" "),
            );
        }

        if !self.not_complete_send {
            self.not_complete_send = true;
            match cmd {
                CommandKind::Peek | CommandKind::PeekReady | CommandKind::PeekDelayed | CommandKind::PeekBuried => {
                    return (true, format!("FOUND {} {}", self.job.id(), self.job.bytes));
                }
                CommandKind::Reserve | CommandKind::ReserveWithTimeout | CommandKind::ReserveJob => {
                    return (
                        true,
                        format!("RESERVED {} {}", self.job.id(), self.job.bytes),
                    );
                }
                CommandKind::ListTubes => {
                    return (true, format!("OK {}", self.yaml.as_ref().unwrap().len()));
                }
                CommandKind::ListTubesWatched => {
                    return (true, format!("OK {}", self.yaml.as_ref().unwrap().len()));
                }
                CommandKind::Stats | CommandKind::StatsJob | CommandKind::StatsTube => {
                    return (true, format!("OK {}", self.yaml.as_ref().unwrap().len()));
                }
                _ => unreachable!(),
            }
        }
        if let Some(ref yaml) = self.yaml {
            return (false, yaml.clone());
        }
        (false, self.job.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_reply_reserve_job() {
        let mut cmd = Command::default();
        cmd.name = "reserve-job".to_string();
        cmd.job = Job::default();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        // reserve-job 成功时返回 RESERVED <id> <bytes>
        assert!(more);
        assert!(reply.starts_with("RESERVED "));
    }
    
    #[test]
    fn test_reply_peek_buried() {
        let mut cmd = Command::default();
        cmd.name = "peek-buried".to_string();
        cmd.job = Job::default();
        cmd.job.bytes = 50;
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        // peek-buried 返回 (true, "FOUND <id> <bytes>")
        assert!(more);
        assert!(reply.starts_with("FOUND "));
        assert!(reply.contains(" 50")); // bytes
    }
    
    #[test]
    fn test_reply_draining() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.err = Err(ProtocolError::Draining);
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "DRAINING");
    }

    #[test]
    fn test_reply_put_inserted() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.job = Job::default();
        
        // 使用 tokio 运行时测试异步函数
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        // 格式: INSERTED <id>
        assert!(reply.starts_with("INSERTED "), "Expected 'INSERTED <id>', got: {}", reply);
    }

    #[test]
    fn test_reply_use() {
        let mut cmd = Command::default();
        cmd.name = "use".to_string();
        cmd.params.insert("tube".to_string(), "mytube".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "USING mytube");
    }

    #[test]
    fn test_reply_watch() {
        let mut cmd = Command::default();
        cmd.name = "watch".to_string();
        cmd.params.insert("count".to_string(), "3".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "WATCHING 3");
    }

    #[test]
    fn test_reply_ignore() {
        let mut cmd = Command::default();
        cmd.name = "ignore".to_string();
        cmd.params.insert("count".to_string(), "2".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "WATCHING 2");
    }

    #[test]
    fn test_reply_delete() {
        let mut cmd = Command::default();
        cmd.name = "delete".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "DELETED");
    }

    #[test]
    fn test_reply_release() {
        let mut cmd = Command::default();
        cmd.name = "release".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "RELEASED");
    }

    #[test]
    fn test_reply_bury() {
        let mut cmd = Command::default();
        cmd.name = "bury".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "BURIED");
    }

    #[test]
    fn test_reply_touch() {
        let mut cmd = Command::default();
        cmd.name = "touch".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "TOUCHED");
    }

    #[test]
    fn test_reply_kick() {
        let mut cmd = Command::default();
        cmd.name = "kick".to_string();
        cmd.params.insert("count".to_string(), "5".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "KICKED 5");
    }

    #[test]
    fn test_reply_kick_job() {
        let mut cmd = Command::default();
        cmd.name = "kick-job".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "KICKED");
    }

    #[test]
    fn test_reply_pause_tube() {
        let mut cmd = Command::default();
        cmd.name = "pause-tube".to_string();
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "PAUSED");
    }

    #[test]
    fn test_reply_list_tube_used() {
        let mut cmd = Command::default();
        cmd.name = "list-tube-used".to_string();
        cmd.params.insert("tube".to_string(), "mytube".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "USING mytube");
    }

    #[test]
    fn test_reply_peek() {
        let mut cmd = Command::default();
        cmd.name = "peek".to_string();
        cmd.job = Job::default();
        cmd.job.bytes = 100;
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        // peek 返回 (true, "FOUND <id> <bytes>") 表示还有更多数据要发送
        assert!(more);
        assert!(reply.starts_with("FOUND "));
        assert!(reply.contains(" 100")); // bytes
    }

    #[test]
    fn test_reply_reserve() {
        let mut cmd = Command::default();
        cmd.name = "reserve".to_string();
        cmd.job = Job::default();
        cmd.job.bytes = 200;
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        // reserve 返回 (true, "RESERVED <id> <bytes>") 表示还有更多数据要发送
        assert!(more);
        assert!(reply.starts_with("RESERVED "));
        assert!(reply.contains(" 200")); // bytes
    }

    #[test]
    fn test_reply_error_not_found() {
        let mut cmd = Command::default();
        cmd.name = "delete".to_string();
        cmd.err = Err(ProtocolError::NotFound.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "NOT_FOUND");
    }

    #[test]
    fn test_reply_error_not_ignored() {
        let mut cmd = Command::default();
        cmd.name = "ignore".to_string();
        cmd.err = Err(ProtocolError::NotIgnored.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "NOT_IGNORED");
    }

    #[test]
    fn test_reply_put_buried() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.job = Job::default();
        cmd.err = Err(ProtocolError::Buried.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        // 格式: BURIED <id>
        assert!(reply.starts_with("BURIED "), "Expected 'BURIED <id>', got: {}", reply);
    }

    #[test]
    fn test_reply_release_buried() {
        let mut cmd = Command::default();
        cmd.name = "release".to_string();
        cmd.job = Job::default();
        cmd.err = Err(ProtocolError::Buried.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        // 格式: BURIED <id>
        assert!(reply.starts_with("BURIED "), "Expected 'BURIED <id>', got: {}", reply);
    }

    #[test]
    fn test_reply_timed_out() {
        let mut cmd = Command::default();
        cmd.name = "reserve-with-timeout".to_string();
        cmd.err = Err(ProtocolError::TimedOut.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "TIMED_OUT");
    }

    #[test]
    fn test_reply_deadline_soon() {
        let mut cmd = Command::default();
        cmd.name = "reserve".to_string();
        cmd.err = Err(ProtocolError::DeadlineSoon.into());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "DEADLINE_SOON");
    }

    #[test]
    fn test_reply_stats() {
        let mut cmd = Command::default();
        cmd.name = "stats".to_string();
        cmd.yaml = Some("current-tubes: 5\n".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(more);
        assert!(reply.starts_with("OK "));
    }

    #[test]
    fn test_reply_stats_job() {
        let mut cmd = Command::default();
        cmd.name = "stats-job".to_string();
        cmd.yaml = Some("id: 123\nstate: ready\n".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(more);
        assert!(reply.starts_with("OK "));
    }

    #[test]
    fn test_reply_stats_tube() {
        let mut cmd = Command::default();
        cmd.name = "stats-tube".to_string();
        cmd.yaml = Some("name: default\n".to_string());
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(more);
        assert!(reply.starts_with("OK "));
    }

    #[test]
    fn test_reply_out_of_memory() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.err = Err(ProtocolError::OutOfMemory);
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "OUT_OF_MEMORY");
    }

    #[test]
    fn test_reply_expected_crlf() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.err = Err(ProtocolError::ExpectedCrlf);
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "EXPECTED_CRLF");
    }

    #[test]
    fn test_reply_internal_error() {
        let mut cmd = Command::default();
        cmd.name = "put".to_string();
        cmd.err = Err(ProtocolError::InternalError);
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (more, reply) = rt.block_on(cmd.reply());
        
        assert!(!more);
        assert_eq!(reply, "INTERNAL_ERROR");
    }

    #[test]
    fn test_validate_tube_name_valid() {
        // 测试有效的 tube 名称
        assert!(Command::validate_tube_name("default").is_ok());
        assert!(Command::validate_tube_name("my-tube").is_ok());
        assert!(Command::validate_tube_name("my_tube").is_ok());
        assert!(Command::validate_tube_name("my.tube").is_ok());
        assert!(Command::validate_tube_name("my+tube").is_ok());
        assert!(Command::validate_tube_name("my/tube").is_ok());
        assert!(Command::validate_tube_name("my;tube").is_ok());
        assert!(Command::validate_tube_name("my$tube").is_ok());
        assert!(Command::validate_tube_name("my(tube)").is_ok());
        assert!(Command::validate_tube_name("tube123").is_ok());
    }

    #[test]
    fn test_validate_tube_name_invalid() {
        // 测试无效的 tube 名称
        // 不能以连字符开头
        assert!(Command::validate_tube_name("-tube").is_err());
        // 不能包含非法字符
        assert!(Command::validate_tube_name("my@tube").is_err());
        assert!(Command::validate_tube_name("my#tube").is_err());
        assert!(Command::validate_tube_name("my tube").is_err()); // 包含空格
        // 不能超过 200 字节
        let long_name = "a".repeat(201);
        assert!(Command::validate_tube_name(&long_name).is_err());
    }
}
