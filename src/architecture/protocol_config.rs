use crate::architecture::cmd::CommandKind;

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CommandParseOptions {
    pub expected_length: usize,
    pub waiting_for_more: bool,
    pub params: Vec<String>,
    #[allow(dead_code)]
    pub name: CommandKind,
}

#[derive(Debug, Clone)]
pub struct CommandReplyOptions {
    #[allow(dead_code)]
    pub result: bool,
    pub message: String,
    pub param: String,
    pub use_job_id: bool,
}

lazy_static! {
    pub static ref COMMAND_PARSE_OPTIONS: HashMap<String, CommandParseOptions> = {
        let mut m = HashMap::new();
        m.insert(
            CommandKind::Use.to_string(),
            CommandParseOptions {
                name: CommandKind::Use,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["tube".to_string()],
            },
        );
        m.insert(
            CommandKind::Put.to_string(),
            CommandParseOptions {
                name: CommandKind::Put,
                expected_length: 5,
                waiting_for_more: true,
                params: vec![
                    "pri".to_string(),
                    "delay".to_string(),
                    "ttr".to_string(),
                    "bytes".to_string(),
                ],
            },
        );
        m.insert(
            CommandKind::Watch.to_string(),
            CommandParseOptions {
                name: CommandKind::Watch,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["tube".to_string()],
            },
        );
        m.insert(
            CommandKind::Ignore.to_string(),
            CommandParseOptions {
                name: CommandKind::Ignore,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["tube".to_string()],
            },
        );
        m.insert(
            CommandKind::Reserve.to_string(),
            CommandParseOptions {
                name: CommandKind::Reserve,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::ReserveWithTimeout.to_string(),
            CommandParseOptions {
                name: CommandKind::ReserveWithTimeout,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["timeout".to_string()],
            },
        );
        m.insert(
            CommandKind::ReserveJob.to_string(),
            CommandParseOptions {
                name: CommandKind::ReserveJob,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::Delete.to_string(),
            CommandParseOptions {
                name: CommandKind::Delete,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::Release.to_string(),
            CommandParseOptions {
                name: CommandKind::Release,
                expected_length: 4,
                waiting_for_more: false,
                params: vec!["id".to_string(), "pri".to_string(), "delay".to_string()],
            },
        );
        m.insert(
            CommandKind::Bury.to_string(),
            CommandParseOptions {
                name: CommandKind::Bury,
                expected_length: 3,
                waiting_for_more: false,
                params: vec!["id".to_string(), "pri".to_string()],
            },
        );
        m.insert(
            CommandKind::Touch.to_string(),
            CommandParseOptions {
                name: CommandKind::Touch,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::Quit.to_string(),
            CommandParseOptions {
                name: CommandKind::Quit,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::Kick.to_string(),
            CommandParseOptions {
                name: CommandKind::Kick,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["bound".to_string()],
            },
        );
        m.insert(
            CommandKind::KickJob.to_string(),
            CommandParseOptions {
                name: CommandKind::KickJob,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::PauseTube.to_string(),
            CommandParseOptions {
                name: CommandKind::PauseTube,
                expected_length: 3,
                waiting_for_more: false,
                params: vec!["tube".to_string(), "delay".to_string()],
            },
        );
        m.insert(
            CommandKind::Peek.to_string(),
            CommandParseOptions {
                name: CommandKind::Peek,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::PeekReady.to_string(),
            CommandParseOptions {
                name: CommandKind::PeekReady,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::PeekDelayed.to_string(),
            CommandParseOptions {
                name: CommandKind::PeekDelayed,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::PeekBuried.to_string(),
            CommandParseOptions {
                name: CommandKind::PeekBuried,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::ListTubesWatched.to_string(),
            CommandParseOptions {
                name: CommandKind::ListTubesWatched,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::ListTubes.to_string(),
            CommandParseOptions {
                name: CommandKind::ListTubes,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::ListTubeUsed.to_string(),
            CommandParseOptions {
                name: CommandKind::ListTubeUsed,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::Stats.to_string(),
            CommandParseOptions {
                name: CommandKind::Stats,
                expected_length: 1,
                waiting_for_more: false,
                params: vec![],
            },
        );
        m.insert(
            CommandKind::StatsJob.to_string(),
            CommandParseOptions {
                name: CommandKind::StatsJob,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["id".to_string()],
            },
        );
        m.insert(
            CommandKind::StatsTube.to_string(),
            CommandParseOptions {
                name: CommandKind::StatsTube,
                expected_length: 2,
                waiting_for_more: false,
                params: vec!["tube".to_string()],
            },
        );
        m
    };
    pub static ref COMMAND_REPLY_OPTIONS: HashMap<String, CommandReplyOptions> = {
        let mut m = HashMap::new();
        m.insert(
            CommandKind::Use.to_string(),
            CommandReplyOptions {
                result: false,
                message: "USING".to_string(),
                param: "tube".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Put.to_string(),
            CommandReplyOptions {
                result: false,
                message: "INSERTED".to_string(),
                param: "".to_string(),
                use_job_id: true,
            },
        );
        m.insert(
            CommandKind::Watch.to_string(),
            CommandReplyOptions {
                result: false,
                message: "WATCHING".to_string(),
                param: "count".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Ignore.to_string(),
            CommandReplyOptions {
                result: false,
                message: "WATCHING".to_string(),
                param: "count".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Delete.to_string(),
            CommandReplyOptions {
                result: false,
                message: "DELETED".to_string(),
                param: "".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Release.to_string(),
            CommandReplyOptions {
                result: false,
                message: "RELEASED".to_string(),
                param: "".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Bury.to_string(),
            CommandReplyOptions {
                result: false,
                message: "BURIED".to_string(),
                param: "".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::PauseTube.to_string(),
            CommandReplyOptions {
                result: false,
                message: "PAUSED".to_string(),
                param: "".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Touch.to_string(),
            CommandReplyOptions {
                result: false,
                message: "TOUCHED".to_string(),
                param: "".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::Kick.to_string(),
            CommandReplyOptions {
                result: false,
                message: "KICKED".to_string(),
                param: "count".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::KickJob.to_string(),
            CommandReplyOptions {
                result: false,
                message: "KICKED".to_string(),
                param: "".to_string(),
                use_job_id: false,  // kick-job 只返回 KICKED，不包含 job id
            },
        );
        m.insert(
            CommandKind::Watch.to_string(),
            CommandReplyOptions {
                result: false,
                message: "WATCHING".to_string(),
                param: "count".to_string(),
                use_job_id: false,
            },
        );
        m.insert(
            CommandKind::ListTubeUsed.to_string(),
            CommandReplyOptions {
                result: false,
                message: "USING".to_string(),
                param: "tube".to_string(),
                use_job_id: false,
            },
        );
        m
    };
}
