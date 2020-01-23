use crate::architecture::cmd::CMD;

use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CommandParseOptions {
    pub expected_length: usize,
    pub waiting_for_more: bool,
    pub params: Vec<String>,
    pub name: CMD,
}

#[derive(Debug, Clone)]
pub struct CommandReplyOptions {
    pub result: bool,
    pub message: String,
    pub param: String,
    pub use_job_id: bool,
}

lazy_static! {
    pub static ref CMD_PARSE_OPTIONS: HashMap<String, CommandParseOptions> = {
        let mut m = HashMap::new();
        m.insert(CMD::Use.to_string(), CommandParseOptions{
            name: CMD::Use,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["tube".to_string()],
        });
        m.insert(CMD::Put.to_string(), CommandParseOptions{
            name: CMD::Put,
            expected_length: 5,
            waiting_for_more: true,
            params: vec!["pri".to_string(), "delay".to_string(), "ttr".to_string(), "bytes".to_string()],
        });
        m.insert(CMD::Watch.to_string(), CommandParseOptions{
            name: CMD::Watch,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["tube".to_string()]});
        m.insert(CMD::Ignore.to_string(), CommandParseOptions{
            name: CMD::Ignore,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["tube".to_string()],
        });
        m.insert(CMD::Reserve.to_string(), CommandParseOptions{
            name: CMD::Reserve,
            expected_length: 1,
            waiting_for_more: false,
            params: vec![],
        });
        m.insert(CMD::ReserveWithTimeout.to_string(), CommandParseOptions{
            name: CMD::ReserveWithTimeout,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["timeout".to_string()],
        });
        m.insert(CMD::Delete.to_string(), CommandParseOptions{
            name: CMD::Delete,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["id".to_string()],
        });
        m.insert(CMD::Release.to_string(), CommandParseOptions{
            name: CMD::Release,
            expected_length: 4,
            waiting_for_more: false,
            params: vec!["id".to_string(), "pri".to_string(), "delay".to_string()],
        });
        m.insert(CMD::Bury.to_string(), CommandParseOptions{
            name: CMD::Bury,
            expected_length: 3,
            waiting_for_more: false,
            params: vec!["id".to_string(), "pri".to_string()],
        });
        m.insert(CMD::Touch.to_string(), CommandParseOptions{
            name: CMD::Touch,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["id".to_string()],
        });
        m.insert(CMD::Quit.to_string(), CommandParseOptions{
            name: CMD::Quit,
            expected_length: 1,
            waiting_for_more: false,
            params: vec![],
        });
        m.insert(CMD::Kick.to_string(), CommandParseOptions {
            name: CMD::Kick,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["bound".to_string()],
        });
        m.insert(CMD::KickJob.to_string(), CommandParseOptions{
            name: CMD::KickJob,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["id".to_string()],
        });
        m.insert(CMD::Peek.to_string(), CommandParseOptions{
            name: CMD::Peek,
            expected_length: 2,
            waiting_for_more: false,
            params: vec!["id".to_string()],
        });
        m.insert(CMD::ListTubesWatched.to_string(), CommandParseOptions{
            name: CMD::ListTubesWatched,
            expected_length: 1,
            waiting_for_more: false,
            params: vec![],
        });
        m
    };

    pub static ref CMD_REPLY_OPTIONS: HashMap<String, CommandReplyOptions> = {
        let mut m = HashMap::new();
        m.insert(CMD::Use.to_string(), CommandReplyOptions{
            result: false,
            message: "USING".to_string(),
            param: "tube".to_string(),
            use_job_id: false,
        });
        m.insert(CMD::Put.to_string(), CommandReplyOptions{
            result:   false,
			message:  "INSERTED".to_string(),
			param:    "".to_string(),
			use_job_id: true,
		});
		m.insert(CMD::Watch.to_string(), CommandReplyOptions {
			result:   false,
			message:  "WATCHING".to_string(),
			param:    "count".to_string(),
			use_job_id: false,
		});
		m.insert(CMD::Ignore.to_string(), CommandReplyOptions{
			result:   false,
			message:  "WATCHING".to_string(),
			param:    "count".to_string(),
			use_job_id: false,
		});
		m.insert(CMD::Delete.to_string(), CommandReplyOptions{
			result:   false,
			message:  "DELETED".to_string(),
			param:    "".to_string(),
			use_job_id: false,
		});
		m.insert(CMD::Release.to_string(), CommandReplyOptions{
			result:   false,
			message:  "RELEASED".to_string(),
			param:    "".to_string(),
			use_job_id: false,
		});
	    m.insert(CMD::Bury.to_string(), CommandReplyOptions{
			result:   false,
			message:  "BURIED".to_string(),
			param:    "".to_string(),
			use_job_id: false,
		});
		m.insert(CMD::Touch.to_string(), CommandReplyOptions{
			result:   false,
			message:  "INSERTED".to_string(),
			param:    "".to_string(),
			use_job_id: true,
		});
		m.insert(CMD::Kick.to_string(), CommandReplyOptions{
			result:   false,
			message:  "KICKED".to_string(),
			param:    "".to_string(),
			use_job_id: false,
		});
		m.insert(CMD::KickJob.to_string(), CommandReplyOptions{
			result:   false,
			message:  "KICKED".to_string(),
			param:    "".to_string(),
			use_job_id: true,
		});
        m
    };
}