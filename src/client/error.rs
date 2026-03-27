//! Error types for the beanstalk client.

use thiserror::Error;

/// Errors returned by the beanstalk server in response to commands.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeanstalkError {
    #[error("bad command format")]
    BadFormat,
    #[error("buried")]
    Buried,
    #[error("deadline soon")]
    DeadlineSoon,
    #[error("draining")]
    Draining,
    #[error("expected CR LF")]
    ExpectedCrlf,
    #[error("internal error")]
    Internal,
    #[error("job too big")]
    JobTooBig,
    #[error("not found")]
    NotFound,
    #[error("not ignored")]
    NotIgnored,
    #[error("server is out of memory")]
    OutOfMemory,
    #[error("timeout")]
    Timeout,
    #[error("unknown command")]
    UnknownCommand,
}

/// Name validation errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NameError {
    #[error("name is empty: {0}")]
    Empty(String),
    #[error("name has bad char: {0}")]
    BadChar(String),
    #[error("name is too long: {0}")]
    TooLong(String),
}

/// Client error wrapping I/O and protocol errors.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{op}: {source}")]
    Conn {
        op: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("protocol error: {0}")]
    Protocol(BeanstalkError),
    #[error("unknown response: {0}")]
    UnknownResponse(String),
    #[error("name error: {0}")]
    Name(#[from] NameError),
    #[error("duration must be non-negative")]
    NegativeDuration,
}

impl ClientError {
    pub fn conn(op: impl Into<String>, err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Conn {
            op: op.into(),
            source: err.into(),
        }
    }
}

/// Map server response string to protocol error.
pub fn find_resp_error(s: &str) -> ClientError {
    let err = match s {
        "BAD_FORMAT" => BeanstalkError::BadFormat,
        "BURIED" => BeanstalkError::Buried,
        "DEADLINE_SOON" => BeanstalkError::DeadlineSoon,
        "DRAINING" => BeanstalkError::Draining,
        "EXPECTED_CRLF" => BeanstalkError::ExpectedCrlf,
        "INTERNAL_ERROR" => BeanstalkError::Internal,
        "JOB_TOO_BIG" => BeanstalkError::JobTooBig,
        "NOT_FOUND" => BeanstalkError::NotFound,
        "NOT_IGNORED" => BeanstalkError::NotIgnored,
        "OUT_OF_MEMORY" => BeanstalkError::OutOfMemory,
        "TIMED_OUT" => BeanstalkError::Timeout,
        "UNKNOWN_COMMAND" => BeanstalkError::UnknownCommand,
        _ => return ClientError::UnknownResponse(s.to_string()),
    };
    ClientError::Protocol(err)
}
