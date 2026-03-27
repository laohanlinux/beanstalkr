use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransitionError {
    #[error("failed to transition state from {0} to Ready")]
    Ready(&'static str),
    #[error("failed to transition state from {0} to Delayed")]
    Delayed(&'static str),
    #[error("failed to transition state from {0} to Reserved")]
    Reserved(&'static str),
    #[error("failed to transition state from {0} to Buried")]
    Buried(&'static str),
}

#[derive(Debug, Error, Clone)]
pub enum ProtocolError {
    #[error("BAD_FORMAT")]
    BadFormat,
    #[error("UNKNOWN_COMMAND")]
    UnknownCommand,
    #[error("NOT_FOUND")]
    NotFound,
    #[error("NOT_IGNORED")]
    NotIgnored,
    #[error("EXPECTED_CRLF")]
    ExpectedCrlf,
    #[error("JOB_TOO_BIG")]
    JobTooBig,
    #[error("TIMED_OUT")]
    TimedOut,
    #[error("DEADLINE_SOON")]
    DeadlineSoon,
    #[error("DRAINING")]
    Draining,
    #[error("BURIED")]
    Buried,
    #[error("NOT_KICKABLE")]
    NotKickable,
    #[error("OUT_OF_MEMORY")]
    OutOfMemory,
    #[error("INTERNAL_ERROR")]
    InternalError,
}
