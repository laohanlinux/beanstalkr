use failure::{Fail, Error};

#[derive(Debug, Fail)]
pub enum TransitionError {
    #[fail(display = "failed to transition state from {} to Ready", _0)]
    Ready(&'static str),
    #[fail(display = "failed to transition state from {} to Delayed", _0)]
    Delayed(&'static str),
    #[fail(display = "failed to transition state from {} to Reserved", _0)]
    Reserved(&'static str),
    #[fail(display = "failed to transition state from {} to Buried", _0)]
    Buried(&'static str),
}

#[derive(Debug, Fail, Clone)]
pub enum ProtocolError {
    #[fail(display = "BAD_FORMAT")]
    BadFormat,
    #[fail(display = "UNKNOWN_COMMAND")]
    UnknownCommand,
    #[fail(display = "NOT_FOUND")]
    NotFound,
    #[fail(display = "NOT_IGNORED")]
    NotIgnored,
    #[fail(display = "EXPECTED_CRLF")]
    ExpectedCrlf,
    #[fail(display = "JOB_TOO_BIG")]
    JobTooBig,
    #[fail(display = "TIMED_OUT")]
    TimedOut,
}