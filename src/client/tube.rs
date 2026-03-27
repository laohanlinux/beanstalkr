//! A single tube on the server.

use std::collections::HashMap;
use std::time::Duration;

use crate::client::conn::{BodyReadMode, Conn};
use crate::client::error::ClientError;
use crate::client::parse;

/// Represents a single tube on the beanstalkd server.
#[derive(Clone, Debug)]
pub struct Tube {
    name: String,
}

impl Tube {
    pub(crate) fn new(_conn: &Conn, name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    /// Create a Tube with the given name (without a connection).
    pub fn named(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Tube name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Put a job into this tube.
    pub async fn put(
        &self,
        conn: &mut Conn,
        body: &[u8],
        pri: u32,
        delay: Duration,
        ttr: Duration,
    ) -> Result<u64, ClientError> {
        if delay.as_secs_f64() < 0.0 || ttr.as_secs_f64() < 0.0 {
            return Err(ClientError::NegativeDuration);
        }
        conn.cmd(
            Some(&self.name),
            None,
            Some(body),
            "put",
            &[
                pri.to_string(),
                delay.as_secs().to_string(),
                ttr.as_secs().to_string(),
            ],
        )
        .await?;
        let (line, _) = conn
            .read_resp(false, "INSERTED ", "put", BodyReadMode::JobBody)
            .await?;
        let id = line
            .strip_prefix("INSERTED ")
            .and_then(|s| s.trim().parse().ok())
            .ok_or_else(|| crate::client::error::find_resp_error(&line))?;
        Ok(id)
    }

    /// Peek at the ready job at the front of the queue.
    pub async fn peek_ready(&self, conn: &mut Conn) -> Result<(u64, Vec<u8>), ClientError> {
        conn.cmd(Some(&self.name), None, None, "peek-ready", &[]).await?;
        let (line, body) = conn
            .read_resp(true, "FOUND ", "peek-ready", BodyReadMode::JobBody)
            .await?;
        let id = parse_found_id(&line)?;
        Ok((id, body))
    }

    /// Peek at the next delayed job.
    pub async fn peek_delayed(&self, conn: &mut Conn) -> Result<(u64, Vec<u8>), ClientError> {
        conn.cmd(Some(&self.name), None, None, "peek-delayed", &[]).await?;
        let (line, body) = conn
            .read_resp(true, "FOUND ", "peek-delayed", BodyReadMode::JobBody)
            .await?;
        let id = parse_found_id(&line)?;
        Ok((id, body))
    }

    /// Peek at the next buried job.
    pub async fn peek_buried(&self, conn: &mut Conn) -> Result<(u64, Vec<u8>), ClientError> {
        conn.cmd(Some(&self.name), None, None, "peek-buried", &[]).await?;
        let (line, body) = conn
            .read_resp(true, "FOUND ", "peek-buried", BodyReadMode::JobBody)
            .await?;
        let id = parse_found_id(&line)?;
        Ok((id, body))
    }

    /// Kick up to `bound` jobs from the buried queue to ready.
    pub async fn kick(&self, conn: &mut Conn, bound: u32) -> Result<u32, ClientError> {
        conn.cmd(Some(&self.name), None, None, "kick", &[bound.to_string()])
            .await?;
        let (line, _) = conn
            .read_resp(false, "KICKED ", "kick", BodyReadMode::JobBody)
            .await?;
        let n = line
            .strip_prefix("KICKED ")
            .and_then(|s| s.trim().parse().ok())
            .ok_or_else(|| crate::client::error::find_resp_error(&line))?;
        Ok(n)
    }

    /// Get tube statistics.
    pub async fn stats(&self, conn: &mut Conn) -> Result<HashMap<String, String>, ClientError> {
        conn.cmd(None, None, None, "stats-tube", &[self.name.clone()])
            .await?;
        let (_, body) = conn
            .read_resp(true, "OK", "stats-tube", BodyReadMode::YamlWithCrlf)
            .await?;
        Ok(parse::parse_dict(&body))
    }

    /// Pause new reservations for the given duration.
    pub async fn pause(&self, conn: &mut Conn, delay: Duration) -> Result<(), ClientError> {
        if delay.as_secs_f64() < 0.0 {
            return Err(ClientError::NegativeDuration);
        }
        conn.cmd(
            None,
            None,
            None,
            "pause-tube",
            &[self.name.clone(), delay.as_secs().to_string()],
        )
        .await?;
        conn.read_resp(false, "PAUSED", "pause-tube", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }
}

fn parse_found_id(line: &str) -> Result<u64, ClientError> {
    let rest = line
        .strip_prefix("FOUND ")
        .ok_or_else(|| crate::client::error::find_resp_error(line))?;
    let id_str = rest.split_whitespace().next().unwrap_or(rest);
    id_str
        .parse()
        .map_err(|_| crate::client::error::find_resp_error(line))
}
