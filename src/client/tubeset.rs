//! A set of tubes for reserving jobs.

use std::collections::HashSet;
use std::time::Duration;

use crate::client::conn::{BodyReadMode, Conn};
use crate::client::error::ClientError;

/// Represents a set of tubes for reserve operations.
#[derive(Clone, Debug)]
pub struct TubeSet {
    names: HashSet<String>,
}

impl TubeSet {
    pub(crate) fn new(_conn: &Conn, names: &[&str]) -> Self {
        Self {
            names: names.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Create a TubeSet with the given tube names.
    pub fn with_tubes(names: &[&str]) -> Self {
        Self {
            names: names.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Tube names in this set.
    pub fn names(&self) -> &HashSet<String> {
        &self.names
    }

    /// Reserve a job from one of the tubes in this set.
    /// Returns (job_id, body) or error on timeout.
    pub async fn reserve(
        &self,
        conn: &mut Conn,
        timeout: Duration,
    ) -> Result<(u64, Vec<u8>), ClientError> {
        if timeout.as_secs_f64() < 0.0 {
            return Err(ClientError::NegativeDuration);
        }
        conn.cmd(
            None,
            Some(&self.names),
            None,
            "reserve-with-timeout",
            &[timeout.as_secs().to_string()],
        )
        .await?;
        let (line, body) = conn
            .read_resp(
                true,
                "RESERVED ",
                "reserve-with-timeout",
                BodyReadMode::JobBody,
            )
            .await?;
        let id = line
            .strip_prefix("RESERVED ")
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| crate::client::error::find_resp_error(&line))?;
        Ok((id, body))
    }
}
