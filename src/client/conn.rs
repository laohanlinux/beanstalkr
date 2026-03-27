//! Asynchronous connection to a beanstalkd server.

use std::collections::HashSet;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::client::error::{find_resp_error, ClientError};
use crate::client::name::check_name;
use crate::client::parse::parse_size;
use crate::client::tube::Tube;
use crate::client::tubeset::TubeSet;

/// Default connection timeout.
pub const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Body format after the response line (see beanstalkr `handle_reply`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyReadMode {
    /// RESERVED / FOUND job data: exact byte count, no `\r\n` after body.
    JobBody,
    /// OK … YAML: length bytes plus trailing `\r\n`.
    YamlWithCrlf,
}

/// Duration to seconds for protocol (beanstalk uses whole seconds).
fn dur_secs(d: Duration) -> u64 {
    d.as_secs()
}

/// Asynchronous connection to a beanstalkd server.
pub struct Conn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    used: String,
    watched: HashSet<String>,
}

impl Conn {
    /// Connect to the given address.
    pub async fn connect(addr: impl tokio::net::ToSocketAddrs) -> Result<Self, ClientError> {
        Self::connect_timeout(addr, DEFAULT_DIAL_TIMEOUT).await
    }

    /// Connect to the given address with a custom timeout.
    pub async fn connect_timeout(
        addr: impl tokio::net::ToSocketAddrs,
        timeout: Duration,
    ) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .map_err(|_| {
            ClientError::conn(
                "dial",
                std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timeout"),
            )
        })?
        .map_err(|e| ClientError::conn("dial", e))?;
        Ok(Self::new(stream))
    }

    /// Create a Conn from an existing TcpStream.
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        let reader = BufReader::new(reader);
        let mut watched = HashSet::new();
        watched.insert("default".to_string());
        Self {
            reader,
            writer,
            used: "default".to_string(),
            watched,
        }
    }

    /// Returns a Tube for the default tube.
    pub fn tube(&self) -> Tube {
        Tube::new(self, "default")
    }

    /// Returns a Tube for the given name.
    pub fn tube_named(&self, name: &str) -> Tube {
        Tube::new(self, name)
    }

    /// Returns a TubeSet for the given tube names.
    pub fn tube_set(&self, names: &[&str]) -> TubeSet {
        TubeSet::new(self, names)
    }

    /// Returns the name of the tube currently being used.
    pub fn using(&self) -> &str {
        &self.used
    }

    /// Close the connection gracefully.
    pub async fn close(&mut self) -> Result<(), ClientError> {
        use tokio::io::AsyncWriteExt;
        self.writer.shutdown().await?;
        Ok(())
    }

    async fn print_line(&mut self, cmd: &str, args: &[String]) -> Result<(), ClientError> {
        self.writer.write_all(cmd.as_bytes()).await?;
        for a in args {
            self.writer.write_all(b" ").await?;
            self.writer.write_all(a.as_bytes()).await?;
        }
        self.writer.write_all(b"\r\n").await?;
        Ok(())
    }

    async fn adjust_tubes(
        &mut self,
        tube_name: Option<&str>,
        tube_set_names: Option<&HashSet<String>>,
    ) -> Result<(), ClientError> {
        if let Some(name) = tube_name {
            if name != self.used {
                check_name(name)?;
                self.print_line("use", &[name.to_string()]).await?;
                self.used = name.to_string();
            }
        }
        if let Some(names) = tube_set_names {
            for s in names {
                if !self.watched.contains(s) {
                    check_name(s)?;
                    self.print_line("watch", &[s.clone()]).await?;
                }
            }
            for s in self.watched.clone().iter() {
                if !names.contains(s) {
                    self.print_line("ignore", &[s.clone()]).await?;
                }
            }
            self.watched = names.clone();
        }
        Ok(())
    }

    pub(crate) async fn cmd(
        &mut self,
        tube_name: Option<&str>,
        tube_set_names: Option<&HashSet<String>>,
        body: Option<&[u8]>,
        op: &str,
        args: &[String],
    ) -> Result<(), ClientError> {
        self.adjust_tubes(tube_name, tube_set_names).await?;
        if let Some(b) = body {
            let mut all_args = args.to_vec();
            all_args.push(b.len().to_string());
            self.print_line(op, &all_args).await?;
            self.writer.write_all(b).await?;
            self.writer.write_all(b"\r\n").await?;
        } else {
            self.print_line(op, args).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    /// How to read the body after the first line.
    ///
    /// - [`JobBody`](BodyReadMode::JobBody): RESERVED/FOUND — exactly `size` bytes (no trailing CRLF;
    ///   matches beanstalkr `handle_reply` for reserve/peek).
    /// - [`YamlWithCrlf`](BodyReadMode::YamlWithCrlf): OK … — `size` bytes + `\r\n` (classic beanstalkd YAML).
    pub(crate) async fn read_resp(
        &mut self,
        read_body: bool,
        expect_prefix: &str,
        op: &str,
        body_mode: BodyReadMode,
    ) -> Result<(String, Vec<u8>), ClientError> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = self.reader.read_line(&mut line).await?;
            if n == 0 {
                return Err(ClientError::conn(op, "connection closed"));
            }
            let line = line.trim_end_matches(|c| c == '\r' || c == '\n').to_string();
            if !line.starts_with("WATCHING ") && !line.starts_with("USING ") {
                break;
            }
        }

        let to_scan = line;
        let body = if read_body {
            let (_remaining, size) = parse_size(&to_scan)?;
            // Must read through `BufReader`, not `get_mut()`: data after the line may
            // already be buffered by `read_line`.
            let buf = match body_mode {
                BodyReadMode::JobBody => {
                    let mut buf = vec![0u8; size];
                    tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf)
                        .await
                        .map_err(|e| ClientError::conn(op, e))?;
                    buf
                }
                BodyReadMode::YamlWithCrlf => {
                    let mut buf = vec![0u8; size + 2];
                    tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf)
                        .await
                        .map_err(|e| ClientError::conn(op, e))?;
                    buf.truncate(size);
                    buf
                }
            };
            buf
        } else {
            vec![]
        };

        scan_response(&to_scan, expect_prefix, op)?;
        Ok((to_scan, body))
    }

    /// Put a job into the default tube.
    pub async fn put(
        &mut self,
        body: &[u8],
        pri: u32,
        delay: Duration,
        ttr: Duration,
    ) -> Result<u64, ClientError> {
        self.tube().put(self, body, pri, delay, ttr).await
    }

    /// Reserve a job from the default tube.
    pub async fn reserve(
        &mut self,
        timeout: Duration,
    ) -> Result<(u64, Vec<u8>), ClientError> {
        self.tube_set(&["default"]).reserve(self, timeout).await
    }

    /// Delete the given job.
    pub async fn delete(&mut self, id: u64) -> Result<(), ClientError> {
        self.cmd(None, None, None, "delete", &[id.to_string()]).await?;
        self.read_resp(false, "DELETED", "delete", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }

    /// Release a job back to the ready queue.
    pub async fn release(
        &mut self,
        id: u64,
        pri: u32,
        delay: Duration,
    ) -> Result<(), ClientError> {
        self.cmd(
            None,
            None,
            None,
            "release",
            &[id.to_string(), pri.to_string(), dur_secs(delay).to_string()],
        )
        .await?;
        self.read_resp(false, "RELEASED", "release", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }

    /// Bury a job in the holding area.
    pub async fn bury(&mut self, id: u64, pri: u32) -> Result<(), ClientError> {
        self.cmd(None, None, None, "bury", &[id.to_string(), pri.to_string()])
            .await?;
        self.read_resp(false, "BURIED", "bury", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }

    /// Kick a specific job to the ready queue.
    pub async fn kick_job(&mut self, id: u64) -> Result<(), ClientError> {
        self.cmd(None, None, None, "kick-job", &[id.to_string()]).await?;
        self.read_resp(false, "KICKED", "kick-job", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }

    /// Touch resets the reservation timer for the given job.
    pub async fn touch(&mut self, id: u64) -> Result<(), ClientError> {
        self.cmd(None, None, None, "touch", &[id.to_string()]).await?;
        self.read_resp(false, "TOUCHED", "touch", BodyReadMode::JobBody)
            .await?;
        Ok(())
    }

    /// Peek at a job by id.
    pub async fn peek(&mut self, id: u64) -> Result<(u64, Vec<u8>), ClientError> {
        self.cmd(None, None, None, "peek", &[id.to_string()]).await?;
        let (line, body) = self
            .read_resp(true, "FOUND ", "peek", BodyReadMode::JobBody)
            .await?;
        let parsed_id = parse_found_id(&line)?;
        Ok((parsed_id, body))
    }

    /// Reserve a specific job by id.
    pub async fn reserve_job(&mut self, id: u64) -> Result<(u64, Vec<u8>), ClientError> {
        self.cmd(None, None, None, "reserve-job", &[id.to_string()]).await?;
        let (line, body) = self
            .read_resp(true, "RESERVED ", "reserve-job", BodyReadMode::JobBody)
            .await?;
        let parsed_id = parse_reserved_id(&line)?;
        Ok((parsed_id, body))
    }

    /// Get global server statistics.
    pub async fn stats(&mut self) -> Result<std::collections::HashMap<String, String>, ClientError> {
        self.cmd(None, None, None, "stats", &[]).await?;
        let (_, body) = self
            .read_resp(true, "OK", "stats", BodyReadMode::YamlWithCrlf)
            .await?;
        Ok(crate::client::parse::parse_dict(&body))
    }

    /// Get statistics for a specific job.
    pub async fn stats_job(
        &mut self,
        id: u64,
    ) -> Result<std::collections::HashMap<String, String>, ClientError> {
        self.cmd(None, None, None, "stats-job", &[id.to_string()]).await?;
        let (_, body) = self
            .read_resp(true, "OK", "stats-job", BodyReadMode::YamlWithCrlf)
            .await?;
        Ok(crate::client::parse::parse_dict(&body))
    }

    /// List all tubes on the server.
    pub async fn list_tubes(&mut self) -> Result<Vec<String>, ClientError> {
        self.cmd(None, None, None, "list-tubes", &[]).await?;
        let (_, body) = self
            .read_resp(true, "OK", "list-tubes", BodyReadMode::YamlWithCrlf)
            .await?;
        Ok(crate::client::parse::parse_list(&body))
    }
}

fn scan_response(line: &str, expect_prefix: &str, _op: &str) -> Result<(), ClientError> {
    if line.starts_with(expect_prefix) {
        return Ok(());
    }
    Err(find_resp_error(line))
}

/// Parse job id from "FOUND <id>" or "FOUND <id> <size>" (remaining after size stripped).
fn parse_found_id(line: &str) -> Result<u64, ClientError> {
    let rest = line.strip_prefix("FOUND ").ok_or_else(|| find_resp_error(line))?;
    let id_str = rest.split_whitespace().next().unwrap_or(rest);
    id_str.parse().map_err(|_| find_resp_error(line))
}

fn parse_reserved_id(line: &str) -> Result<u64, ClientError> {
    let rest = line.strip_prefix("RESERVED ").ok_or_else(|| find_resp_error(line))?;
    let id_str = rest.split_whitespace().next().unwrap_or(rest);
    id_str.parse().map_err(|_| find_resp_error(line))
}
