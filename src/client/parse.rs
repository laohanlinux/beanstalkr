//! Response body parsing (YAML-like dict/list) and size extraction.

use std::collections::HashMap;

use crate::client::error::{find_resp_error, ClientError};

const YAML_HEAD: &[u8] = b"---\n";
const NL: u8 = b'\n';
const COLON_SPACE: &[u8] = b": ";
const MINUS_SPACE: &[u8] = b"- ";

/// Parses a YAML-like dict from stats/list-tubes response body.
pub fn parse_dict(dat: &[u8]) -> HashMap<String, String> {
    if dat.is_empty() {
        return HashMap::new();
    }
    let mut dat = dat;
    if dat.starts_with(YAML_HEAD) {
        dat = &dat[4..];
    }
    let mut d = HashMap::new();
    for line in dat.split(|&b| b == NL) {
        if let Some(colon_pos) = line.iter().position(|&b| b == b':') {
            if colon_pos + 2 <= line.len() && &line[colon_pos..colon_pos + 2] == COLON_SPACE {
                let key = String::from_utf8_lossy(&line[..colon_pos]).to_string();
                let val = String::from_utf8_lossy(&line[colon_pos + 2..]).to_string();
                d.insert(key, val);
            }
        }
    }
    d
}

/// Parses a YAML-like list from list-tubes response body.
pub fn parse_list(dat: &[u8]) -> Vec<String> {
    if dat.is_empty() {
        return vec![];
    }
    let mut dat = dat;
    if dat.starts_with(YAML_HEAD) {
        dat = &dat[4..];
    }
    let mut list = Vec::new();
    for line in dat.split(|&b| b == NL) {
        if line.starts_with(MINUS_SPACE) && line.len() >= 2 {
            list.push(String::from_utf8_lossy(&line[2..]).to_string());
        }
    }
    list
}

/// Extracts body size from response line (last space-separated token).
/// Returns (line_without_size, size).
/// For error responses (e.g. TIMED_OUT) with no body, returns error.
pub fn parse_size(s: &str) -> Result<(&str, usize), ClientError> {
    let s = s.trim();
    let i = s.rfind(' ').ok_or_else(|| find_resp_error(s))?;
    let rest = s[i + 1..].trim();
    let n: usize = rest
        .parse()
        .map_err(|_| find_resp_error(s))?;
    Ok((s[..i].trim_end(), n))
}
