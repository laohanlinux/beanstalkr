//! Tube name validation per beanstalkd protocol.

use crate::client::error::NameError;

/// Allowed name characters in the beanstalkd protocol.
pub const NAME_CHARS: &str = r#"\-\+/;.$_()0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"#;

/// Maximum tube name length.
pub const MAX_NAME_LEN: usize = 200;

/// Validates a tube name according to the beanstalkd protocol.
pub fn check_name(s: &str) -> Result<(), NameError> {
    if s.is_empty() {
        return Err(NameError::Empty(s.to_string()));
    }
    if s.len() >= MAX_NAME_LEN {
        return Err(NameError::TooLong(s.to_string()));
    }
    if !contains_only(s, NAME_CHARS) {
        return Err(NameError::BadChar(s.to_string()));
    }
    Ok(())
}

fn contains_only(s: &str, chars: &str) -> bool {
    for c in s.chars() {
        if !chars.contains(c) {
            return false;
        }
    }
    true
}
