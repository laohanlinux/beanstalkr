//! 工具模块
//!
//! 提供各种辅助功能，包括 socket 配置、时间处理等

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket};
use tracing::debug;

/// 创建配置好的 TCP 监听器
///
/// 设置以下 socket 选项（与 C 版本 beanstalkd 一致）：
/// - SO_REUSEADDR - 允许地址重用
/// - SO_KEEPALIVE - 保持连接活跃
/// - TCP_NODELAY - 禁用 Nagle 算法
pub async fn create_server_socket(addr: &str) -> io::Result<TcpListener> {
    let socket_addr: SocketAddr = addr.parse().map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid address: {}", e))
    })?;

    let socket = if socket_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    // SO_REUSEADDR
    socket.set_reuseaddr(true)?;
    
    // SO_KEEPALIVE
    socket.set_keepalive(true)?;
    
    // 绑定地址
    socket.bind(socket_addr)?;
    
    debug!("Socket bound to {}", socket_addr);
    
    // 监听，backlog 设置为 1024（与 C 版本一致）
    let listener = socket.listen(1024)?;
    
    // 设置 TCP_NODELAY（需要在 accepted 连接上设置）
    // 这个会在 ClientHandler 中处理
    
    Ok(listener)
}

/// 配置 accepted 连接的 socket 选项
///
/// 需要在每个新的 TCP 连接上设置：
/// - TCP_NODELAY - 禁用 Nagle 算法，减少延迟
pub fn configure_client_socket(stream: &tokio::net::TcpStream) -> io::Result<()> {
    // TCP_NODELAY
    stream.set_nodelay(true)?;
    
    Ok(())
}

/// 获取纳秒级时间戳
///
/// 对应 C 版本的 nanoseconds() 函数
pub fn nanoseconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// 获取微秒级时间戳
pub fn microseconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// 获取当前时间（秒）
pub fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// 格式化持续时间（用于日志）
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    
    if secs > 0 {
        format!("{}.{:03}s", secs, millis)
    } else {
        format!("{}ms", millis)
    }
}

/// 警告宏（类似 C 版本的 twarn/twarnx）
#[macro_export]
macro_rules! warnx {
    ($fmt:expr) => {
        tracing::warn!($fmt)
    };
    ($fmt:expr, $($arg:tt)*) => {
        tracing::warn!($fmt, $($arg)*)
    };
}

/// 带 errno 的警告（类似 C 版本的 twarn）
#[macro_export]
macro_rules! warn {
    ($fmt:expr) => {
        tracing::warn!("{}: {}", $fmt, std::io::Error::last_os_error())
    };
    ($fmt:expr, $($arg:tt)*) => {
        tracing::warn!("{}: {}", format!($fmt, $($arg)*), std::io::Error::last_os_error())
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_nanoseconds() {
        let t1 = nanoseconds();
        std::thread::sleep(Duration::from_millis(1));
        let t2 = nanoseconds();
        assert!(t2 > t1);
    }
    
    #[test]
    fn test_now_secs() {
        let t1 = now_secs();
        std::thread::sleep(Duration::from_millis(10));
        let t2 = now_secs();
        assert!(t2 >= t1);
    }
}
