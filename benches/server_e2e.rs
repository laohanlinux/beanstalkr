//! 端到端 TCP 基准：子进程启动真实 `beanstalkr`，经协议 put / reserve / delete。
//!
//! 需要先能运行服务端二进制（release 优先）：
//!   cargo build --release --bin beanstalkr
//!
//! 运行：
//!   cargo bench --bench server_e2e
//!
//! 在 macOS 上，「每轮新建 TCP」类基准若在几秒内建立大量连接，临时端口会堆在 `TIME_WAIT` 中，
//! 进而出现 `connect` 失败 `EADDRNOTAVAIL` (49)。本文件对 bench 用连接设置 `SO_LINGER=0`，
//! 关闭时发 RST，减轻端口耗尽。

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

use beanstalkr::client::{Conn, Tube, TubeSet};
use criterion::Throughput;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use socket2::SockRef;

/// 与 `Conn::connect_timeout` 类似，但设置 `SO_LINGER=0`，减轻短连接风暴下的临时端口耗尽。
async fn bench_connect(addr: SocketAddr, timeout: Duration) -> Conn {
    let stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr))
        .await
        .expect("connect deadline")
        .expect("tcp connect");
    let std_stream = stream.into_std().expect("into_std");
    let _ = SockRef::from(&std_stream).set_linger(Some(Duration::from_secs(0)));
    let stream = tokio::net::TcpStream::from_std(std_stream).expect("from_std");
    Conn::new(stream)
}

fn server_binary() -> PathBuf {
    std::env::var_os("CARGO_BIN_EXE_beanstalkr")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let release = dir.join("target/release/beanstalkr");
            if release.exists() {
                return release;
            }
            dir.join("target/debug/beanstalkr")
        })
}

/// 子进程 beanstalkr，绑定随机端口；`-z 1MiB` 便于大包体场景贴近生产配置。
struct BeanstalkdChild {
    child: Child,
    addr: SocketAddr,
}

impl BeanstalkdChild {
    fn spawn() -> Self {
        let bin = server_binary();
        assert!(
            bin.exists(),
            "beanstalkr binary not found at {}. Run `cargo build --release --bin beanstalkr`.",
            bin.display()
        );

        let port = {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
            let p = l.local_addr().expect("local_addr").port();
            drop(l);
            thread::sleep(Duration::from_millis(10));
            p
        };

        let child = Command::new(&bin)
            .args([
                "-l",
                "127.0.0.1",
                "-p",
                &port.to_string(),
                "-z",
                "1048576",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn beanstalkr");

        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        for _ in 0..100 {
            thread::sleep(Duration::from_millis(50));
            if TcpStream::connect(addr).is_ok() {
                thread::sleep(Duration::from_millis(150));
                return Self { child, addr };
            }
        }
        panic!("beanstalkr did not accept connections on {addr}");
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for BeanstalkdChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        thread::sleep(Duration::from_millis(50));
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

fn payload(len: usize) -> Vec<u8> {
    vec![0x5Au8; len]
}

fn e2e_tcp_production(c: &mut Criterion) {
    let _srv = BeanstalkdChild::spawn();
    let addr = _srv.addr();
    let rt = runtime();
    let ttr = Duration::from_secs(120);

    {
        let mut group = c.benchmark_group("e2e_tcp/persistent_conn");
        group.sample_size(25);
        group.measurement_time(Duration::from_secs(8));

        let mut conn = rt.block_on(bench_connect(addr, Duration::from_secs(5)));

        group.bench_function("persistent_conn_put_reserve_delete", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let id = conn
                        .put(
                            black_box(b"x".as_slice()),
                            1,
                            Duration::ZERO,
                            ttr,
                        )
                        .await
                        .expect("put");
                    let (rid, _body) = conn
                        .reserve(Duration::from_secs(5))
                        .await
                        .expect("reserve");
                    assert_eq!(id, rid);
                    conn.delete(rid).await.expect("delete");
                });
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("e2e_tcp/new_conn");
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(10));

        group.bench_function("new_tcp_conn_each_iteration", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut conn = bench_connect(addr, Duration::from_secs(3)).await;
                    let id = conn
                        .put(b"x", 1, Duration::ZERO, ttr)
                        .await
                        .expect("put");
                    let (rid, _) = conn
                        .reserve(Duration::from_secs(5))
                        .await
                        .expect("reserve");
                    assert_eq!(id, rid);
                    conn.delete(rid).await.expect("delete");
                });
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("e2e_tcp/batch");
        group.sample_size(15);
        group.measurement_time(Duration::from_secs(12));
        group.throughput(Throughput::Elements(32));

        group.bench_function("two_conns_put32_reserve32_delete32", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut prod = bench_connect(addr, Duration::from_secs(3)).await;
                    let mut cons = bench_connect(addr, Duration::from_secs(3)).await;
                    for _ in 0..32 {
                        prod.put(b"j", 1, Duration::ZERO, ttr)
                            .await
                            .expect("put");
                    }
                    for _ in 0..32 {
                        let (id, _) = cons
                            .reserve(Duration::from_secs(5))
                            .await
                            .expect("reserve");
                        cons.delete(id).await.expect("delete");
                    }
                });
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("e2e_tcp/payload");
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(8));
        let body_8k = payload(8192);
        let mut conn = rt.block_on(bench_connect(addr, Duration::from_secs(5)));

        group.bench_function("persistent_8KiB_body", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let id = conn
                        .put(black_box(body_8k.as_slice()), 1, Duration::ZERO, ttr)
                        .await
                        .expect("put");
                    let (rid, body) = conn
                        .reserve(Duration::from_secs(5))
                        .await
                        .expect("reserve");
                    black_box(body.len());
                    assert_eq!(id, rid);
                    conn.delete(rid).await.expect("delete");
                });
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("e2e_tcp/named_tube");
        group.sample_size(20);
        group.measurement_time(Duration::from_secs(8));

        let tube = Tube::named("bench_prod_tube");
        // 本实现对 `ignore default` 返回 NOT_IGNORED，因此 worker 需保留对 default 的 watch（与多 tube 生产配置一致）。
        let watched = TubeSet::with_tubes(&["default", "bench_prod_tube"]);

        let mut prod = rt.block_on(bench_connect(addr, Duration::from_secs(5)));
        let mut cons = rt.block_on(bench_connect(addr, Duration::from_secs(5)));

        group.bench_function("producer_consumer_different_tubes", |b| {
            b.iter(|| {
                rt.block_on(async {
                    tube.put(&mut prod, b"job", 1, Duration::ZERO, ttr)
                        .await
                        .expect("put");
                    let (id, _) = watched
                        .reserve(&mut cons, Duration::from_secs(5))
                        .await
                        .expect("reserve");
                    cons.delete(id).await.expect("delete");
                });
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("e2e_tcp/concurrent");
        group.sample_size(12);
        group.measurement_time(Duration::from_secs(15));
        group.throughput(Throughput::Elements(32));

        group.bench_function("four_producers_8_puts_each_then_single_consumer_drain", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::new();
                    for _ in 0..4 {
                        handles.push(tokio::spawn(async move {
                            let mut c = bench_connect(addr, Duration::from_secs(3)).await;
                            for _ in 0..8 {
                                c.put(b"p", 1, Duration::ZERO, ttr)
                                    .await
                                    .expect("put");
                            }
                        }));
                    }
                    for h in handles {
                        h.await.expect("producer join");
                    }
                    let mut c = bench_connect(addr, Duration::from_secs(3)).await;
                    for _ in 0..32 {
                        let (id, _) = c.reserve(Duration::from_secs(5)).await.expect("reserve");
                        c.delete(id).await.expect("delete");
                    }
                });
            });
        });
        group.finish();
    }
}

criterion_group!(benches, e2e_tcp_production);
criterion_main!(benches);
