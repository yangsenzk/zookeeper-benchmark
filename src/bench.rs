use std::sync::mpsc;
use std::thread;
use std::time;

use rand::Rng;
use ratelimit;
use zookeeper_zk::{Acl, CreateMode, WatchedEvent, Watcher, ZkError, ZooKeeper, ZooKeeperExt};

use crate::cmd::Cli;
use crate::consts::BENCH_ROOT;
use crate::latency::RequestLatency;
use crate::model::BenchRes;

struct LoggingWatcher;

impl Watcher for LoggingWatcher {
    fn handle(&self, _e: WatchedEvent) {}
}

/// divide node_num into client_num parts, returns the vec of start and end.
fn divide_batch(node_num: i32, client_num: i32) -> Vec<(i32, i32)> {
    let (minimal_per_client, remainder) = (node_num / client_num, node_num % client_num);
    let mut result = Vec::new();
    let (mut start, mut i) = (0, 0);
    while start < node_num {
        let mut cur_client = minimal_per_client;
        if i < remainder {
            cur_client = minimal_per_client + 1;
            i += 1;
        }
        result.push((start, start + cur_client));
        start += cur_client;
    }
    result
}

// pre-create some znode with data-sized value
pub fn pre_create(params: &Cli) {
    let mut zk = connect_zk(params.address.as_str()).unwrap();

    // 如果是多客户端节点并行压测时,则只需要一个客户端进行预先创建节点
    // 先删除测试目录
    _ = zk.delete_recursive(BENCH_ROOT);

    // 创建测试根node
    zk.ensure_path(BENCH_ROOT).unwrap();

    // 限制qps
    let mut limiter: Option<ratelimit::Ratelimiter> = None;
    if params.qps_per_conn > 0 {
        limiter = Some(ratelimit::Ratelimiter::new(
            100,
            1,
            params.qps_per_conn as u64,
        ));
    }

    // 创建子节点
    let mut i = 0;
    while i < params.node_num {
        let path = format!("{}/{:0>10}", BENCH_ROOT, i);
        match limiter {
            Some(ref l) => {
                l.wait();
            }
            None => {}
        }
        let result = zk.create(
            path.as_str(),
            vec![1; params.data_size as usize],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        );
        match result {
            Ok(_) => {
                i += 1;
            }
            Err(e) => match e {
                ZkError::NodeExists => {
                    i += 1;
                }
                _ => {
                    _ = zk.close();
                    zk = connect_zk(params.address.as_str()).unwrap();
                }
            },
        }
    }
    _ = zk.close();
}

// 清理压测数据
pub fn post_clean(params: &Cli) {
    let mut zk = connect_zk(params.address.as_str()).unwrap();

    // 限速
    let mut limiter: Option<ratelimit::Ratelimiter> = None;
    if params.qps_per_conn > 0 {
        limiter = Some(ratelimit::Ratelimiter::new(
            50,
            1,
            params.qps_per_conn as u64,
        ));
    }
    // 顺序删除/bench_root下面的节点
    let mut i = 0;
    while i < params.node_num {
        match limiter {
            Some(ref l) => {
                l.wait();
            }
            None => {}
        }
        let path = format!("{}/{:0>10}", BENCH_ROOT, i);
        if i % 1000 == 0 {
            println!("now deleting node start with {:?}", path);
        }
        match zk.delete(path.as_str(), None) {
            Ok(_) => {
                i += 1;
            }
            Err(e) => match e {
                ZkError::NoNode => {
                    i += 1;
                }
                _ => {
                    println!("failed to delete node {}, error: {}", path, e);
                    _ = zk.close();
                    zk = connect_zk(params.address.as_str()).unwrap();
                }
            },
        }
    }
    println!(
        "finished deleting znode and trying to delete {} recursively...",
        BENCH_ROOT
    );
    // 再递归删除节点
    match zk.delete_recursive(BENCH_ROOT) {
        Ok(_) => {
            println!("delete {} recursively success...", BENCH_ROOT);
        }
        Err(e) => {
            println!("delete {} recursively failed with error: {}", BENCH_ROOT, e);
        }
    }
    _ = zk.close();
}

// 压测create操作
pub fn bench_create(params: &Cli) -> Option<BenchRes> {
    // // 先删除所有压测空间的znode
    // post_clean(params);

    // 创建根节点
    let zk = connect_zk(params.address.as_str()).unwrap();
    _ = zk.delete_recursive(BENCH_ROOT);
    zk.ensure_path(BENCH_ROOT).unwrap();
    let _ = zk.close();

    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];

    let batches = divide_batch(params.node_num, params.client_num);

    // 当前线程压测开始时间
    let start_time = time::Instant::now();
    println!("starting {} bench...", params.op);
    let mut thread_id = 0;
    for batch in batches {
        let thread_sender = tx.clone();
        let param = params.clone();
        let random_value: Vec<u8> = vec![0; param.data_size as usize];

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", thread_id))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str()).unwrap();
                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);
                // qps limiter
                let mut limiter: Option<ratelimit::Ratelimiter> = None;
                if param.qps_per_conn > 0 {
                    limiter = Some(ratelimit::Ratelimiter::new(
                        50,
                        1,
                        param.qps_per_conn as u64,
                    ));
                }
                let mut znode_index = batch.0;
                while znode_index < batch.1 {
                    match limiter {
                        Some(ref l) => {
                            l.wait();
                        }
                        None => {}
                    }
                    let path = format!("{}/{:0>10}", BENCH_ROOT, znode_index);

                    // 每条create请求的延迟
                    let start = time::Instant::now();
                    let result = zk_cli.create(
                        &path,
                        random_value.clone(),
                        Acl::open_unsafe().clone(),
                        CreateMode::Persistent,
                    );
                    match result {
                        Ok(_) => {
                            res.total_success += 1;
                            res.performance.insert(
                                param.op.to_string(),
                                time::Instant::now().duration_since(start).as_millis() as u32,
                            );
                            znode_index += 1;
                        }
                        Err(e) => {
                            match e {
                                ZkError::NodeExists => {
                                    res.total_success += 1;
                                    res.performance.insert(
                                        param.op.to_string(),
                                        time::Instant::now().duration_since(start).as_millis()
                                            as u32,
                                    );
                                    znode_index += 1;
                                }
                                _ => {
                                    println!(
                                        "failed to create znode {}, error: {}. Reconnecting...",
                                        path, e
                                    );
                                    res.total_failure += 1;
                                    _ = zk_cli.close(); // 先close一下,不管result
                                    zk_cli = connect_zk(param.address.as_str()).unwrap();
                                }
                            }
                        }
                    }
                }
                thread_sender.send(res).unwrap();
                _ = zk_cli.close();
            });
        handles.push(handle.unwrap());
        thread_id += 1;
    }
    for handle in handles {
        _ = handle.join();
    }
    drop(tx);

    let mut total_res = BenchRes::new(params.client_num, params.duration as i32);
    for thread_res in rx.iter() {
        // println!("cnt from tx: {:?}", serde_json::to_string(&thread_res).unwrap());
        total_res.total_success += thread_res.total_success;
        total_res.total_failure += thread_res.total_failure;
        total_res.performance.add(&thread_res.performance);
    }
    // 所有请求处理完成的实际时长
    let actual_duration = time::Instant::now().duration_since(start_time);
    total_res.duration = actual_duration.as_secs_f32();
    total_res.total_qps =
        total_res.total_success as f32 / (actual_duration.as_millis() as f32 / 1000.0);
    total_res
        .performance
        .cal_qps(actual_duration.as_millis() as u64);
    Some(total_res)
}

// 压测set操作
pub fn bench_set(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();
        let mut random_value: Vec<u8> = vec![0; param.data_size as usize];

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk = connect_zk(&param.address.as_str()).unwrap();

                let mut rng = rand::thread_rng();
                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);
                // 当前线程压测开始时间
                let start_time = time::Instant::now();
                // qps limiter
                let mut limiter: Option<ratelimit::Ratelimiter> = None;
                if param.qps_per_conn > 0 {
                    limiter = Some(ratelimit::Ratelimiter::new(
                        50,
                        1,
                        param.qps_per_conn as u64,
                    ));
                };
                loop {
                    match limiter {
                        Some(ref l) => {
                            l.wait();
                        }
                        None => {}
                    }
                    if time::Instant::now().duration_since(start_time).as_secs() > param.duration {
                        // 计算单个连接的QPS
                        thread_sender.send(res).unwrap();
                        break;
                    }

                    // 随机set某一个node
                    let idx: i32 = rng.gen_range(0..param.node_num);
                    let path = format!("{}/{:0>10}", BENCH_ROOT, idx);

                    // 随机改变value某一位的值
                    let random_index = rng.gen_range(0..param.data_size);
                    random_value[random_index as usize] = rng.gen_range(0..10) as u8;
                    let start = time::Instant::now();
                    let result = zk.set_data(&path, random_value.clone(), Some(-1));
                    match result {
                        Ok(_) => {
                            res.total_success += 1;
                            res.performance.insert(
                                param.op.to_string(),
                                time::Instant::now().duration_since(start).as_millis() as u32,
                            );
                        }
                        Err(e) => {
                            println!(
                                "failed to set znode {}, error: {}. Reconnecting...",
                                path, e
                            );
                            res.total_failure += 1;
                            _ = zk.close(); // 先close一下,不管result
                            match e {
                                ZkError::ConnectionLoss => {
                                    // 重连zk
                                    zk = connect_zk(param.address.as_str()).unwrap();
                                }
                                _ => {
                                    // 暂时还是重连zk
                                    zk = connect_zk(param.address.as_str()).unwrap();
                                }
                            }
                        }
                    }
                }
                _ = zk.close();
            });
        handles.push(handle.unwrap())
    }
    for handle in handles {
        _ = handle.join();
    }
    drop(tx);

    let mut total_res = BenchRes::new(params.client_num, params.duration as i32);
    for thread_res in rx.iter() {
        // println!("cnt from tx: {:?}", serde_json::to_string(&thread_res).unwrap());
        total_res.total_success += thread_res.total_success;
        total_res.total_failure += thread_res.total_failure;
        total_res.performance.add(&thread_res.performance);
    }
    total_res.total_qps = total_res.total_success as f32 / total_res.duration as f32;
    total_res.performance.cal_qps(params.duration * 1000);
    Some(total_res)
}

// 压测get操作
pub fn bench_get(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str()).unwrap();

                let mut rng = rand::thread_rng();
                // 当前线程的压测结果
                let mut res = BenchRes {
                    client_num: 1,
                    total_success: 0,
                    total_failure: 0,
                    performance: RequestLatency::new(),
                    duration: param.duration as f32,
                    total_qps: 0.0,
                };
                // 当前线程压测开始时间
                let start_time = time::Instant::now();

                // qps limiter
                let mut limiter: Option<ratelimit::Ratelimiter> = None;
                if param.qps_per_conn > 0 {
                    limiter = Some(ratelimit::Ratelimiter::new(
                        50,
                        1,
                        param.qps_per_conn as u64,
                    ));
                };
                loop {
                    match limiter {
                        Some(ref l) => {
                            l.wait();
                        }
                        None => {}
                    }
                    if time::Instant::now().duration_since(start_time).as_secs() > param.duration {
                        // 计算单个连接的QPS
                        thread_sender.send(res).unwrap();
                        break;
                    }

                    // 随机get一个znode
                    let idx: i32 = rng.gen_range(0..param.node_num);
                    let path = format!("{}/{:0>10}", BENCH_ROOT, idx);

                    let start = time::Instant::now();
                    let result = zk_cli.get_data(&path, false);
                    match result {
                        Ok(_) => {
                            res.total_success += 1;
                            res.performance.insert(
                                param.op.to_string(),
                                time::Instant::now().duration_since(start).as_millis() as u32,
                            );
                        }
                        Err(e) => {
                            println!(
                                "failed to set znode {}, error: {}. Reconnecting...",
                                path, e
                            );
                            res.total_failure += 1;
                            _ = zk_cli.close(); // 先close一下,不管result
                            match e {
                                ZkError::ConnectionLoss => {
                                    zk_cli = connect_zk(param.address.as_str()).unwrap();
                                    // 重连zk
                                }
                                _ => {
                                    // 暂时还是重连zk
                                    zk_cli = connect_zk(param.address.as_str()).unwrap();
                                }
                            }
                        }
                    }
                }
                _ = zk_cli.close();
            });
        handles.push(handle.unwrap())
    }
    for handle in handles {
        _ = handle.join();
    }
    drop(tx);

    // 汇总结果
    let mut total_res = BenchRes::new(params.client_num, params.duration as i32);
    for thread_res in rx.iter() {
        // println!("cnt from tx: {:?}", serde_json::to_string(&res).unwrap());
        total_res.total_success += thread_res.total_success;
        total_res.total_failure += thread_res.total_failure;
        total_res.performance.add(&thread_res.performance);
    }

    total_res.total_qps = total_res.total_success as f32 / params.duration as f32;
    total_res.performance.cal_qps(params.duration * 1000);
    Some(total_res)
}

// 压测getset操作
pub fn bench_getset(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();
        let mut random_value: Vec<u8> = vec![0; param.data_size as usize];

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str()).unwrap();

                let mut rng = rand::thread_rng();

                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);
                // 当前线程压测开始时间
                let start_time = time::Instant::now();

                // qps limiter
                let mut limiter: Option<ratelimit::Ratelimiter> = None;
                if param.qps_per_conn > 0 {
                    limiter = Some(ratelimit::Ratelimiter::new(
                        50,
                        1,
                        param.qps_per_conn as u64,
                    ));
                };
                loop {
                    match limiter {
                        Some(ref l) => {
                            l.wait();
                        }
                        None => {}
                    }
                    if time::Instant::now().duration_since(start_time).as_secs() > param.duration {
                        // 计算单个连接的QPS
                        thread_sender.send(res).unwrap();
                        break;
                    }

                    // 随机get一个znode
                    let idx: i32 = rng.gen_range(0..param.node_num);
                    let path = format!("{}/{:0>10}", BENCH_ROOT, idx);

                    let start = time::Instant::now();

                    if rng.gen_range(0.0..=1.0) < param.rw_ratio {
                        let get_result = zk_cli.get_data(&path, false);
                        match get_result {
                            Ok(_) => {
                                res.total_success += 1;
                                res.performance.insert(
                                    "get".to_string(),
                                    time::Instant::now().duration_since(start).as_millis() as u32,
                                );
                            }
                            Err(e) => {
                                println!(
                                    "failed to set znode {}, error: {}. Reconnecting...",
                                    path, e
                                );
                                res.total_failure += 1;
                                _ = zk_cli.close(); // 先close一下,不管result
                                zk_cli = connect_zk(param.address.as_str()).unwrap();
                                // 重连zk
                            }
                        }
                    } else {
                        // 随机改变value某一位的值
                        let random_index = rng.gen_range(0..param.data_size);
                        random_value[random_index as usize] = rng.gen_range(0..10) as u8;

                        let set_result = zk_cli.set_data(&path, random_value.clone(), Some(-1));
                        match set_result {
                            Ok(_) => {
                                res.total_success += 1;
                                res.performance.insert(
                                    "set".to_string(),
                                    time::Instant::now().duration_since(start).as_millis() as u32,
                                );
                            }
                            Err(e) => {
                                println!(
                                    "failed to set znode {}, error: {}. Reconnecting...",
                                    path, e
                                );
                                res.total_failure += 1;
                                _ = zk_cli.close(); // 先close一下,不管result
                                zk_cli = connect_zk(param.address.as_str()).unwrap();
                                // 重连zk
                            }
                        }
                    }
                }
                _ = zk_cli.close();
            });
        handles.push(handle.unwrap())
    }
    for handle in handles {
        _ = handle.join();
    }
    drop(tx);

    // 汇总结果
    let mut total_res = BenchRes::new(params.client_num, params.duration as i32);
    for thread_res in rx.iter() {
        // println!("cnt from tx: {:?}", serde_json::to_string(&res).unwrap());
        total_res.total_success += thread_res.total_success;
        total_res.total_failure += thread_res.total_failure;
        total_res.performance.add(&thread_res.performance);
    }

    total_res.total_qps = total_res.total_success as f32 / params.duration as f32;
    total_res.performance.cal_qps(params.duration * 1000);
    Some(total_res)
}

/// 连接zk.最多连续重试10次,连续10次都连接不上的话认为zk有问题
pub fn connect_zk(addr: &str) -> Result<ZooKeeper, ZkError> {
    let mut retry = 0;
    let mut rng = rand::thread_rng();
    loop {
        match ZooKeeper::connect(addr, time::Duration::from_secs(5), LoggingWatcher) {
            Ok(zk) => {
                return Ok(zk);
            }
            Err(e) => {
                println!("Error connecting to ZooKeeper: {}", e);
                retry += 1;
                if retry >= 10 {
                    return Err(e);
                }
                thread::sleep(time::Duration::from_millis(rng.gen_range(10..200)));
            }
        }
    }
}
