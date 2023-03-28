use std::sync::mpsc;
use std::thread;
use std::time;

use clap::Parser;
use rand;
use rand::Rng;
use serde;
use serde_json;
use zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZkError, ZooKeeper, ZooKeeperExt};

use zookeeper_benchmark::latency::RequestLatency;

struct LoggingWatcher;

impl Watcher for LoggingWatcher {
    fn handle(&self, _e: WatchedEvent) {}
}

const BENCH_ROOT: &str = "/bench_root";

#[derive(Parser, serde::Serialize, Clone)]
#[command(name = "zookeeper-benchmark")]
#[command(author = "configcenter")]
#[command(version = "1.0")]
#[command(about = "configcenter zookeeper instance benchmark tool", long_about = None)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:2181")]
    address: String,
    #[arg(long, default_value_t = 10)]
    client_num: i32,
    #[arg(long, default_value_t = 1024)]
    data_size: i32,
    #[arg(long, default_value_t = 10000)]
    node_num: i32,
    #[arg(long, default_value_t = 100)]
    duration: u64,
    #[arg(long, default_value_t = 0.8)]
    rw_ratio: f32,
    #[arg(long, default_value = "")]
    op: String,
}


#[derive(Debug, Clone, serde::Serialize)]
struct BenchRes {
    // 客户端数量
    client_num: i32,

    // 成功的请求总数
    total_success: i32,

    // 失败的请求总数
    total_failure: i32,

    // 延迟/qps等指标
    performance: RequestLatency,

    // 压测时长
    duration: f32,

    // 混合读写等场景下的整体qps
    total_qps: f32,
}

impl BenchRes {
    fn new(client_num: i32, duration: i32) -> BenchRes {
        BenchRes {
            client_num,
            duration: duration as f32,
            total_success: 0,
            total_failure: 0,
            total_qps: 0.0,
            performance: RequestLatency::new(),
        }
    }
}

// 预先创建znode
fn pre_create(params: &Cli) {
    let zk = ZooKeeper::connect(params.address.as_str(), time::Duration::from_secs(15), LoggingWatcher).unwrap();

    // 如果是多客户端节点并行压测时,则只需要一个客户端进行预先创建节点
    // 先删除测试目录
    _ = zk.delete_recursive(BENCH_ROOT);

    // 创建测试根node
    let result = zk.create(BENCH_ROOT, vec![0; 10], Acl::open_unsafe().clone(), CreateMode::Persistent);
    match result {
        Ok(_) => {}
        Err(e) => {
            panic!("create bench root node failed: {}", e)
        }
    }

    // 创建子节点
    for i in 0..params.node_num {
        let path = format!("{}/{:0>10}", BENCH_ROOT, i);
        let _result = zk.create(
            path.as_str(),
            vec![1; params.data_size as usize],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        ).unwrap();
    }
    _ = zk.close();
}

// 清理压测数据
fn post_clean(params: &Cli) {
    let zk = ZooKeeper::connect(params.address.as_str(), time::Duration::from_secs(15), LoggingWatcher).unwrap();

    // 递归删除节点
    _ = zk.delete_recursive(BENCH_ROOT);
}

// 压测create操作
fn bench_create(params: &Cli) -> Option<BenchRes> {
    let zk = connect_zk(params.address.as_str());
    // 先删除所有压测空间的znode
    _ = zk.delete_recursive(BENCH_ROOT);
    zk.create(BENCH_ROOT, vec![0; 10], Acl::open_unsafe().clone(), CreateMode::Persistent).unwrap();
    let _ = zk.close();

    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];

    // 每个客户端至少创建的znode数量
    let (minimal_node_num, remainder) = (params.node_num / params.client_num, params.node_num % params.client_num);

    // 当前线程压测开始时间
    let start_time = time::Instant::now();
    println!("starting {} bench...", params.op);
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();
        let random_value: Vec<u8> = vec![0; param.data_size as usize];
        let mut per_client_znode_num = minimal_node_num;
        if i < remainder {
            per_client_znode_num = minimal_node_num + 1;
        }
        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str());

                let cur_client_znode_root = format!("{}/{:0>4}", BENCH_ROOT, i);
                zk_cli.create(&cur_client_znode_root, vec![0, 1], Acl::open_unsafe().clone(), CreateMode::Persistent).unwrap();

                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);

                let mut znode_cnt = 0;
                while znode_cnt < per_client_znode_num {
                    let path = format!("{}/{:0>10}", cur_client_znode_root, znode_cnt);

                    // 每条create请求的延迟
                    let start = time::Instant::now();
                    let result = zk_cli.create(&path, random_value.clone(), Acl::open_unsafe().clone(), CreateMode::Persistent);
                    match result {
                        Ok(_) => {
                            res.total_success += 1;
                            res.performance.insert(param.op.to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                            znode_cnt += 1;
                        }
                        Err(e) => {
                            match e {
                                ZkError::NodeExists => {
                                    res.total_success += 1;
                                    res.performance.insert(param.op.to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                                    znode_cnt += 1;
                                }
                                _ => {
                                    println!("failed to create znode {}, error: {}. Reconnecting...", path, e);
                                    res.total_failure += 1;
                                    _ = zk_cli.close(); // 先close一下,不管result
                                    zk_cli = connect_zk(param.address.as_str());
                                }
                            }
                        }
                    }
                }
                thread_sender.send(res).unwrap();
                _ = zk_cli.close();
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
    // 所有请求处理完成的实际时长
    let actual_duration = time::Instant::now().duration_since(start_time);
    total_res.duration = actual_duration.as_secs_f32();
    total_res.total_qps = total_res.total_success as f32 / (actual_duration.as_millis() as f32 / 1000.0);
    total_res.performance.cal_qps(actual_duration.as_millis() as u64);
    Some(total_res)
}


// 压测set操作
fn bench_set(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();
        let mut random_value: Vec<u8> = vec![0; param.data_size as usize];

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk = connect_zk(&param.address.as_str());

                let mut rng = rand::thread_rng();
                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);
                // 当前线程压测开始时间
                let start_time = time::Instant::now();
                loop {
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
                            res.performance.insert(param.op.to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                        }
                        Err(e) => {
                            println!("failed to set znode {}, error: {}. Reconnecting...", path, e);
                            res.total_failure += 1;
                            _ = zk.close(); // 先close一下,不管result
                            match e {
                                ZkError::ConnectionLoss => {
                                    zk = connect_zk(param.address.as_str()); // 重连zk
                                }
                                _ => {
                                    // 暂时还是重连zk
                                    zk = connect_zk(param.address.as_str());
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
fn bench_get(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str());

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
                loop {
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
                            res.performance.insert(param.op.to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                        }
                        Err(e) => {
                            println!("failed to set znode {}, error: {}. Reconnecting...", path, e);
                            res.total_failure += 1;
                            _ = zk_cli.close(); // 先close一下,不管result
                            match e {
                                ZkError::ConnectionLoss => {
                                    zk_cli = connect_zk(param.address.as_str()); // 重连zk
                                }
                                _ => {
                                    // 暂时还是重连zk
                                    zk_cli = connect_zk(param.address.as_str());
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
fn bench_getset(params: &Cli) -> Option<BenchRes> {
    let (tx, rx): (mpsc::Sender<BenchRes>, mpsc::Receiver<BenchRes>) = mpsc::channel();

    let mut handles = vec![];
    for i in 0..params.client_num {
        let thread_sender = tx.clone();
        let param = params.clone();
        let mut random_value: Vec<u8> = vec![0; param.data_size as usize];

        let handle = thread::Builder::new()
            .name(format!("thread-{:0>4}", i))
            .spawn(move || {
                let mut zk_cli = connect_zk(&param.address.as_str());

                let mut rng = rand::thread_rng();
                // 当前线程的压测结果
                let mut res = BenchRes::new(param.client_num, param.duration as i32);
                // 当前线程压测开始时间
                let start_time = time::Instant::now();
                loop {
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
                                res.performance.insert("get".to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                            }
                            Err(e) => {
                                println!("failed to set znode {}, error: {}. Reconnecting...", path, e);
                                res.total_failure += 1;
                                _ = zk_cli.close(); // 先close一下,不管result
                                zk_cli = connect_zk(param.address.as_str()); // 重连zk
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
                                res.performance.insert("set".to_string(), time::Instant::now().duration_since(start).as_millis() as u32);
                            }
                            Err(e) => {
                                println!("failed to set znode {}, error: {}. Reconnecting...", path, e);
                                res.total_failure += 1;
                                _ = zk_cli.close(); // 先close一下,不管result
                                zk_cli = connect_zk(param.address.as_str()); // 重连zk
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

// 重新连接zk
fn connect_zk(addr: &str) -> ZooKeeper {
    loop {
        match ZooKeeper::connect(addr, time::Duration::from_secs(10), LoggingWatcher) {
            Ok(zk) => {
                return zk;
            }
            Err(e) => {
                println!("Error connecting to ZooKeeper: {}", e);
                thread::sleep(time::Duration::from_millis(5));
            }
        }
    }
}

fn main() {
    // 解析命令行参数
    let params = Cli::parse();
    println!("benchmark parameters: {}", serde_json::to_string(&params.clone()).unwrap());

    match params.op.as_str() {
        "pre-create" => {
            pre_create(&params);
        }
        "post-clean" => {
            post_clean(&params);
        }
        "set" => {
            let bench_res = bench_set(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "get" => {
            let bench_res = bench_get(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "getset" => {
            let bench_res = bench_getset(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "create" => {
            let bench_res = bench_create(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        _ => {
            panic!("operation {} is not supported yet!", params.op)
        }
    };
}