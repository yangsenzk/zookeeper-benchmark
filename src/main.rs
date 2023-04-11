use clap::Parser;
use serde_json;

mod bench;
mod cmd;
mod latency;
mod model;
pub mod consts;

fn main() {
    // 解析命令行参数
    let params = cmd::Cli::parse();
    println!(
        "benchmark parameters: {}",
        serde_json::to_string(&params.clone()).unwrap()
    );

    match params.op.as_str() {
        "pre-create" => {
            bench::pre_create(&params);
        }
        "post-clean" => {
            bench::post_clean(&params);
        }
        "set" => {
            let bench_res = bench::bench_set(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "get" => {
            let bench_res = bench::bench_get(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "getset" => {
            let bench_res = bench::bench_getset(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        "create" => {
            let bench_res = bench::bench_create(&params);
            println!("{}", serde_json::to_string(&bench_res).unwrap());
        }
        _ => {
            panic!("operation {} is not supported yet!", params.op)
        }
    };
}
