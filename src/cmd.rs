use clap::Parser;

#[derive(Parser, serde::Serialize, Clone)]
#[command(name = "zookeeper-benchmark")]
#[command(author = "configcenter")]
#[command(version = "1.0")]
#[command(about = "configcenter zookeeper instance benchmark tool", long_about = None)]
pub struct Cli {
    #[arg(long, default_value = "127.0.0.1:2181")]
    pub address: String,
    #[arg(long, default_value_t = 10)]
    pub client_num: i32,
    #[arg(long, default_value_t = 1024)]
    pub data_size: i32,
    #[arg(long, default_value_t = 10000)]
    pub node_num: i32,
    #[arg(long, default_value_t = 100)]
    pub duration: u64,
    #[arg(long, default_value_t = 0.8)]
    pub rw_ratio: f32,
    #[arg(long, default_value = "")]
    pub op: String,
    #[arg(long, default_value_t = 0)]
    pub qps_per_conn: i32,
}
