use crate::latency::RequestLatency;

#[derive(Debug, Clone, serde::Serialize)]
pub struct BenchRes {
    // 客户端数量
    pub client_num: i32,

    // 成功的请求总数
    pub total_success: i32,

    // 失败的请求总数
    pub total_failure: i32,

    // 延迟/qps等指标
    pub performance: RequestLatency,

    // 压测时长
    pub duration: f32,

    // 混合读写等场景下的整体qps
    pub total_qps: f32,
}

impl BenchRes {
    pub fn new(client_num: i32, duration: i32) -> BenchRes {
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
