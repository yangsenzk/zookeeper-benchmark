use std::collections::HashMap;

use serde::Serialize;

#[derive(Serialize, Clone, Debug)]
pub struct RequestLatency {
    op_latency: HashMap<String, OpResult>,
}

#[derive(Debug, Clone, Serialize)]
struct OpResult {
    request_cnt: u32,
    #[serde(skip_serializing)]
    bucket: HashMap<u32, u32>,
    #[serde(skip_serializing)]
    detail_quantile: Vec<(u32, f32)>,
    short_quantile: HashMap<String, u32>,
    qps: f32,
}

impl OpResult {
    pub fn new() -> OpResult {
        OpResult {
            request_cnt: 0,
            bucket: HashMap::new(),
            detail_quantile: vec![],
            short_quantile: HashMap::new(),
            qps: 0.0,
        }
    }
}

impl RequestLatency {
    pub fn new() -> RequestLatency {
        RequestLatency {
            op_latency: HashMap::new(),
        }
    }

    pub fn insert(&mut self, op: String, val: u32) {
        if !self.op_latency.contains_key(op.as_str()) {
            self.op_latency.insert(op.to_string(), OpResult::new());
        }

        match self.op_latency.get_mut(op.as_str()) {
            Some(op_result) => {
                match op_result.bucket.get(&val) {
                    Some(cur_cnt) => {
                        op_result.bucket.insert(val, cur_cnt + 1);
                    }
                    None => {
                        op_result.bucket.insert(val, 1);
                    }
                }
                op_result.request_cnt += 1;
            }
            None => {
                self.op_latency.insert(op.to_string(), OpResult::new());
            }
        }
    }

    fn update_quantile(&mut self) {
        for (_op, result) in self.op_latency.iter_mut() {
            let mut keys: Vec<&u32> = result.bucket.keys().collect();
            keys.sort();
            let mut cumulative_sum = 0;
            result.detail_quantile = Vec::new();
            for key in keys {
                let cnt = result.bucket.get(key).unwrap_or(&0u32);
                cumulative_sum += cnt;

                let percent = cumulative_sum as f32 / result.request_cnt as f32;
                result.detail_quantile.push((*key, percent));

                let k;
                if percent <= 0.90 {
                    k = String::from("0.90");
                } else if percent <= 0.95 {
                    k = String::from("0.95");
                } else if percent <= 0.99 {
                    k = String::from("0.99");
                } else if percent <= 0.999 {
                    k = String::from("0.999");
                } else {
                    k = String::from("1.00");
                }

                let p = result.short_quantile.get(k.as_str()).unwrap_or(&0u32);
                if *p < *key {
                    result.short_quantile.insert(k, *key);
                }
            }
        }
    }

    pub fn add(&mut self, another: &RequestLatency) {
        for (op, another_result) in another.op_latency.iter() {
            if !self.op_latency.contains_key(op) {
                self.op_latency
                    .insert(op.to_string(), another_result.clone());
                continue;
            }
            let this_result = self.op_latency.get_mut(op.as_str()).unwrap();
            // 更新bucket和cnt
            for (t, another_cnt) in another_result.bucket.iter() {
                let this_cnt = this_result.bucket.get(t).unwrap_or(&0u32);
                this_result.bucket.insert(*t, *this_cnt + *another_cnt);
                this_result.request_cnt += another_cnt;
            }
        }

        self.update_quantile();
    }

    pub fn cal_qps(&mut self, duration_millis: u64) {
        for (_op, res) in self.op_latency.iter_mut() {
            res.qps = res.request_cnt as f32 / (duration_millis as f32 / 1000.0);
        }
    }
}
