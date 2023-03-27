# 关于本项目

本项目是对ConfigCenter Zookeeper进行性能压测的一个小工具，具有较强的定制型，因此在当前实现阶段中不具有通用性。
实现之初，用golang实现了一个并发读写zookeeper实例的工具，对一个中规格的实例进行压测发现测试数据远低于预期（set/get
qps），猜测瓶颈可能在客户端。遂用rust实现了相同的压测逻辑，实测数据要好不少。

# 测试方法

## 参数设置

本工具支持设置如下参数。

- address：指定zk实例地址，默认值为127.0.0.1:2181
- client-num：压测时zk客户端数量，默认值为10
- data-size：set操作时设置znode value大小，单位：字节，默认值为1024字节
- node-num：压测空间的znode数量，默认值为10000个
- duration：对get/set操作进行压测的时长，单位：秒，默认值为100
- op：支持压测操作类型。支持的压测操作类型，见[支持的压测操作](##支持的压测操作)

```bash
(tob_env) root@n168-003-120:/home/yzk# ./zookeeper-benchmark --help
configcenter zookeeper instance benchmark tool

Usage: zookeeper-benchmark [OPTIONS]

Options:
      --address <ADDRESS>        [default: 127.0.0.1:2181]
      --client-num <CLIENT_NUM>  [default: 10]
      --data-size <DATA_SIZE>    [default: 1024]
      --node-num <NODE_NUM>      [default: 10000]
      --duration <DURATION>      [default: 100]
      --op <OP>                  [default: ]
  -h, --help                     Print help
  -V, --version                  Print version
```

```bash
./zookeeper-benchmark --client-num 100 --address cc-zk-7b63e0415413.config.inner.ivolces.com:2181 --op set --node-num 50000 --duration 60 --data-size 1024
```

## 支持的压测操作
- pre-create：预创建znode。在进行get/set压测前需要预创建指定数量的znode节点。
- post-clean：压测结束之后删除压测空间的znode及数据。
- get：测试get操作
- set：测试set操作

# 压测结果

```json
{
  "client_num": 100,
  "success": 800878,
  "failure": 0,
  "qps": 13347.967,
  "latency": {
    "short_quantile": {
      // p90 latency in milliseconds
      "0.90": 21,
      // p95 latency in milliseconds
      "0.95": 26,
      // p99 latency in milliseconds
      "0.99": 39,
      // p99.9 latency in milliseconds
      "0.999": 104,
      // max latency in milliseconds
      "1.00": 242
    },
    "total_cnt": 800878
  }
}
```