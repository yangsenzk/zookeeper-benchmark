# 关于本项目

本项目是对ConfigCenter Zookeeper进行性能压测的一个小工具，具有较强的定制型，因此在当前实现阶段中不具有通用性。
实现之初，用golang实现了一个并发读写zookeeper实例的工具，对一个中规格的实例进行压测发现测试数据远低于预期（set/get
qps），猜测瓶颈可能在客户端。遂用rust实现了相同的压测逻辑，实测数据要好不少。

更多使用方法，请参考：https://bytedance.feishu.cn/wiki/wikcn8nwozfic2rxsKY7UWbvifX