# 性能测试

## 测试环境
### ECS 虚拟机
* 规格族：计算型 c5 
* 实例规格：ecs.c5.4xlarge
* CPU：16 vCPU，Intel Xeon(Skylake) Platinum 8163，2.5 GHz
* MEM：32GB
* 内网带宽：5 Gbps
* OS：Linux version 3.10.0-957.1.3.el7.x86_64

### JVM
OpenJDK 64-Bit Server VM (build 25.191-b12, mixed mode)

### 日志样例
测试中使用的日志包含 8 个键值对以及 topic、source 两个字段。为了模拟数据的随机性，我们给每个字段值追加了一个随机后缀。其中，topic 后缀取值范围是 \[0, 5)，source 后缀取值范围是 \[0, 10)，其余 8 个键值对后缀取值范围是 \[0, 单线程发送次数)。单条日志大小约为 550 字节，格式如下：
``` 
__topic__:  topic-<suffix>  
__source__:  source-<suffix>
content_key_1:  1abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>
content_key_2:  2abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>
content_key_3:  3abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>
content_key_4:  4abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>
content_key_5:  5abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>  
content_key_6:  6abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>  
content_key_7:  7abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>  
content_key_8:  8abcdefghijklmnopqrstuvwxyz!@#$%^&*()_0123456789-<suffix>  
```

### Project & Logstore
* Project：在 ECS 所在 region 创建目标 project 并通过 VPC 网络服务入口进行访问。
* Logstore：在该 project 下创建一个分区数为 10 的 logstore（未开启索引），该 logstore 的写入流量最大为 50 MB/s，参阅[数据读写](https://help.aliyun.com/document_detail/92571.html)。

## 测试用例

### 测试程序说明
* ProducerConfig.totalSizeInBytes: 具体用例中调整
* ProducerConfig.batchSizeThresholdInBytes: 3 \* 1024 \* 1024
* ProducerConfig.batchCountThreshold：40960
* ProducerConfig.lingerMs：2000
* ProducerConfig.ioThreadCount: 具体用例中调整
* JVM 初始堆大小：2 GB
* JVM 最大堆大小：2 GB
* 调用`Producer.send()`方法的线程数量：10
* 每个线程发送日志条数：20,000,000
* 发送日志总大小：约 115 GB
* 客户端压缩后大小：约 12 GB
* 发送日志总条数：200,000,000

测试代码：[SamplePerformance.java](https://github.com/aliyun/aliyun-log-producer-sample/blob/master/src/main/java/com/aliyun/openservices/aliyun/log/producer/sample/SamplePerformance.java)

运行步骤：[SamplePerformance 运行步骤
](https://github.com/aliyun/aliyun-log-producer-sample/blob/master/PERF_README_CN.md)

### 调整 IO 线程数量
将 ProducerConfig.totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），通过调整 ProducerConfig.ioThreadCount 观察程序性能。

| IO 线程数量 | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 | 说明 |
| -------- | -------- | -------- | -------- | -------- |
| 1 | 34.296 MB/s | 3.658 MB/s | 60% | 未达 10 个 shard 写入能力里上限。 |
| 2 | 74.131 MB/s | 7.907MB/s | 120% | 未达 10 个 shard 写入能力里上限。 |
| 4 | 142.142 MB/s | 15.160 MB/s | 235% | 未达 10 个 shard 写入能力里上限。 |
| 8 | 279.335 MB/s | 29.792 MB/s | 480% | 未达 10 个 shard 写入能力里上限。 |
| 16 | 450.440 MB/s | 48.040 MB/s | 900% | 未达 10 个 shard 写入能力里上限。 |
| 32 | 508.207 MB/s | 54.201 MB/s | 1350% | 达到 10 个 shard 写入能力里上限，服务端偶尔返回 Write quota exceed。 |

**说明：** CPU 时间主要花费在对象的序列化和压缩上，在吞吐量较高的情况下 CPU 使用率比较高。但在日常环境中，单机数据流量均值为 100KB/S，因此造成的 CPU 消耗几乎可以忽略不计。

### 调整 totalSizeInBytes
将 ProducerConfig.ioThreadCount 设置为8，通过调整 ProducerConfig.totalSizeInBytes 观察程序性能。

| TotalSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 | 说明 |
| -------- | -------- | -------- | -------- | -------- |
| 52,428,800 | 268.159 MB/s | 28.599 MB/s | 400% | 未达 10 个 shard 写入能力里上限。 |
| 209,715,200 | 218.876 MB/s | 23.344 MB/s | 630% | 未达 10 个 shard 写入能力里上限。 |
| 419,430,400 | 200.331 MB/s | 21.366 MB/s | 700% | 未达 10 个 shard 写入能力里上限。 |

## 总结
1. 增加 IO 线程数量可以显著提高吞吐量，尤其是当 IO 线程数量少于可用处理器个数时。
2. 调整 totalSizeInBytes 对吞吐量影响不够显著，增加 totalSizeInBytes 会造成更多的 CPU 消耗，建议使用默认值。

