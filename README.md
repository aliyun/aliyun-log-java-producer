# Aliyun LOG Java Producer

[![Build Status](https://travis-ci.org/aliyun/aliyun-log-producer-java.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-log-producer-java)
[![License](https://img.shields.io/badge/license-Apache2.0-blue.svg)](/LICENSE)

[README in English](/README_EN.md)

Aliyun LOG Java Producer 是一个易于使用且高度可配置的 Java 类库，专门为运行在大数据、高并发场景下的 Java 应用量身打造。

## 功能特点
1. 线程安全 - producer 接口暴露的所有方法都是线程安全的。
2. 异步发送 - 调用 producer 的发送接口通常能够立即返回。Producer 内部会缓存并合并发送数据，然后批量发送以提高吞吐量。
3. 自动重试 - 对可重试的异常，producer 会根据用户配置的最大重试次数和重试退避时间进行重试。
4. 行为追溯 - 用户通过 callback 或 future 不仅能获取当前数据是否发送成功的信息，还可以获得该数据每次被尝试发送的信息，有利于问题追溯和行为决策。
5. 上下文还原 - 同一个 producer 实例产生的日志在同一上下文中，在服务端可以查看某条日志前后相关的日志。
6. 优雅关闭 - 保证 close 方法退时，producer 缓存的所有数据都能被处理，用户也能得到相应的通知。

## 功能优势

使用 producer 相对于直接通过 API 或 SDK 向 LogHub 写数据会有如下优势。

### 高性能
在海量数据、资源有限的前提下，写入端要达到目标吞吐量需要实现复杂的控制逻辑，包括多线程、缓存策略、批量发送等，另外还要充分考虑失败重试的场景。Producer 实现了上述功能，在为您带来性能优势的同时简化了程序开发步骤。

### 异步非阻塞
在可用内存充足的前提下，producer 会对发往 LogHub 的数据进行缓存，因此用户调用 send 方法时能够立即返回，不会阻塞，达到计算与 I/O 逻辑分离的目的。稍后，用户可以通过返回的 future 对象或传入的 callback 获得数据发送的结果。

### 资源可控制
可以通过参数控制 producer 用于缓存待发送数据的内存大小，同时还可以配置用于执行数据发送任务的线程数量。这样做一方面避免了 producer 无限制地消耗资源，另一方面可以让您根据实际情况平衡资源消耗和写入吞吐量。

## 安装

### Maven 使用者
将下列依赖加入到您项目的 pom.xml 中。
```
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>aliyun-log-producer</artifactId>
    <version>0.3.10</version>
</dependency>
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>aliyun-log</artifactId>
    <version>0.6.33</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>2.5.0</version>
</dependency>
```

jar-with-dependency 版本，可以解决producer依赖的版本冲突
```
<dependency>
    <groupId>com.aliyun.openservices</groupId>
    <artifactId>aliyun-log</artifactId>
    <version>0.6.35</version>
  <classifier>jar-with-dependencies</classifier>
</dependency>
```

### Gradle 使用者
```
compile 'com.aliyun.openservices:aliyun-log-producer:0.3.10'
compile 'com.aliyun.openservices:aliyun-log:0.6.33'
compile 'com.google.protobuf:protobuf-java:2.5.0'
```

## RAM 子账号访问
如果您使用子账号 AK，请确保该子账号拥有目标 project、logStore 的写权限，具体请参考 [RAM 子用户访问](https://help.aliyun.com/document_detail/29049.html)。

| Action | Resource |
|---|---|
| log:PostLogStoreLogs | acs:log:${regionName}:${projectOwnerAliUid}:project/${projectName}/logstore/${logstoreName} |

## 快速入门

参考教程 [Aliyun LOG Java Producer 快速入门](https://yq.aliyun.com/articles/682761)。

## 原理剖析

参考文章[日志上云利器 - Aliyun LOG Java Producer](https://yq.aliyun.com/articles/682762)。

## 应用示例

https://github.com/aliyun/aliyun-log-producer-sample

## 异常诊断

参考文档[异常诊断](/DIAGNOSIS_CN.md)。

## 常见问题

参考文档[常见问题](/FAQ_CN.md)。

## 关于性能

* [性能测试报告](/PERFORMANCE_CN.md)
* [性能诊断利器 JProfiler 快速入门和最佳实践](https://yq.aliyun.com/articles/684776)

## 关于升级

Aliyun LOG Java Producer 是对老版 log-loghub-producer 的全面升级，解决了上一版存在的多个问题，包括网络异常情况下 CPU 占用率过高、关闭 producer 可能出现少量数据丢失等问题。另外，在容错方面也进行了加强，即使您存在误用，在资源、吞吐、隔离等方面都有较好的保证。基于上述原因，强烈建议使用老版 producer 的用户进行升级。

## 问题反馈
如果您在使用过程中遇到了问题，可以创建 [GitHub Issue](https://github.com/aliyun/aliyun-log-producer/issues) 或者前往阿里云支持中心[提交工单](https://workorder.console.aliyun.com/#/ticket/createIndex)。
