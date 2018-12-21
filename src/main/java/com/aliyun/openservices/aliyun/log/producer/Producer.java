package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;

public interface Producer {

    ListenableFuture<Result> send(String project, String logStore, LogItem logItem) throws InterruptedException;

    ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem) throws InterruptedException;

    ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem) throws InterruptedException;

    ListenableFuture<Result> send(String project, String logStore, LogItem logItem, Callback callback) throws InterruptedException;

    ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem, Callback callback) throws InterruptedException;

    ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem, Callback callback) throws InterruptedException;

    void close() throws InterruptedException;

    void close(long timeoutMs) throws InterruptedException;

    ProducerConfig getProducerConfig();

    int getBatchCount();

    int availableMemoryInBytes();

}
