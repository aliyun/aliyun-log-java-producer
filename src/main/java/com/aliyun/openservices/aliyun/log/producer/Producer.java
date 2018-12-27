package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The interface for the {@link LogProducer}
 * @see LogProducer
 */
public interface Producer {

    /**
     * See {@link LogProducer#send(String, String, LogItem)}
     */
    ListenableFuture<Result> send(String project, String logStore, LogItem logItem) throws InterruptedException;

    /**
     * See {@link LogProducer#send(String, String, String, String, LogItem)}
     */
    ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem) throws InterruptedException;

    /**
     * See {@link LogProducer#send(String, String, String, String, String, LogItem)}
     */
    ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem) throws InterruptedException;

    /**
     * See {@link LogProducer#send(String, String, LogItem, Callback)}
     */
    ListenableFuture<Result> send(String project, String logStore, LogItem logItem, Callback callback) throws InterruptedException;

    /**
     * See {@link LogProducer#send(String, String, String, String, LogItem, Callback)}
     */
    ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem, Callback callback) throws InterruptedException;

    /**
     * See {@link LogProducer#send(String, String, String, String, String, LogItem, Callback)}
     */
    ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem, Callback callback) throws InterruptedException;

    /**
     * See {@link LogProducer#close()}
     */
    void close() throws InterruptedException;

    /**
     * See {@link LogProducer#close(long)}
     */
    void close(long timeoutMs) throws InterruptedException;

    /**
     * See {@link LogProducer#getProducerConfig()}
     */
    ProducerConfig getProducerConfig();

    /**
     * See {@link LogProducer#getBatchCount()}
     */
    int getBatchCount();

    /**
     * See {@link LogProducer#availableMemoryInBytes()}
     */
    int availableMemoryInBytes();

}
