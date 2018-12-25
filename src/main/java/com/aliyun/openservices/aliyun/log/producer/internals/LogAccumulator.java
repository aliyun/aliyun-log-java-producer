package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.Callback;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.TimeoutException;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class LogAccumulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogAccumulator.class);

    private static final AtomicLong BATCH_ID = new AtomicLong(0);

    private final String producerHash;

    private final ProducerConfig producerConfig;

    private final Semaphore memoryController;

    private final RetryQueue retryQueue;

    private final BlockingQueue<ProducerBatch> successQueue;

    private final BlockingQueue<ProducerBatch> failureQueue;

    private final IOThreadPool ioThreadPool;

    private final AtomicInteger batchCount;

    private final ConcurrentMap<GroupKey, ProducerBatchHolder> batches;

    private final AtomicInteger appendsInProgress;

    private volatile boolean closed;

    public LogAccumulator(String producerHash,
                          ProducerConfig producerConfig,
                          Semaphore memoryController,
                          RetryQueue retryQueue,
                          BlockingQueue<ProducerBatch> successQueue,
                          BlockingQueue<ProducerBatch> failureQueue,
                          IOThreadPool ioThreadPool,
                          AtomicInteger batchCount) {
        this.producerHash = producerHash;
        this.producerConfig = producerConfig;
        this.memoryController = memoryController;
        this.retryQueue = retryQueue;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
        this.ioThreadPool = ioThreadPool;
        this.batchCount = batchCount;
        this.batches = new ConcurrentHashMap<GroupKey, ProducerBatchHolder>();
        this.appendsInProgress = new AtomicInteger(0);
        this.closed = false;
    }

    public ListenableFuture<Result> append(String project, String logStore, String topic, String source, String shardHash, LogItem logItem, Callback callback) throws InterruptedException {
        appendsInProgress.incrementAndGet();
        try {
            return doAppend(project, logStore, topic, source, shardHash, logItem, callback);
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    private ListenableFuture<Result> doAppend(String project, String logStore, String topic, String source, String shardHash, LogItem logItem, Callback callback) throws InterruptedException {
        if (closed)
            throw new IllegalStateException("cannot append after the log accumulator was closed");
        int sizeInBytes = LogSizeCalculator.calculate(logItem);
        ensureValidLogSize(sizeInBytes);
        boolean acquired = false;
        try {
            long maxBlockMs = producerConfig.getMaxBlockMs();
            LOGGER.trace("Prepare to acquire bytes, sizeInBytes={}, maxBlockMs={}, project={}, logStore={}", sizeInBytes, maxBlockMs, project, logStore);
            if (maxBlockMs >= 0) {
                acquired = memoryController.tryAcquire(sizeInBytes, maxBlockMs, TimeUnit.MILLISECONDS);
                if (!acquired)
                    throw new TimeoutException("failed to acquire memory within the configured max blocking time " + producerConfig.getMaxBlockMs() + " ms");
            } else {
                memoryController.acquire(sizeInBytes);
                acquired = true;
            }
            GroupKey groupKey = new GroupKey(project, logStore, topic, source, shardHash);
            ProducerBatchHolder holder = getOrCreateProducerBatchHolder(groupKey);
            synchronized (holder) {
                if (holder.producerBatch != null) {
                    ListenableFuture<Result> f = holder.producerBatch.tryAppend(logItem, sizeInBytes, callback);
                    if (f != null)
                        return f;
                    else
                        holder.transferProducerBatch(
                                ioThreadPool,
                                producerConfig,
                                retryQueue,
                                successQueue,
                                failureQueue,
                                batchCount);
                }
                holder.producerBatch = new ProducerBatch(
                        groupKey,
                        Utils.generatePackageId(producerHash, BATCH_ID),
                        producerConfig.getMaxBatchSizeInBytes(),
                        producerConfig.getMaxBatchCount(),
                        producerConfig.getMaxReservedAttempts(),
                        System.currentTimeMillis());
                ListenableFuture<Result> f = holder.producerBatch.tryAppend(logItem, sizeInBytes, callback);
                if (f == null)
                    throw new IllegalArgumentException("the log is " + sizeInBytes +
                            " bytes which is larger than the maxBatchSizeInBytes you specified");
                batchCount.incrementAndGet();
                if (holder.producerBatch.isFull()) {
                    holder.transferProducerBatch(
                            ioThreadPool,
                            producerConfig,
                            retryQueue,
                            successQueue,
                            failureQueue,
                            batchCount);
                }
                return f;
            }
        } finally {
            if (!acquired)
                memoryController.release(sizeInBytes);
        }
    }

    public ExpiredBatches expiredBatches() {
        long nowMs = System.currentTimeMillis();
        ExpiredBatches expiredBatches = new ExpiredBatches();
        long remainingMs = producerConfig.getLingerMs();
        for (Map.Entry<GroupKey, ProducerBatchHolder> entry : batches.entrySet()) {
            ProducerBatchHolder holder = entry.getValue();
            synchronized (holder) {
                if (holder.producerBatch == null)
                    continue;
                long curRemainingMs = holder.producerBatch.remainingMs(nowMs, producerConfig.getLingerMs());
                if (curRemainingMs <= 0)
                    holder.transferProducerBatch(expiredBatches);
                else {
                    remainingMs = Math.min(remainingMs, curRemainingMs);
                }
            }
        }
        expiredBatches.setRemainingMs(remainingMs);
        return expiredBatches;
    }

    public List<ProducerBatch> remainingBatches() {
        if (!closed)
            throw new IllegalStateException("cannot get the remaining batches before the log accumulator is closed");
        List<ProducerBatch> remainingBatches = new ArrayList<ProducerBatch>();
        while (appendsInProgress()) {
            drainTo(remainingBatches);
        }
        drainTo(remainingBatches);
        batches.clear();
        return remainingBatches;
    }

    private int drainTo(List<ProducerBatch> c) {
        int n = 0;
        for (Map.Entry<GroupKey, ProducerBatchHolder> entry : batches.entrySet()) {
            ProducerBatchHolder holder = entry.getValue();
            synchronized (holder) {
                if (holder.producerBatch == null)
                    continue;
                c.add(holder.producerBatch);
                ++n;
                holder.producerBatch = null;
            }
        }
        return n;
    }

    private void ensureValidLogSize(int sizeInBytes) {
        if (sizeInBytes > producerConfig.getMaxBatchSizeInBytes())
            throw new IllegalArgumentException("the log is " + sizeInBytes +
                    " bytes which is larger than the maxBatchSizeInBytes you specified");
        if (sizeInBytes > producerConfig.getTotalSizeInBytes())
            throw new IllegalArgumentException("the log is " + sizeInBytes +
                    " bytes which is larger than the totalSizeInBytes you specified");
    }

    private ProducerBatchHolder getOrCreateProducerBatchHolder(GroupKey groupKey) {
        ProducerBatchHolder holder = batches.get(groupKey);
        if (holder != null)
            return holder;
        holder = new ProducerBatchHolder();
        ProducerBatchHolder previous = batches.putIfAbsent(groupKey, holder);
        if (previous == null)
            return holder;
        else
            return previous;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }

    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    final private static class ProducerBatchHolder {

        ProducerBatch producerBatch;

        void transferProducerBatch(IOThreadPool ioThreadPool,
                                   ProducerConfig producerConfig,
                                   RetryQueue retryQueue,
                                   BlockingQueue<ProducerBatch> successQueue,
                                   BlockingQueue<ProducerBatch> failureQueue,
                                   AtomicInteger batchCount) {
            if (producerBatch == null)
                return;
            ioThreadPool.submit(
                    new SendProducerBatchTask(
                            producerBatch,
                            producerConfig,
                            retryQueue,
                            successQueue,
                            failureQueue,
                            batchCount));
            producerBatch = null;
        }

        void transferProducerBatch(ExpiredBatches expiredBatches) {
            if (producerBatch == null)
                return;
            expiredBatches.add(producerBatch);
            producerBatch = null;
        }

    }

}
