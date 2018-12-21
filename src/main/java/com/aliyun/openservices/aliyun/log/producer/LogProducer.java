package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.internals.*;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LogProducer implements Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogProducer.class);

    private static final AtomicInteger INSTANCE_ID_GENERATOR = new AtomicInteger(0);

    private static final String LOG_PRODUCER_PREFIX = "aliyun-log-producer-";

    private static final String MOVER_SUFFIX = "-mover";

    private static final String SUCCESS_BATCH_HANDLER_SUFFIX = "-success-batch-handler";

    private static final String FAILURE_BATCH_HANDLER_SUFFIX = "-failure-batch-handler";

    private final int instanceId;

    private final String name;

    private final String producerHash;

    private final ProducerConfig producerConfig;

    private final Semaphore memoryController;

    private final RetryQueue retryQueue;

    private final IOThreadPool ioThreadPool;

    private final LogAccumulator accumulator;

    private final Mover mover;

    private final BatchHandler successBatchHandler;

    private final BatchHandler failureBatchHandler;

    private final AtomicInteger batchCount = new AtomicInteger(0);

    public LogProducer(ProducerConfig producerConfig) {
        this.instanceId = INSTANCE_ID_GENERATOR.getAndIncrement();
        this.name = LOG_PRODUCER_PREFIX + this.instanceId;
        this.producerHash = Utils.generateProducerHash(this.instanceId);
        this.producerConfig = producerConfig;
        this.memoryController = new Semaphore(producerConfig.getTotalSizeInBytes());
        this.retryQueue = new RetryQueue(producerConfig.getBaseRetryBackoffMs());
        BlockingQueue<ProducerBatch> successQueue = new LinkedBlockingQueue<ProducerBatch>();
        BlockingQueue<ProducerBatch> failureQueue = new LinkedBlockingQueue<ProducerBatch>();
        this.ioThreadPool = new IOThreadPool(producerConfig.getIoThreadCount(), this.name);
        this.accumulator = new LogAccumulator(
                this.producerHash,
                producerConfig,
                this.memoryController,
                this.retryQueue,
                successQueue,
                failureQueue,
                this.ioThreadPool,
                this.batchCount);
        this.mover = new Mover(
                this.name + MOVER_SUFFIX,
                producerConfig,
                this.accumulator,
                this.retryQueue,
                successQueue,
                failureQueue,
                this.ioThreadPool,
                this.batchCount);
        this.successBatchHandler = new BatchHandler(
                this.name + SUCCESS_BATCH_HANDLER_SUFFIX,
                successQueue,
                this.batchCount,
                this.memoryController);
        this.failureBatchHandler = new BatchHandler(
                this.name + FAILURE_BATCH_HANDLER_SUFFIX,
                failureQueue,
                this.batchCount,
                this.memoryController);
        this.mover.start();
        this.successBatchHandler.start();
        this.failureBatchHandler.start();
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, LogItem logItem) throws InterruptedException {
        return send(project, logStore, "", "", logItem);
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem) throws InterruptedException {
        return send(project, logStore, topic, source, "", logItem);
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem) throws InterruptedException {
        return send(project, logStore, topic, source, shardHash, logItem, null);
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, LogItem logItem, Callback callback) throws InterruptedException {
        return send(project, logStore, "", "", logItem, callback);
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, String topic, String source, LogItem logItem, Callback callback) throws InterruptedException {
        return send(project, logStore, topic, source, "", logItem, callback);
    }

    @Override
    public ListenableFuture<Result> send(String project, String logStore, String topic, String source, String shardHash, LogItem logItem, Callback callback) throws InterruptedException {
        Utils.assertArgumentNotNullOrEmpty(project, "project");
        Utils.assertArgumentNotNullOrEmpty(logStore, "logStore");
        if (topic == null)
            topic = "";
        Utils.assertArgumentNotNull(logItem, "logItem");
        return accumulator.append(project, logStore, topic, source, shardHash, logItem, callback);
    }

    @Override
    public void close() throws InterruptedException {
        close(Long.MAX_VALUE);
    }

    @Override
    public void close(long timeoutMs) throws InterruptedException {
        if (timeoutMs < 0)
            throw new IllegalArgumentException("timeoutMs must be greater than or equal to 0, got " + timeoutMs);
        IllegalStateException firstException = null;
        LOGGER.info("Closing the log producer, timeoutMs={}", timeoutMs);
        try {
            timeoutMs = closeMover(timeoutMs);
        } catch (IllegalStateException e) {
            firstException = e;
        }
        LOGGER.debug("After close mover, timeoutMs={}", timeoutMs);
        try {
            timeoutMs = closeIOThreadPool(timeoutMs);
        } catch (IllegalStateException e) {
            if (firstException == null)
                firstException = e;
        }
        LOGGER.debug("After close ioThreadPool, timeoutMs={}", timeoutMs);
        try {
            timeoutMs = closeSuccessBatchHandler(timeoutMs);
        } catch (IllegalStateException e) {
            if (firstException == null)
                firstException = e;
        }
        LOGGER.debug("After close success batch handler, timeoutMs={}", timeoutMs);
        try {
            timeoutMs = closeFailureBatchHandler(timeoutMs);
        } catch (IllegalStateException e) {
            if (firstException == null)
                firstException = e;
        }
        LOGGER.debug("After close failure batch handler, timeoutMs={}", timeoutMs);
        if (firstException != null)
            throw firstException;
        LOGGER.info("The log producer has been closed");
    }

    private long closeMover(long timeoutMs) throws InterruptedException {
        long startMs = System.currentTimeMillis();
        accumulator.close();
        retryQueue.close();
        mover.close();
        mover.join(timeoutMs);
        if (mover.isAlive()) {
            LOGGER.warn("The mover thread is still alive");
            throw new IllegalStateException("the mover thread is still alive");
        }
        long nowMs = System.currentTimeMillis();
        return Math.max(0, timeoutMs - nowMs + startMs);
    }

    private long closeIOThreadPool(long timeoutMs) throws InterruptedException {
        long startMs = System.currentTimeMillis();
        ioThreadPool.shutdown();
        if (ioThreadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS))
            LOGGER.debug("The ioThreadPool is terminated");
        else {
            LOGGER.warn("The ioThreadPool is not fully terminated");
            throw new IllegalStateException("the ioThreadPool is not fully terminated");
        }
        long nowMs = System.currentTimeMillis();
        return Math.max(0, timeoutMs - nowMs + startMs);
    }

    private long closeSuccessBatchHandler(long timeoutMs) throws InterruptedException {
        long startMs = System.currentTimeMillis();
        successBatchHandler.close();
        boolean invokedFromCallback = Thread.currentThread() == this.successBatchHandler;
        if (invokedFromCallback) {
            LOGGER.warn("Skip join success batch handler since you have incorrectly invoked close from the producer call-back");
            return timeoutMs;
        }
        successBatchHandler.join(timeoutMs);
        if (successBatchHandler.isAlive()) {
            LOGGER.warn("The success batch handler thread is still alive");
            throw new IllegalStateException("the success batch handler thread is still alive");
        }
        long nowMs = System.currentTimeMillis();
        return Math.max(0, timeoutMs - nowMs + startMs);
    }

    private long closeFailureBatchHandler(long timeoutMs) throws InterruptedException {
        long startMs = System.currentTimeMillis();
        failureBatchHandler.close();
        boolean invokedFromCallback = Thread.currentThread() == this.successBatchHandler || Thread.currentThread() == this.failureBatchHandler;
        if (invokedFromCallback) {
            LOGGER.warn("Skip join failure batch handler since you have incorrectly invoked close from the producer call-back");
            return timeoutMs;
        }
        failureBatchHandler.join(timeoutMs);
        if (failureBatchHandler.isAlive()) {
            LOGGER.warn("The failure batch handler thread is still alive");
            throw new IllegalStateException("the failure batch handler thread is still alive");
        }
        long nowMs = System.currentTimeMillis();
        return Math.max(0, timeoutMs - nowMs + startMs);
    }

    @Override
    public ProducerConfig getProducerConfig() {
        return producerConfig;
    }

    @Override
    public int getBatchCount() {
        return batchCount.get();
    }

    @Override
    public int availableMemoryInBytes() {
        return memoryController.availablePermits();
    }

}
