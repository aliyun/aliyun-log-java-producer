package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Mover extends LogThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerBatch.class);

    private final ProducerConfig producerConfig;

    private final LogAccumulator accumulator;

    private final RetryQueue retryQueue;

    private final BlockingQueue<ProducerBatch> successQueue;

    private final BlockingQueue<ProducerBatch> failureQueue;

    private final IOThreadPool ioThreadPool;

    private final AtomicInteger batchCount;

    private volatile boolean closed;

    public Mover(String name,
                 ProducerConfig producerConfig,
                 LogAccumulator accumulator,
                 RetryQueue retryQueue,
                 BlockingQueue<ProducerBatch> successQueue,
                 BlockingQueue<ProducerBatch> failureQueue,
                 IOThreadPool ioThreadPool,
                 AtomicInteger batchCount) {
        super(name, true);
        this.producerConfig = producerConfig;
        this.accumulator = accumulator;
        this.retryQueue = retryQueue;
        this.successQueue = successQueue;
        this.failureQueue = failureQueue;
        this.ioThreadPool = ioThreadPool;
        this.batchCount = batchCount;
        this.closed = false;
    }

    @Override
    public void run() {
        while (!closed) {
            try {
                runOnce();
            } catch (Exception e) {
                LOGGER.error("Uncaught exception in mover, e=", e);
            }
        }
        LOGGER.debug("Beginning shutdown of mover thread");
        List<ProducerBatch> incompleteBatches = incompleteBatches();
        LOGGER.debug("Submit incomplete batches, size={}", incompleteBatches.size());
        submitIncompleteBatches(incompleteBatches);
        LOGGER.debug("Shutdown of mover thread has completed");
    }

    private void runOnce() {
        long sleepTimeMs = moveBatches(System.currentTimeMillis());
        doSleep(sleepTimeMs);
    }

    private long moveBatches(long nowMs) {
        LOGGER.debug("Prepare to move expired batches from accumulator and retry queue to ioThreadPool");
        long remainingMs = doMoveBatches(nowMs);
        LOGGER.debug("Move expired batches successfully");
        return remainingMs;
    }

    private long doMoveBatches(long nowMs) {
        ExpiredBatches expiredBatches = accumulator.expiredBatches(nowMs);
        LOGGER.debug("Expired batches from accumulator, size={}", expiredBatches.getBatches().size());
        ExpiredBatches expiredRetryBatches = retryQueue.expiredBatches(nowMs);
        LOGGER.debug("Expired batches from retryQueue, size={}", expiredRetryBatches.getBatches().size());
        for (ProducerBatch b : expiredBatches.getBatches()) {
            ioThreadPool.submit(createSendProducerBatchTask(b));
        }
        for (ProducerBatch b : expiredRetryBatches.getBatches()) {
            ioThreadPool.submit(createSendProducerBatchTask(b));
        }
        return Math.min(expiredBatches.getRemainingMs(), expiredRetryBatches.getRemainingMs());
    }

    private List<ProducerBatch> incompleteBatches() {
        List<ProducerBatch> incompleteBatches = accumulator.remainingBatches();
        incompleteBatches.addAll(retryQueue.remainingBatches());
        return incompleteBatches;
    }

    private void submitIncompleteBatches(List<ProducerBatch> incompleteBatches) {
        for (ProducerBatch b : incompleteBatches)
            ioThreadPool.submit(createSendProducerBatchTask(b));
    }

    private SendProducerBatchTask createSendProducerBatchTask(ProducerBatch batch) {
        return new SendProducerBatchTask(
                batch,
                producerConfig,
                retryQueue,
                successQueue,
                failureQueue,
                batchCount);
    }

    private void doSleep(long sleepTimeMs) {
        LOGGER.debug("Prepare to sleep, sleepTimeMs={}", sleepTimeMs);
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            LOGGER.info("The mover has been interrupted from sleeping");
        }
    }

    public void close() {
        this.closed = true;
        interrupt();
    }

}
