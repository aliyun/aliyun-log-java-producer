package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.*;
import com.aliyun.openservices.log.Client;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mover extends LogThread {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerBatch.class);

  private final ProducerConfig producerConfig;

  private final Map<String, Client> clientPool;

  private final LogAccumulator accumulator;

  private final RetryQueue retryQueue;

  private final BlockingQueue<ProducerBatch> successQueue;

  private final BlockingQueue<ProducerBatch> failureQueue;

  private final IOThreadPool ioThreadPool;

  private final AtomicInteger batchCount;

  private volatile boolean closed;

  public Mover(
      String name,
      ProducerConfig producerConfig,
      Map<String, Client> clientPool,
      LogAccumulator accumulator,
      RetryQueue retryQueue,
      BlockingQueue<ProducerBatch> successQueue,
      BlockingQueue<ProducerBatch> failureQueue,
      IOThreadPool ioThreadPool,
      AtomicInteger batchCount) {
    super(name, true);
    this.producerConfig = producerConfig;
    this.clientPool = clientPool;
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
    loopMoveBatches();
    LOGGER.debug("Beginning shutdown of mover thread");
    List<ProducerBatch> incompleteBatches = incompleteBatches();
    LOGGER.debug("Submit incomplete batches, size={}", incompleteBatches.size());
    submitIncompleteBatches(incompleteBatches);
    LOGGER.debug("Shutdown of mover thread has completed");
  }

  private void loopMoveBatches() {
    while (!closed) {
      try {
        moveBatches();
      } catch (Exception e) {
        LOGGER.error("Uncaught exception in mover, e=", e);
      }
    }
  }

  private void moveBatches() {
    LOGGER.debug(
        "Prepare to move expired batches from accumulator and retry queue to ioThreadPool");
    doMoveBatches();
    LOGGER.debug("Move expired batches successfully");
  }

  private void doMoveBatches() {
    ExpiredBatches expiredBatches = accumulator.expiredBatches();
    LOGGER.debug(
        "Expired batches from accumulator, size={}, remainingMs={}",
        expiredBatches.getBatches().size(),
        expiredBatches.getRemainingMs());
    for (ProducerBatch b : expiredBatches.getBatches()) {
      ioThreadPool.submit(createSendProducerBatchTask(b));
    }
    List<ProducerBatch> expiredRetryBatches =
        retryQueue.expiredBatches(expiredBatches.getRemainingMs());
    LOGGER.debug("Expired batches from retry queue, size={}", expiredRetryBatches.size());
    for (ProducerBatch b : expiredRetryBatches) {
      ioThreadPool.submit(createSendProducerBatchTask(b));
    }
  }

  private List<ProducerBatch> incompleteBatches() {
    List<ProducerBatch> incompleteBatches = accumulator.remainingBatches();
    incompleteBatches.addAll(retryQueue.remainingBatches());
    return incompleteBatches;
  }

  private void submitIncompleteBatches(List<ProducerBatch> incompleteBatches) {
    for (ProducerBatch b : incompleteBatches) {
      ioThreadPool.submit(createSendProducerBatchTask(b));
    }
  }

  private SendProducerBatchTask createSendProducerBatchTask(ProducerBatch batch) {
    return new SendProducerBatchTask(
        batch, producerConfig, clientPool, retryQueue, successQueue, failureQueue, batchCount);
  }

  public void close() {
    this.closed = true;
    interrupt();
  }
}
