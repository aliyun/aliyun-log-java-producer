package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryQueue {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryQueue.class);

  private final DelayQueue<ProducerBatch> retryBatches = new DelayQueue<ProducerBatch>();

  private final AtomicInteger putsInProgress;

  private volatile boolean closed;

  public RetryQueue() {
    this.putsInProgress = new AtomicInteger(0);
    this.closed = false;
  }

  public void put(ProducerBatch batch) {
    putsInProgress.incrementAndGet();
    try {
      if (closed) {
        throw new IllegalStateException("cannot put after the retry queue was closed");
      }
      retryBatches.put(batch);
    } finally {
      putsInProgress.decrementAndGet();
    }
  }

  public List<ProducerBatch> expiredBatches(long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    List<ProducerBatch> expiredBatches = new ArrayList<ProducerBatch>();
    retryBatches.drainTo(expiredBatches);
    if (!expiredBatches.isEmpty()) {
      return expiredBatches;
    }
    while (true) {
      if (timeoutMs < 0) {
        break;
      }
      ProducerBatch batch;
      try {
        batch = retryBatches.poll(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted when poll batch from the retry batches");
        break;
      }
      if (batch == null) {
        break;
      }
      expiredBatches.add(batch);
      retryBatches.drainTo(expiredBatches);
      if (!expiredBatches.isEmpty()) {
        break;
      }
      timeoutMs = deadline - System.currentTimeMillis();
    }
    return expiredBatches;
  }

  public List<ProducerBatch> remainingBatches() {
    if (!closed) {
      throw new IllegalStateException(
          "cannot get the remaining batches before the retry queue closed");
    }
    while (true) {
      if (!putsInProgress()) {
        break;
      }
    }
    List<ProducerBatch> remainingBatches = new ArrayList<ProducerBatch>(retryBatches);
    retryBatches.clear();
    return remainingBatches;
  }

  public boolean isClosed() {
    return closed;
  }

  public void close() {
    this.closed = true;
  }

  private boolean putsInProgress() {
    return putsInProgress.get() > 0;
  }
}
