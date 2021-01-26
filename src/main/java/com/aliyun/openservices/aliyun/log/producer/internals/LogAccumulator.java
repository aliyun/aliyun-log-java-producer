package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.Callback;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.aliyun.log.producer.errors.LogSizeTooLargeException;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.errors.TimeoutException;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogAccumulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogAccumulator.class);

  private static final AtomicLong BATCH_ID = new AtomicLong(0);

  private final String producerHash;

  private final ProducerConfig producerConfig;

  private final Map<String, Client> clientPool;

  private final Semaphore memoryController;

  private final RetryQueue retryQueue;

  private final BlockingQueue<ProducerBatch> successQueue;

  private final BlockingQueue<ProducerBatch> failureQueue;

  private final IOThreadPool ioThreadPool;

  private final AtomicInteger batchCount;

  private final ConcurrentMap<GroupKey, ProducerBatchHolder> batches;

  private final AtomicInteger appendsInProgress;

  private volatile boolean closed;

  public LogAccumulator(
      String producerHash,
      ProducerConfig producerConfig,
      Map<String, Client> clientPool,
      Semaphore memoryController,
      RetryQueue retryQueue,
      BlockingQueue<ProducerBatch> successQueue,
      BlockingQueue<ProducerBatch> failureQueue,
      IOThreadPool ioThreadPool,
      AtomicInteger batchCount) {
    this.producerHash = producerHash;
    this.producerConfig = producerConfig;
    this.clientPool = clientPool;
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

  public ListenableFuture<Result> append(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      List<LogItem> logItems,
      Callback callback)
      throws InterruptedException, ProducerException {
    appendsInProgress.incrementAndGet();
    try {
      return doAppend(project, logStore, topic, source, shardHash, logItems, callback);
    } finally {
      appendsInProgress.decrementAndGet();
    }
  }

  private ListenableFuture<Result> doAppend(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      List<LogItem> logItems,
      Callback callback)
      throws InterruptedException, ProducerException {
    if (closed) {
      throw new IllegalStateException("cannot append after the log accumulator was closed");
    }
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    ensureValidLogSize(sizeInBytes);
    long maxBlockMs = producerConfig.getMaxBlockMs();
    LOGGER.trace(
        "Prepare to acquire bytes, sizeInBytes={}, maxBlockMs={}, project={}, logStore={}",
        sizeInBytes,
        maxBlockMs,
        project,
        logStore);
    if (maxBlockMs >= 0) {
      boolean acquired =
          memoryController.tryAcquire(sizeInBytes, maxBlockMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        LOGGER.warn(
            "Failed to acquire memory within the configured max blocking time {} ms, "
                + "requiredSizeInBytes={}, availableSizeInBytes={}",
            producerConfig.getMaxBlockMs(),
            sizeInBytes,
            memoryController.availablePermits());
        throw new TimeoutException(
            "failed to acquire memory within the configured max blocking time "
                + producerConfig.getMaxBlockMs()
                + " ms");
      }
    } else {
      memoryController.acquire(sizeInBytes);
    }
    try {
      GroupKey groupKey = new GroupKey(project, logStore, topic, source, shardHash);
      ProducerBatchHolder holder = getOrCreateProducerBatchHolder(groupKey);
      synchronized (holder) {
        return appendToHolder(groupKey, logItems, callback, sizeInBytes, holder);
      }
    } catch (Exception e) {
      memoryController.release(sizeInBytes);
      throw new ProducerException(e);
    }
  }

  private ListenableFuture<Result> appendToHolder(
      GroupKey groupKey,
      List<LogItem> logItems,
      Callback callback,
      int sizeInBytes,
      ProducerBatchHolder holder) {
    if (holder.producerBatch != null) {
      ListenableFuture<Result> f = holder.producerBatch.tryAppend(logItems, sizeInBytes, callback);
      if (f != null) {
        if (holder.producerBatch.isMeetSendCondition()) {
          holder.transferProducerBatch(
              ioThreadPool,
              producerConfig,
              clientPool,
              retryQueue,
              successQueue,
              failureQueue,
              batchCount);
        }
        return f;
      } else {
        holder.transferProducerBatch(
            ioThreadPool,
            producerConfig,
            clientPool,
            retryQueue,
            successQueue,
            failureQueue,
            batchCount);
      }
    }
    holder.producerBatch =
        new ProducerBatch(
            groupKey,
            Utils.generatePackageId(producerHash, BATCH_ID),
            producerConfig.getBatchSizeThresholdInBytes(),
            producerConfig.getBatchCountThreshold(),
            producerConfig.getMaxReservedAttempts(),
            System.currentTimeMillis());
    ListenableFuture<Result> f = holder.producerBatch.tryAppend(logItems, sizeInBytes, callback);
    batchCount.incrementAndGet();
    if (holder.producerBatch.isMeetSendCondition()) {
      holder.transferProducerBatch(
          ioThreadPool,
          producerConfig,
          clientPool,
          retryQueue,
          successQueue,
          failureQueue,
          batchCount);
    }
    return f;
  }

  public ExpiredBatches expiredBatches() {
    long nowMs = System.currentTimeMillis();
    ExpiredBatches expiredBatches = new ExpiredBatches();
    long remainingMs = producerConfig.getLingerMs();
    for (Map.Entry<GroupKey, ProducerBatchHolder> entry : batches.entrySet()) {
      ProducerBatchHolder holder = entry.getValue();
      synchronized (holder) {
        if (holder.producerBatch == null) {
          continue;
        }
        long curRemainingMs = holder.producerBatch.remainingMs(nowMs, producerConfig.getLingerMs());
        if (curRemainingMs <= 0) {
          holder.transferProducerBatch(expiredBatches);
        } else {
          remainingMs = Math.min(remainingMs, curRemainingMs);
        }
      }
    }
    expiredBatches.setRemainingMs(remainingMs);
    return expiredBatches;
  }

  public List<ProducerBatch> remainingBatches() {
    if (!closed) {
      throw new IllegalStateException(
          "cannot get the remaining batches before the log accumulator closed");
    }
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
        if (holder.producerBatch == null) {
          continue;
        }
        c.add(holder.producerBatch);
        ++n;
        holder.producerBatch = null;
      }
    }
    return n;
  }

  private void ensureValidLogSize(int sizeInBytes) throws LogSizeTooLargeException {
    if (sizeInBytes > ProducerConfig.MAX_BATCH_SIZE_IN_BYTES) {
      throw new LogSizeTooLargeException(
          "the logs is "
              + sizeInBytes
              + " bytes which is larger than MAX_BATCH_SIZE_IN_BYTES "
              + ProducerConfig.MAX_BATCH_SIZE_IN_BYTES);
    }
    if (sizeInBytes > producerConfig.getTotalSizeInBytes()) {
      throw new LogSizeTooLargeException(
          "the logs is "
              + sizeInBytes
              + " bytes which is larger than the totalSizeInBytes you specified");
    }
  }

  private ProducerBatchHolder getOrCreateProducerBatchHolder(GroupKey groupKey) {
    ProducerBatchHolder holder = batches.get(groupKey);
    if (holder != null) {
      return holder;
    }
    holder = new ProducerBatchHolder();
    ProducerBatchHolder previous = batches.putIfAbsent(groupKey, holder);
    if (previous == null) {
      return holder;
    } else {
      return previous;
    }
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

  private static final class ProducerBatchHolder {

    ProducerBatch producerBatch;

    void transferProducerBatch(
        IOThreadPool ioThreadPool,
        ProducerConfig producerConfig,
        Map<String, Client> clientPool,
        RetryQueue retryQueue,
        BlockingQueue<ProducerBatch> successQueue,
        BlockingQueue<ProducerBatch> failureQueue,
        AtomicInteger batchCount) {
      if (producerBatch == null) {
        return;
      }
      ioThreadPool.submit(
          new SendProducerBatchTask(
              producerBatch,
              producerConfig,
              clientPool,
              retryQueue,
              successQueue,
              failureQueue,
              batchCount));
      producerBatch = null;
    }

    void transferProducerBatch(ExpiredBatches expiredBatches) {
      if (producerBatch == null) {
        return;
      }
      expiredBatches.add(producerBatch);
      producerBatch = null;
    }
  }
}
