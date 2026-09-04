package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class LogAccumulatorTest {

  @Test
  public void testSubmitDoesNotHoldProducerBatchHolderLock() throws Exception {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setBatchCountThreshold(1);
    final BlockingIOThreadPool ioThreadPool = new BlockingIOThreadPool();
    Map<String, Client> clientPool = Collections.emptyMap();
    BlockingQueue<ProducerBatch> successQueue = new LinkedBlockingQueue<ProducerBatch>();
    BlockingQueue<ProducerBatch> failureQueue = new LinkedBlockingQueue<ProducerBatch>();
    final LogAccumulator accumulator =
        new LogAccumulator(
            "producer-hash",
            producerConfig,
            clientPool,
            new Semaphore(producerConfig.getTotalSizeInBytes()),
            new RetryQueue(),
            successQueue,
            failureQueue,
            ioThreadPool,
            new AtomicInteger());
    ExecutorService appenders = Executors.newFixedThreadPool(2);

    try {
      Future<ListenableFuture<Result>> firstAppend = appenders.submit(newAppendTask(accumulator));
      Assert.assertTrue(ioThreadPool.firstSubmitEntered.await(5, TimeUnit.SECONDS));

      Future<ListenableFuture<Result>> secondAppend = appenders.submit(newAppendTask(accumulator));
      Assert.assertNotNull(secondAppend.get(5, TimeUnit.SECONDS));

      ioThreadPool.allowFirstSubmitToReturn.countDown();
      Assert.assertNotNull(firstAppend.get(5, TimeUnit.SECONDS));
    } finally {
      ioThreadPool.allowFirstSubmitToReturn.countDown();
      appenders.shutdownNow();
      ioThreadPool.shutdown();
    }
  }

  @Test
  public void testAppendCanTransferOldAndNewBatchOutsideLock() throws Exception {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setBatchCountThreshold(ProducerConfig.MAX_BATCH_COUNT);
    final RecordingIOThreadPool ioThreadPool = new RecordingIOThreadPool();
    final AtomicInteger batchCount = new AtomicInteger();
    LogAccumulator accumulator =
        new LogAccumulator(
            "producer-hash",
            producerConfig,
            Collections.<String, Client>emptyMap(),
            new Semaphore(producerConfig.getTotalSizeInBytes()),
            new RetryQueue(),
            new LinkedBlockingQueue<ProducerBatch>(),
            new LinkedBlockingQueue<ProducerBatch>(),
            ioThreadPool,
            batchCount);

    try {
      accumulator.append(
          "project",
          "logStore",
          "topic",
          "source",
          null,
          Collections.singletonList(new LogItem()),
          null);
      Assert.assertEquals(0, ioThreadPool.submitCount.get());

      accumulator.append(
          "project",
          "logStore",
          "topic",
          "source",
          null,
          newLogItems(ProducerConfig.MAX_BATCH_COUNT),
          null);
      Assert.assertEquals(2, ioThreadPool.submitCount.get());
      Assert.assertEquals(2, batchCount.get());
    } finally {
      ioThreadPool.shutdown();
    }
  }

  private Callable<ListenableFuture<Result>> newAppendTask(final LogAccumulator accumulator) {
    return new Callable<ListenableFuture<Result>>() {
      @Override
      public ListenableFuture<Result> call() throws Exception {
        return accumulator.append(
            "project",
            "logStore",
            "topic",
            "source",
            null,
            Collections.singletonList(new LogItem()),
            null);
      }
    };
  }

  private static List<LogItem> newLogItems(int count) {
    List<LogItem> logItems = new ArrayList<LogItem>(count);
    for (int i = 0; i < count; ++i) {
      logItems.add(new LogItem());
    }
    return logItems;
  }

  private static final class BlockingIOThreadPool extends IOThreadPool {

    private final AtomicInteger submitCount = new AtomicInteger();
    private final CountDownLatch firstSubmitEntered = new CountDownLatch(1);
    private final CountDownLatch allowFirstSubmitToReturn = new CountDownLatch(1);

    BlockingIOThreadPool() {
      super(1, "test-log-accumulator");
    }

    @Override
    public void submit(SendProducerBatchTask task) {
      if (submitCount.incrementAndGet() != 1) {
        return;
      }
      firstSubmitEntered.countDown();
      try {
        allowFirstSubmitToReturn.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private static final class RecordingIOThreadPool extends IOThreadPool {

    private final AtomicInteger submitCount = new AtomicInteger();

    RecordingIOThreadPool() {
      super(1, "test-log-accumulator-recording");
    }

    @Override
    public void submit(SendProducerBatchTask task) {
      submitCount.incrementAndGet();
    }
  }
}
