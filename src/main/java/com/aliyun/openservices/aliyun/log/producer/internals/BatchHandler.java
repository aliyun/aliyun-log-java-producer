package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchHandler extends LogThread {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchHandler.class);

  private final BlockingQueue<ProducerBatch> batches;

  private final AtomicInteger batchCount;

  private final Semaphore memoryController;

  private volatile boolean closed;

  public BatchHandler(
      String name,
      BlockingQueue<ProducerBatch> batches,
      AtomicInteger batchCount,
      Semaphore memoryController) {
    super(name, true);
    this.batches = batches;
    this.batchCount = batchCount;
    this.memoryController = memoryController;
    this.closed = false;
  }

  @Override
  public void run() {
    loopHandleBatches();
    handleRemainingBatches();
  }

  private void loopHandleBatches() {
    while (!closed) {
      try {
        ProducerBatch b = batches.take();
        handle(b);
      } catch (InterruptedException e) {
        LOGGER.info("The batch handler has been interrupted");
      }
    }
  }

  private void handleRemainingBatches() {
    List<ProducerBatch> remainingBatches = new ArrayList<ProducerBatch>();
    batches.drainTo(remainingBatches);
    for (ProducerBatch b : remainingBatches) {
      handle(b);
    }
  }

  private void handle(ProducerBatch batch) {
    try {
      batch.fireCallbacksAndSetFutures();
    } catch (Throwable t) {
      LOGGER.error("Failed to handle batch, batch={}, e=", batch, t);
    } finally {
      batchCount.decrementAndGet();
      memoryController.release(batch.getCurBatchSizeInBytes());
    }
  }

  public boolean isClosed() {
    return closed;
  }

  public void close() {
    this.closed = true;
    interrupt();
  }
}
