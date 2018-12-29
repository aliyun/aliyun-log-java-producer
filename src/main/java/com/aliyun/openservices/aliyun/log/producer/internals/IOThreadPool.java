package com.aliyun.openservices.aliyun.log.producer.internals;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.*;

public class IOThreadPool {

  private static final String IO_THREAD_SUFFIX_FORMAT = "-io-thread-%d";

  private final ExecutorService ioThreadPool;

  public IOThreadPool(int ioThreadCount, String prefix) {
    this.ioThreadPool =
        Executors.newFixedThreadPool(
            ioThreadCount,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(prefix + IO_THREAD_SUFFIX_FORMAT)
                .build());
  }

  public void submit(SendProducerBatchTask task) {
    ioThreadPool.submit(task);
  }

  public void shutdown() {
    ioThreadPool.shutdown();
  }

  public boolean isTerminated() {
    return ioThreadPool.isTerminated();
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return ioThreadPool.awaitTermination(timeout, unit);
  }
}
