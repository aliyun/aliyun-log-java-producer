package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.LogSizeTooLargeException;
import com.aliyun.openservices.aliyun.log.producer.errors.MaxBatchCountExceedException;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.internals.*;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.http.client.ClientConfiguration;
import com.aliyun.openservices.log.http.comm.ServiceClient;
import com.aliyun.openservices.log.http.comm.TimeoutServiceClient;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client that send logs to the Aliyun Log Service.
 *
 * <p>The producer is <i>thread safe</i> and sharing a single producer instance across threads will
 * generally be faster than having multiple instances.
 */
public class LogProducer implements Producer {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogProducer.class);

  private static final AtomicInteger INSTANCE_ID_GENERATOR = new AtomicInteger(0);

  private static final String LOG_PRODUCER_PREFIX = "aliyun-log-producer-";

  private static final String MOVER_SUFFIX = "-mover";

  private static final String SUCCESS_BATCH_HANDLER_SUFFIX = "-success-batch-handler";

  private static final String FAILURE_BATCH_HANDLER_SUFFIX = "-failure-batch-handler";

  private static final String TIMEOUT_THREAD_SUFFIX_FORMAT = "-timeout-thread-%d";

  private final int instanceId;

  private final String name;

  private final String producerHash;

  private final ProducerConfig producerConfig;

  private final Map<String, Client> clientPool = new ConcurrentHashMap<String, Client>();

  private final ServiceClient serviceClient;

  private final Semaphore memoryController;

  private final RetryQueue retryQueue;

  private final IOThreadPool ioThreadPool;

  private final ThreadPoolExecutor timeoutThreadPool;

  private final LogAccumulator accumulator;

  private final Mover mover;

  private final BatchHandler successBatchHandler;

  private final BatchHandler failureBatchHandler;

  private final AtomicInteger batchCount = new AtomicInteger(0);

  private final ShardHashAdjuster adjuster;

  /**
   * Start up a LogProducer instance.
   *
   * <p>Since this creates a series of threads and data structures, it is fairly expensive. Avoid
   * creating more than one per application.
   *
   * <p>All methods in LogProducer are thread-safe.
   *
   * @param producerConfig Configuration for the LogProducer. See the docs for that class for
   *     details.
   * @see ProducerConfig
   */
  public LogProducer(ProducerConfig producerConfig) {
    this.instanceId = INSTANCE_ID_GENERATOR.getAndIncrement();
    this.name = LOG_PRODUCER_PREFIX + this.instanceId;
    this.producerHash = Utils.generateProducerHash(this.instanceId);
    this.producerConfig = producerConfig;
    this.memoryController = new Semaphore(producerConfig.getTotalSizeInBytes());
    this.retryQueue = new RetryQueue();
    BlockingQueue<ProducerBatch> successQueue = new LinkedBlockingQueue<ProducerBatch>();
    BlockingQueue<ProducerBatch> failureQueue = new LinkedBlockingQueue<ProducerBatch>();
    this.ioThreadPool = new IOThreadPool(producerConfig.getIoThreadCount(), this.name);
    this.timeoutThreadPool =
        new ThreadPoolExecutor(
            producerConfig.getIoThreadCount(),
            producerConfig.getIoThreadCount(),
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(this.name + TIMEOUT_THREAD_SUFFIX_FORMAT)
                .build());
    this.serviceClient =
        new TimeoutServiceClient(new ClientConfiguration(), this.timeoutThreadPool);
    this.accumulator =
        new LogAccumulator(
            this.producerHash,
            producerConfig,
            this.clientPool,
            this.memoryController,
            this.retryQueue,
            successQueue,
            failureQueue,
            this.ioThreadPool,
            this.batchCount);
    this.mover =
        new Mover(
            this.name + MOVER_SUFFIX,
            producerConfig,
            this.clientPool,
            this.accumulator,
            this.retryQueue,
            successQueue,
            failureQueue,
            this.ioThreadPool,
            this.batchCount);
    this.successBatchHandler =
        new BatchHandler(
            this.name + SUCCESS_BATCH_HANDLER_SUFFIX,
            successQueue,
            this.batchCount,
            this.memoryController);
    this.failureBatchHandler =
        new BatchHandler(
            this.name + FAILURE_BATCH_HANDLER_SUFFIX,
            failureQueue,
            this.batchCount,
            this.memoryController);
    this.mover.start();
    this.successBatchHandler.start();
    this.failureBatchHandler.start();
    this.adjuster = new ShardHashAdjuster(producerConfig.getBuckets());
  }

  /**
   * Send a log asynchronously. Equivalent to <code>send(project, logStore, "", "", "", logItem,
   * null)</code>. See {@link #send(String, String, String, String, String, LogItem, Callback)} for
   * details.
   */
  @Override
  public ListenableFuture<Result> send(String project, String logStore, LogItem logItem)
      throws InterruptedException, ProducerException {
    return send(project, logStore, "", "", null, logItem, null);
  }

  /**
   * Send a list of logs asynchronously. Equivalent to <code>send(project, logStore, "", "", "",
   * logItems, null)</code>. See {@link #send(String, String, String, String, String, List,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(String project, String logStore, List<LogItem> logItems)
      throws InterruptedException, ProducerException {
    return send(project, logStore, "", "", null, logItems, null);
  }

  /**
   * Send a log asynchronously. Equivalent to <code>send(project, logStore, topic, source, "",
   * logItem, null)</code>. See {@link #send(String, String, String, String, String, LogItem,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project, String logStore, String topic, String source, LogItem logItem)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, null, logItem, null);
  }

  /**
   * Send a list of logs asynchronously. Equivalent to <code>send(project, logStore, topic, source,
   * "", logItems, null)</code>. See {@link #send(String, String, String, String, String, List,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project, String logStore, String topic, String source, List<LogItem> logItems)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, null, logItems, null);
  }

  /**
   * Send a log asynchronously. Equivalent to <code>send(project, logStore, topic, source,
   * shardHash, logItem, null)</code>. See {@link #send(String, String, String, String, String,
   * LogItem, Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      LogItem logItem)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, shardHash, logItem, null);
  }

  /**
   * Send a list of logs asynchronously. Equivalent to <code>send(project, logStore, topic, source,
   * shardHash, logItems, null)</code>. See {@link #send(String, String, String, String, String,
   * List, Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      List<LogItem> logItems)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, shardHash, logItems, null);
  }

  /**
   * Send a log asynchronously. Equivalent to <code>send(project, logStore, "", "", "", logItem,
   * callback)</code>. See {@link #send(String, String, String, String, String, LogItem, Callback)}
   * for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project, String logStore, LogItem logItem, Callback callback)
      throws InterruptedException, ProducerException {
    return send(project, logStore, "", "", null, logItem, callback);
  }

  /**
   * Send a list of logs asynchronously. Equivalent to <code>send(project, logStore, "", "", "",
   * logItems, callback)</code>. See {@link #send(String, String, String, String, String, List,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project, String logStore, List<LogItem> logItems, Callback callback)
      throws InterruptedException, ProducerException {
    return send(project, logStore, "", "", null, logItems, callback);
  }

  /**
   * Send a log asynchronously. Equivalent to <code>send(project, logStore, topic, source, "",
   * logItem, callback)</code>. See {@link #send(String, String, String, String, String, LogItem,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      LogItem logItem,
      Callback callback)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, null, logItem, callback);
  }

  /**
   * Send a list of logs asynchronously. Equivalent to <code>send(project, logStore, topic, source,
   * "", logItems, callback)</code>. See {@link #send(String, String, String, String, String, List,
   * Callback)} for details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      List<LogItem> logItems,
      Callback callback)
      throws InterruptedException, ProducerException {
    return send(project, logStore, topic, source, null, logItems, callback);
  }

  /**
   * Send a log asynchronously. It will convert the log to a log list which only contains one log,
   * and then invoke <code>send(project, logStore, topic, source, logItems, logItem,
   * callback)</code>. See {@link #send(String, String, String, String, String, List, Callback)} for
   * details.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      LogItem logItem,
      Callback callback)
      throws InterruptedException, ProducerException {
    Utils.assertArgumentNotNull(logItem, "logItem");
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(logItem);
    return send(project, logStore, topic, source, shardHash, logItems, callback);
  }

  /**
   * Asynchronously send a list of logs and invoke the provided callback when the send has been
   * acknowledged.
   *
   * <p>The send is asynchronous and this method will return immediately once the logs has been
   * stored in the buffer of logs waiting to be sent. This allows sending many logs in parallel
   * without blocking to wait for the response after each one.
   *
   * <p>A {@link ListenableFuture} is returned that can be used to retrieve the result, either by
   * polling or by registering a callback.
   *
   * <p>The return value can be disregarded if you do not wish to process the result. Under the
   * covers, the producer will automatically re-attempt sends in case of transient errors (including
   * throttling). A failed result is generally returned only if an irrecoverable error is detected
   * (e.g. trying to send to a project or logstore that doesn't exist).
   *
   * <p>Note that callbacks will generally execute in the background batch handler thread of the
   * producer and so should be reasonably fast or they will delay the sending of logs from other
   * threads. If you want to execute blocking or computationally expensive callbacks it is
   * recommended to use your own {@link java.util.concurrent.Executor} in the callback body to
   * parallelize processing.
   *
   * @param project The target project.
   * @param logStore The target logstore.
   * @param topic The topic of the logs.
   * @param source The source of the logs.
   * @param shardHash The shardHash of the logs, used to write this logs to a specific shard in the
   *     logStore.
   * @param logItems The logs to send.
   * @param callback A user-supplied callback to execute when the logs has been acknowledged by the
   *     server (null indicates no callback).
   * @throws IllegalArgumentException If input does not meet stated constraints.
   * @throws IllegalStateException If you want to send logs after the producer was closed.
   * @throws InterruptedException If the thread is interrupted while blocked.
   * @throws com.aliyun.openservices.aliyun.log.producer.errors.TimeoutException If the time taken
   *     for allocating memory for the logs has surpassed.
   * @throws MaxBatchCountExceedException If the log list size exceeds {@link
   *     ProducerConfig#getBatchCountThreshold()}.
   * @throws LogSizeTooLargeException If the total size of the logs exceeds {@link
   *     ProducerConfig#getTotalSizeInBytes()} or {@link
   *     ProducerConfig#getBatchSizeThresholdInBytes()}.
   * @throws ProducerException If a producer related exception occurs that does not belong to the
   *     above exceptions.
   */
  @Override
  public ListenableFuture<Result> send(
      String project,
      String logStore,
      String topic,
      String source,
      String shardHash,
      List<LogItem> logItems,
      Callback callback)
      throws InterruptedException, ProducerException {
    Utils.assertArgumentNotNullOrEmpty(project, "project");
    Utils.assertArgumentNotNullOrEmpty(logStore, "logStore");
    if (topic == null) {
      topic = "";
    }
    Utils.assertArgumentNotNull(logItems, "logItems");
    if (logItems.isEmpty()) {
      throw new IllegalArgumentException("logItems cannot be empty");
    }
    int count = logItems.size();
    if (count > ProducerConfig.MAX_BATCH_COUNT) {
      throw new MaxBatchCountExceedException(
          "the log list size is "
              + count
              + " which exceeds the MAX_BATCH_COUNT "
              + ProducerConfig.MAX_BATCH_COUNT);
    }
    if (shardHash != null && producerConfig.isAdjustShardHash()) {
      shardHash = adjuster.adjust(shardHash);
    }
    return accumulator.append(project, logStore, topic, source, shardHash, logItems, callback);
  }

  /**
   * Close this producer. This method blocks until all previously submitted logs have been handled
   * and all background threads are stopped. This method is equivalent to <code>
   * close(Long.MAX_VALUE)</code>.
   *
   * <p><strong>If close() is called from {@link Callback}, a warning message will be logged and it
   * will skip join batch handler thread. We do this because the sender thread would otherwise try
   * to join itself and block forever.</strong>
   *
   * @throws InterruptedException If the thread is interrupted while blocked.
   * @throws ProducerException If the background threads is still alive.
   */
  @Override
  public void close() throws InterruptedException, ProducerException {
    close(Long.MAX_VALUE);
  }

  /**
   * This method waits up to <code>timeoutMs</code> for the producer to handle all submitted logs
   * and close all background threads.
   *
   * <p><strong>If this method is called from {@link Callback}, a warning message will be logged and
   * it will skip join batch handler thread. We do this because the sender thread would otherwise
   * try to join itself and block forever.</strong>
   *
   * @param timeoutMs The maximum time to wait for producer to handle all submitted logs and close
   *     all background threads. The value should be non-negative. Specifying a timeout of zero
   *     means do not wait for pending send requests to complete.
   * @throws IllegalArgumentException If the <code>timeoutMs</code> is negative.
   * @throws InterruptedException If the thread is interrupted while blocked.
   * @throws ProducerException If the producer is unable to handle all submitted logs or close all
   *     background threads.
   */
  @Override
  public void close(long timeoutMs) throws InterruptedException, ProducerException {
    if (timeoutMs < 0) {
      throw new IllegalArgumentException(
          "timeoutMs must be greater than or equal to 0, got " + timeoutMs);
    }
    ProducerException firstException = null;
    LOGGER.info("Closing the log producer, timeoutMs={}", timeoutMs);
    try {
      timeoutMs = closeMover(timeoutMs);
    } catch (ProducerException e) {
      firstException = e;
    }
    LOGGER.debug("After close mover, timeoutMs={}", timeoutMs);
    try {
      timeoutMs = closeIOThreadPool(timeoutMs);
    } catch (ProducerException e) {
      if (firstException == null) {
        firstException = e;
      }
    }
    LOGGER.debug("After close ioThreadPool, timeoutMs={}", timeoutMs);
    try {
      timeoutMs = closeTimeoutThreadPool(timeoutMs);
    } catch (ProducerException e) {
      if (firstException == null) {
        firstException = e;
      }
    }
    LOGGER.debug("After close timeoutThreadPool, timeoutMs={}", timeoutMs);
    try {
      timeoutMs = closeSuccessBatchHandler(timeoutMs);
    } catch (ProducerException e) {
      if (firstException == null) {
        firstException = e;
      }
    }
    LOGGER.debug("After close success batch handler, timeoutMs={}", timeoutMs);
    try {
      timeoutMs = closeFailureBatchHandler(timeoutMs);
    } catch (ProducerException e) {
      if (firstException == null) {
        firstException = e;
      }
    }
    LOGGER.debug("After close failure batch handler, timeoutMs={}", timeoutMs);
    if (firstException != null) {
      throw firstException;
    }
    LOGGER.info("The log producer has been closed");
  }

  private long closeMover(long timeoutMs) throws InterruptedException, ProducerException {
    long startMs = System.currentTimeMillis();
    accumulator.close();
    retryQueue.close();
    mover.close();
    mover.join(timeoutMs);
    if (mover.isAlive()) {
      LOGGER.warn("The mover thread is still alive");
      throw new ProducerException("the mover thread is still alive");
    }
    long nowMs = System.currentTimeMillis();
    return Math.max(0, timeoutMs - nowMs + startMs);
  }

  private long closeIOThreadPool(long timeoutMs) throws InterruptedException, ProducerException {
    long startMs = System.currentTimeMillis();
    ioThreadPool.shutdown();
    if (ioThreadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
      LOGGER.debug("The ioThreadPool is terminated");
    } else {
      LOGGER.warn("The ioThreadPool is not fully terminated");
      throw new ProducerException("the ioThreadPool is not fully terminated");
    }
    long nowMs = System.currentTimeMillis();
    return Math.max(0, timeoutMs - nowMs + startMs);
  }

  private long closeTimeoutThreadPool(long timeoutMs)
      throws InterruptedException, ProducerException {
    long startMs = System.currentTimeMillis();
    timeoutThreadPool.shutdown();
    if (timeoutThreadPool.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
      LOGGER.debug("The timeoutThreadPool is terminated");
    } else {
      LOGGER.warn("The timeoutThreadPool is not fully terminated");
      throw new ProducerException("the timeoutThreadPool is not fully terminated");
    }
    long nowMs = System.currentTimeMillis();
    return Math.max(0, timeoutMs - nowMs + startMs);
  }

  private long closeSuccessBatchHandler(long timeoutMs)
      throws InterruptedException, ProducerException {
    long startMs = System.currentTimeMillis();
    successBatchHandler.close();
    boolean invokedFromCallback = Thread.currentThread() == this.successBatchHandler;
    if (invokedFromCallback) {
      LOGGER.warn(
          "Skip join success batch handler since you have incorrectly invoked close from the producer call-back");
      return timeoutMs;
    }
    successBatchHandler.join(timeoutMs);
    if (successBatchHandler.isAlive()) {
      LOGGER.warn("The success batch handler thread is still alive");
      throw new ProducerException("the success batch handler thread is still alive");
    }
    long nowMs = System.currentTimeMillis();
    return Math.max(0, timeoutMs - nowMs + startMs);
  }

  private long closeFailureBatchHandler(long timeoutMs)
      throws InterruptedException, ProducerException {
    long startMs = System.currentTimeMillis();
    failureBatchHandler.close();
    boolean invokedFromCallback =
        Thread.currentThread() == this.successBatchHandler
            || Thread.currentThread() == this.failureBatchHandler;
    if (invokedFromCallback) {
      LOGGER.warn(
          "Skip join failure batch handler since you have incorrectly invoked close from the producer call-back");
      return timeoutMs;
    }
    failureBatchHandler.join(timeoutMs);
    if (failureBatchHandler.isAlive()) {
      LOGGER.warn("The failure batch handler thread is still alive");
      throw new ProducerException("the failure batch handler thread is still alive");
    }
    long nowMs = System.currentTimeMillis();
    return Math.max(0, timeoutMs - nowMs + startMs);
  }

  /** @return Producer config of the producer instance. */
  @Override
  public ProducerConfig getProducerConfig() {
    return producerConfig;
  }

  /** @return Uncompleted batch count in the producer. */
  @Override
  public int getBatchCount() {
    return batchCount.get();
  }

  /** @return Available memory size in the producer. */
  @Override
  public int availableMemoryInBytes() {
    return memoryController.availablePermits();
  }

  /** Add or update a project config. */
  @Override
  public void putProjectConfig(ProjectConfig projectConfig) {
    Client client = buildClient(projectConfig);
    clientPool.put(projectConfig.getProject(), client);
  }

  /** Remove a project config. */
  @Override
  public void removeProjectConfig(ProjectConfig projectConfig) {
    clientPool.remove(projectConfig.getProject());
  }

  private Client buildClient(ProjectConfig projectConfig) {
    Client client =
        new Client(
            projectConfig.getEndpoint(),
            projectConfig.getAccessKeyId(),
            projectConfig.getAccessKeySecret(),
            serviceClient);
    String userAgent = projectConfig.getUserAgent();
    if (userAgent != null) {
      client.setUserAgent(userAgent);
    }
    String stsToken = projectConfig.getStsToken();
    if (stsToken != null) {
      client.setSecurityToken(stsToken);
    }
    return client;
  }
}
