package com.aliyun.openservices.aliyun.log.producer;

/**
 * Configuration for {@link LogProducer}. See each each individual set method for details about each
 * parameter.
 */
public class ProducerConfig {

  public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 100 * 1024 * 1024;

  public static final long DEFAULT_MAX_BLOCK_MS = 60 * 1000L;

  public static final int DEFAULT_IO_THREAD_COUNT =
      Math.max(Runtime.getRuntime().availableProcessors(), 1);

  public static final int DEFAULT_BATCH_SIZE_THRESHOLD_IN_BYTES = 512 * 1024;

  public static final int MAX_BATCH_SIZE_IN_BYTES = 8 * 1024 * 1024;

  public static final int DEFAULT_BATCH_COUNT_THRESHOLD = 4096;

  public static final int MAX_BATCH_COUNT = 40960;

  public static final int DEFAULT_LINGER_MS = 2000;

  public static final int LINGER_MS_LOWER_LIMIT = 100;

  public static final int DEFAULT_RETRIES = 10;

  public static final long DEFAULT_BASE_RETRY_BACKOFF_MS = 100L;

  public static final long DEFAULT_MAX_RETRY_BACKOFF_MS = 50 * 1000L;

  public static final int DEFAULT_BUCKETS = 64;

  public static final int BUCKETS_LOWER_LIMIT = 1;

  public static final int BUCKETS_UPPER_LIMIT = 256;

  public enum LogFormat {
    PROTOBUF,
    JSON
  }

  public static final LogFormat DEFAULT_LOG_FORMAT = LogFormat.PROTOBUF;

  private int totalSizeInBytes = DEFAULT_TOTAL_SIZE_IN_BYTES;

  private long maxBlockMs = DEFAULT_MAX_BLOCK_MS;

  private int ioThreadCount = DEFAULT_IO_THREAD_COUNT;

  private int batchSizeThresholdInBytes = DEFAULT_BATCH_SIZE_THRESHOLD_IN_BYTES;

  private int batchCountThreshold = DEFAULT_BATCH_COUNT_THRESHOLD;

  private int lingerMs = DEFAULT_LINGER_MS;

  private int retries = DEFAULT_RETRIES;

  private int maxReservedAttempts = DEFAULT_RETRIES + 1;

  private long baseRetryBackoffMs = DEFAULT_BASE_RETRY_BACKOFF_MS;

  private long maxRetryBackoffMs = DEFAULT_MAX_RETRY_BACKOFF_MS;

  private boolean adjustShardHash = true;

  private int buckets = DEFAULT_BUCKETS;

  private LogFormat logFormat = DEFAULT_LOG_FORMAT;

  /**
   * @return The total bytes of memory the producer can use to buffer logs waiting to be sent to the
   *     server.
   */
  public int getTotalSizeInBytes() {
    return totalSizeInBytes;
  }

  /**
   * Set the total bytes of memory the producer can use to buffer logs waiting to be sent to the
   * server.
   */
  public void setTotalSizeInBytes(int totalSizeInBytes) {
    if (totalSizeInBytes <= 0) {
      throw new IllegalArgumentException(
          "totalSizeInBytes must be greater than 0, got " + totalSizeInBytes);
    }
    this.totalSizeInBytes = totalSizeInBytes;
  }

  /** @return How long <code>LogProducer.send()</code> will block. */
  public long getMaxBlockMs() {
    return maxBlockMs;
  }

  /** Set how long <code>LogProducer.send()</code> will block. */
  public void setMaxBlockMs(long maxBlockMs) {
    this.maxBlockMs = maxBlockMs;
  }

  /** @return The thread count of the background I/O thread pool. */
  public int getIoThreadCount() {
    return ioThreadCount;
  }

  /** Set the thread count of the background I/O thread pool. */
  public void setIoThreadCount(int ioThreadCount) {
    if (ioThreadCount <= 0) {
      throw new IllegalArgumentException(
          "ioThreadCount must be greater than 0, got " + ioThreadCount);
    }
    this.ioThreadCount = ioThreadCount;
  }

  /** @return The batch size threshold. */
  public int getBatchSizeThresholdInBytes() {
    return batchSizeThresholdInBytes;
  }

  /** Set the batch size threshold. */
  public void setBatchSizeThresholdInBytes(int batchSizeThresholdInBytes) {
    if (batchSizeThresholdInBytes < 1 || batchSizeThresholdInBytes > MAX_BATCH_SIZE_IN_BYTES) {
      throw new IllegalArgumentException(
          String.format(
              "batchSizeThresholdInBytes must be between 1 and %d, got %d",
              MAX_BATCH_SIZE_IN_BYTES, batchSizeThresholdInBytes));
    }
    this.batchSizeThresholdInBytes = batchSizeThresholdInBytes;
  }

  /** @return The batch count threshold. */
  public int getBatchCountThreshold() {
    return batchCountThreshold;
  }

  /** Set the batch count threshold. */
  public void setBatchCountThreshold(int batchCountThreshold) {
    if (batchCountThreshold < 1 || batchCountThreshold > MAX_BATCH_COUNT) {
      throw new IllegalArgumentException(
          String.format(
              "batchCountThreshold must be between 1 and %d, got %d",
              MAX_BATCH_COUNT, batchCountThreshold));
    }
    this.batchCountThreshold = batchCountThreshold;
  }

  /** @return The max linger time of a log. */
  public int getLingerMs() {
    return lingerMs;
  }

  /** Set the max linger time of a log. */
  public void setLingerMs(int lingerMs) {
    if (lingerMs < LINGER_MS_LOWER_LIMIT) {
      throw new IllegalArgumentException(
          String.format(
              "lingerMs must be greater than or equal to %d, got %d",
              LINGER_MS_LOWER_LIMIT, lingerMs));
    }
    this.lingerMs = lingerMs;
  }

  /** @return The retry times for transient error. */
  public int getRetries() {
    return retries;
  }

  /**
   * Set the retry times for transient error. Setting a value greater than zero will cause the
   * client to resend any log whose send fails with a potentially transient error.
   */
  public void setRetries(int retries) {
    this.retries = retries;
  }

  /** @return How many {@link Attempt}s will be reserved in a {@link Result}. */
  public int getMaxReservedAttempts() {
    return maxReservedAttempts;
  }

  /** Set how many {@link Attempt}s will be reserved in a {@link Result}. */
  public void setMaxReservedAttempts(int maxReservedAttempts) {
    if (maxReservedAttempts <= 0) {
      throw new IllegalArgumentException(
          "maxReservedAttempts must be greater than 0, got " + maxReservedAttempts);
    }
    this.maxReservedAttempts = maxReservedAttempts;
  }

  /**
   * @return The amount of time to wait before attempting to retry a failed request for the first
   *     time.
   */
  public long getBaseRetryBackoffMs() {
    return baseRetryBackoffMs;
  }

  /**
   * Set the amount of time to wait before attempting to retry a failed request for the first time.
   */
  public void setBaseRetryBackoffMs(long baseRetryBackoffMs) {
    if (baseRetryBackoffMs <= 0) {
      throw new IllegalArgumentException(
          "baseRetryBackoffMs must be greater than 0, got " + baseRetryBackoffMs);
    }
    this.baseRetryBackoffMs = baseRetryBackoffMs;
  }

  /** @return The upper limit of time to wait before attempting to retry a failed request. */
  public long getMaxRetryBackoffMs() {
    return maxRetryBackoffMs;
  }

  /** Set the upper limit of time to wait before attempting to retry a failed request. */
  public void setMaxRetryBackoffMs(long maxRetryBackoffMs) {
    if (maxRetryBackoffMs <= 0) {
      throw new IllegalArgumentException(
          "maxRetryBackoffMs must be greater than 0, got " + maxRetryBackoffMs);
    }
    this.maxRetryBackoffMs = maxRetryBackoffMs;
  }

  /** @return The flag of whether to adjust shard hash. */
  public boolean isAdjustShardHash() {
    return adjustShardHash;
  }

  /** Specify whether to adjust shard hash. */
  public void setAdjustShardHash(boolean adjustShardHash) {
    this.adjustShardHash = adjustShardHash;
  }

  /** @return The buckets of the shard hash. */
  public int getBuckets() {
    return buckets;
  }

  /** Set the buckets of the shard hash. */
  public void setBuckets(int buckets) {
    if (buckets < BUCKETS_LOWER_LIMIT || buckets > BUCKETS_UPPER_LIMIT) {
      throw new IllegalArgumentException(
          String.format(
              "buckets must be between %d and %d, got %d",
              BUCKETS_LOWER_LIMIT, BUCKETS_UPPER_LIMIT, buckets));
    }
    if (!ShardHashAdjuster.isPowerOfTwo(buckets)) {
      throw new IllegalArgumentException("buckets must be a power of 2, got " + buckets);
    }
    this.buckets = buckets;
  }

  /** @return The content type of the request. */
  public LogFormat getLogFormat() {
    return logFormat;
  }

  /** Set the content type of the request. */
  public void setLogFormat(LogFormat logFormat) {
    this.logFormat = logFormat;
  }
}
