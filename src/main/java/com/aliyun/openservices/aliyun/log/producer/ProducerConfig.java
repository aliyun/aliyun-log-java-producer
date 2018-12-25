package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.internals.Utils;

public class ProducerConfig {

    public static final int DEFAULT_TOTAL_SIZE_IN_BYTES = 100 * 1024 * 1024;

    public static final long DEFAULT_MAX_BLOCK_MS = 60 * 1000L;

    public static final int DEFAULT_IO_THREAD_COUNT = Math.max(Runtime.getRuntime().availableProcessors() * 2, 1);

    public static final int DEFAULT_MAX_BATCH_SIZE_IN_BYTES = 3 * 1024 * 1024;

    public static final int MAX_BATCH_SIZE_IN_BYTES_UPPER_LIMIT = 3 * 1024 * 1024;

    public static final int DEFAULT_MAX_BATCH_COUNT = 4096;

    public static final int MAX_BATCH_COUNT_UPPER_LIMIT = 4096;

    public static final int DEFAULT_LINGER_MS = 2000;

    public static final int LINGER_MS_LOWER_LIMIT = 100;

    public static final int DEFAULT_RETRIES = 10;

    public static final long DEFAULT_BASE_RETRY_BACKOFF_MS = 100L;

    public static final long DEFAULT_MAX_RETRY_BACKOFF_MS = 600 * 1000L;

    public static final String DEFAULT_USER_AGENT = "aliyun-log-java-producer";

    public enum LogFormat {
        PROTOBUF, JSON
    }

    public static final LogFormat DEFAULT_LOG_FORMAT = LogFormat.PROTOBUF;

    private final ProjectConfigs projectConfigs;

    private int totalSizeInBytes = DEFAULT_TOTAL_SIZE_IN_BYTES;

    private long maxBlockMs = DEFAULT_MAX_BLOCK_MS;

    private int ioThreadCount = DEFAULT_IO_THREAD_COUNT;

    private int maxBatchSizeInBytes = DEFAULT_MAX_BATCH_SIZE_IN_BYTES;

    private int maxBatchCount = DEFAULT_MAX_BATCH_COUNT;

    private int lingerMs = DEFAULT_LINGER_MS;

    private int retries = DEFAULT_RETRIES;

    private int maxReservedAttempts = DEFAULT_RETRIES;

    private long baseRetryBackoffMs = DEFAULT_BASE_RETRY_BACKOFF_MS;

    private long maxRetryBackoffMs = DEFAULT_MAX_RETRY_BACKOFF_MS;

    private String userAgent = DEFAULT_USER_AGENT;

    private LogFormat logFormat = DEFAULT_LOG_FORMAT;

    public ProducerConfig(ProjectConfigs projectConfigs) {
        Utils.assertArgumentNotNull(projectConfigs, "projectConfigs");
        this.projectConfigs = projectConfigs;
    }

    public ProjectConfigs getProjectConfigs() {
        return projectConfigs;
    }

    public int getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public void setTotalSizeInBytes(int totalSizeInBytes) {
        if (totalSizeInBytes <= 0) {
            throw new IllegalArgumentException("totalSizeInBytes must be greater than 0, got " + totalSizeInBytes);
        }
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public long getMaxBlockMs() {
        return maxBlockMs;
    }

    public void setMaxBlockMs(long maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public void setIoThreadCount(int ioThreadCount) {
        if (ioThreadCount <= 0) {
            throw new IllegalArgumentException("ioThreadCount must be greater than 0, got " + ioThreadCount);
        }
        this.ioThreadCount = ioThreadCount;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public void setMaxBatchSizeInBytes(int maxBatchSizeInBytes) {
        if (maxBatchSizeInBytes < 1 || maxBatchSizeInBytes > MAX_BATCH_SIZE_IN_BYTES_UPPER_LIMIT) {
            throw new IllegalArgumentException(String.format("maxBatchSizeInBytes must be between 1 and %d, got %d", MAX_BATCH_SIZE_IN_BYTES_UPPER_LIMIT, maxBatchSizeInBytes));
        }
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
    }

    public int getMaxBatchCount() {
        return maxBatchCount;
    }

    public void setMaxBatchCount(int maxBatchCount) {
        if (maxBatchCount < 1 || maxBatchCount > MAX_BATCH_COUNT_UPPER_LIMIT) {
            throw new IllegalArgumentException(String.format("maxBatchCount must be between 1 and %d, got %d", MAX_BATCH_COUNT_UPPER_LIMIT, maxBatchCount));
        }
        this.maxBatchCount = maxBatchCount;
    }

    public int getLingerMs() {
        return lingerMs;
    }

    public void setLingerMs(int lingerMs) {
        if (lingerMs < LINGER_MS_LOWER_LIMIT) {
            throw new IllegalArgumentException(String.format("lingerMs must be greater than or equal to %d, got %d", LINGER_MS_LOWER_LIMIT, lingerMs));
        }
        this.lingerMs = lingerMs;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getMaxReservedAttempts() {
        return maxReservedAttempts;
    }

    public void setMaxReservedAttempts(int maxReservedAttempts) {
        if (maxReservedAttempts <= 0) {
            throw new IllegalArgumentException("maxReservedAttempts must be greater than 0, got " + baseRetryBackoffMs);
        }
        this.maxReservedAttempts = maxReservedAttempts;
    }

    public long getBaseRetryBackoffMs() {
        return baseRetryBackoffMs;
    }

    public void setBaseRetryBackoffMs(long baseRetryBackoffMs) {
        if (baseRetryBackoffMs <= 0) {
            throw new IllegalArgumentException("baseRetryBackoffMs must be greater than 0, got " + baseRetryBackoffMs);
        }
        this.baseRetryBackoffMs = baseRetryBackoffMs;
    }

    public long getMaxRetryBackoffMs() {
        return maxRetryBackoffMs;
    }

    public void setMaxRetryBackoffMs(long maxRetryBackoffMs) {
        if (maxRetryBackoffMs <= 0) {
            throw new IllegalArgumentException("maxRetryBackoffMs must be greater than 0, got " + maxRetryBackoffMs);
        }
        this.maxRetryBackoffMs = maxRetryBackoffMs;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public LogFormat getLogFormat() {
        return logFormat;
    }

    public void setLogFormat(LogFormat logFormat) {
        this.logFormat = logFormat;
    }

}
