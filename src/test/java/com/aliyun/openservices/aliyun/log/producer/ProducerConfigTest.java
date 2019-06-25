package com.aliyun.openservices.aliyun.log.producer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProducerConfigTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testProducerConfigDefaultValue() {
    ProducerConfig producerConfig = new ProducerConfig();
    Assert.assertEquals(
        ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES, producerConfig.getTotalSizeInBytes());
    Assert.assertEquals(ProducerConfig.DEFAULT_MAX_BLOCK_MS, producerConfig.getMaxBlockMs());
    Assert.assertEquals(ProducerConfig.DEFAULT_IO_THREAD_COUNT, producerConfig.getIoThreadCount());
    Assert.assertEquals(
        ProducerConfig.DEFAULT_BATCH_SIZE_THRESHOLD_IN_BYTES,
        producerConfig.getBatchSizeThresholdInBytes());
    Assert.assertEquals(
        ProducerConfig.DEFAULT_BATCH_COUNT_THRESHOLD, producerConfig.getBatchCountThreshold());
    Assert.assertEquals(ProducerConfig.DEFAULT_LINGER_MS, producerConfig.getLingerMs());
    Assert.assertEquals(ProducerConfig.DEFAULT_RETRIES, producerConfig.getRetries());
    Assert.assertEquals(
        ProducerConfig.DEFAULT_RETRIES + 1, producerConfig.getMaxReservedAttempts());
    Assert.assertEquals(
        ProducerConfig.DEFAULT_BASE_RETRY_BACKOFF_MS, producerConfig.getBaseRetryBackoffMs());
    Assert.assertEquals(
        ProducerConfig.DEFAULT_MAX_RETRY_BACKOFF_MS, producerConfig.getMaxRetryBackoffMs());
    Assert.assertTrue(producerConfig.isAdjustShardHash());
    Assert.assertEquals(ProducerConfig.DEFAULT_BUCKETS, producerConfig.getBuckets());
    Assert.assertEquals(ProducerConfig.DEFAULT_LOG_FORMAT, producerConfig.getLogFormat());
  }

  @Test
  public void testInvalidTotalSizeInBytes() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("totalSizeInBytes must be greater than 0, got 0");
    producerConfig.setTotalSizeInBytes(0);
  }

  @Test
  public void testInvalidIoThreadCount() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("ioThreadCount must be greater than 0, got 0");
    producerConfig.setIoThreadCount(0);
  }

  @Test
  public void testInvalidMaxBatchSizeInBytes() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "batchSizeThresholdInBytes must be between 1 and "
            + ProducerConfig.MAX_BATCH_SIZE_IN_BYTES
            + ", got -1");
    producerConfig.setBatchSizeThresholdInBytes(-1);
  }

  @Test
  public void testInvalidMaxBatchCount() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "batchCountThreshold must be between 1 and "
            + ProducerConfig.MAX_BATCH_COUNT
            + ", got -100");
    producerConfig.setBatchCountThreshold(-100);
  }

  @Test
  public void testInvalidLingerMs() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("lingerMs must be greater than or equal to 100, got -1");
    producerConfig.setLingerMs(-1);
  }

  @Test
  public void testInvalidLingerMs2() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("lingerMs must be greater than or equal to 100, got 99");
    producerConfig.setLingerMs(99);
  }

  @Test
  public void testInvalidMaxReservedAttempts() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxReservedAttempts must be greater than 0, got 0");
    producerConfig.setMaxReservedAttempts(0);
  }

  @Test
  public void testInvalidBaseRetryBackoffMs() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("baseRetryBackoffMs must be greater than 0, got 0");
    producerConfig.setBaseRetryBackoffMs(0);
  }

  @Test
  public void testInvalidMaxRetryBackoffMs() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxRetryBackoffMs must be greater than 0, got -1");
    producerConfig.setMaxRetryBackoffMs(-1);
  }

  @Test
  public void testInvalidBuckets() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("buckets must be between 1 and 256, got 0");
    producerConfig.setBuckets(0);
  }

  @Test
  public void testInvalidBuckets2() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("buckets must be between 1 and 256, got 257");
    producerConfig.setBuckets(257);
  }

  @Test
  public void testInvalidBuckets3() {
    ProducerConfig producerConfig = new ProducerConfig();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("buckets must be a power of 2, got 15");
    producerConfig.setBuckets(15);
  }
}
