package com.aliyun.openservices.aliyun.log.producer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProducerConfigTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testProducerConfigDefaultValue() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        Assert.assertEquals(ProducerConfig.DEFAULT_TOTAL_SIZE_IN_BYTES, producerConfig.getTotalSizeInBytes());
        Assert.assertEquals(ProducerConfig.DEFAULT_MAX_BLOCK_MS, producerConfig.getMaxBlockMs());
        Assert.assertEquals(ProducerConfig.DEFAULT_IO_THREAD_COUNT, producerConfig.getIoThreadCount());
        Assert.assertEquals(ProducerConfig.DEFAULT_MAX_BATCH_SIZE_IN_BYTES, producerConfig.getMaxBatchSizeInBytes());
        Assert.assertEquals(ProducerConfig.DEFAULT_MAX_BATCH_COUNT, producerConfig.getMaxBatchCount());
        Assert.assertEquals(ProducerConfig.DEFAULT_LINGER_MS, producerConfig.getLingerMs());
        Assert.assertEquals(ProducerConfig.DEFAULT_RETRIES, producerConfig.getRetries());
        Assert.assertEquals(ProducerConfig.DEFAULT_BASE_RETRY_BACKOFF_MS, producerConfig.getBaseRetryBackoffMs());
        Assert.assertEquals(ProducerConfig.DEFAULT_MAX_RETRY_BACKOFF_MS, producerConfig.getMaxRetryBackoffMs());
        Assert.assertEquals(ProducerConfig.DEFAULT_USER_AGENT, producerConfig.getUserAgent());
        Assert.assertEquals(ProducerConfig.DEFAULT_LOG_FORMAT, producerConfig.getLogFormat());
    }

    @Test
    public void testInvalidProjectConfigs() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("projectConfigs cannot be null");
        new ProducerConfig(null);
    }

    @Test
    public void testInvalidTotalSizeInBytes() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("totalSizeInBytes must be greater than 0, got 0");
        producerConfig.setTotalSizeInBytes(0);
    }

    @Test
    public void testInvalidIoThreadCount() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("ioThreadCount must be greater than 0, got 0");
        producerConfig.setIoThreadCount(0);
    }

    @Test
    public void testInvalidMaxBatchSizeInBytes() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("maxBatchSizeInBytes must be between 1 and " + ProducerConfig.MAX_BATCH_SIZE_IN_BYTES_UPPER_LIMIT + ", got -1");
        producerConfig.setMaxBatchSizeInBytes(-1);
    }

    @Test
    public void testInvalidMaxBatchCount() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("maxBatchCount must be between 1 and " + ProducerConfig.MAX_BATCH_COUNT_UPPER_LIMIT + ", got -100");
        producerConfig.setMaxBatchCount(-100);
    }

    @Test
    public void testInvalidLingerMs() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("lingerMs must be greater than 0, got 0");
        producerConfig.setLingerMs(0);
    }

    @Test
    public void testInvalidBaseRetryBackoffMs() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("baseRetryBackoffMs must be greater than 0, got 0");
        producerConfig.setBaseRetryBackoffMs(0);
    }

    @Test
    public void testInvalidMaxRetryBackoffMs() {
        ProducerConfig producerConfig = new ProducerConfig(new ProjectConfigs());
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("maxRetryBackoffMs must be greater than 0, got -1");
        producerConfig.setMaxRetryBackoffMs(-1);
    }

}
