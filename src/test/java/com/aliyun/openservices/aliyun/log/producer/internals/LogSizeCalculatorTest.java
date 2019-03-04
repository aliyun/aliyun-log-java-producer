package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.ProducerTest;
import com.aliyun.openservices.log.common.LogItem;
import org.junit.Assert;
import org.junit.Test;

public class LogSizeCalculatorTest {

  @Test
  public void testCalculateLog() {
    ProducerTest.buildLogItem();
    int sizeInBytes = LogSizeCalculator.calculate(ProducerTest.buildLogItem());
    Assert.assertEquals(12, sizeInBytes);

    LogItem logItem = new LogItem();
    logItem.PushBack("key1", "val1");
    logItem.PushBack("key2", "val2");
    logItem.PushBack("key3", "val3");
    sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(28, sizeInBytes);
  }

  @Test
  public void testCalculateLogNullKey() {
    LogItem logItem = new LogItem();
    logItem.PushBack("key1", "val1");
    logItem.PushBack("key2", "val2");
    logItem.PushBack(null, "null_key");
    int sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(28, sizeInBytes);
  }

  @Test
  public void testCalculateLogNullValue() {
    LogItem logItem = new LogItem();
    logItem.PushBack("key1", "val1");
    logItem.PushBack("key2", "val2");
    logItem.PushBack("null_value", null);
    int sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(30, sizeInBytes);
  }

  @Test
  public void testCalculateLogNullKeyAndValue() {
    LogItem logItem = new LogItem();
    logItem.PushBack(null, null);
    logItem.PushBack(null, null);
    logItem.PushBack(null, null);
    int sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(4, sizeInBytes);
  }

  @Test
  public void testCalculateLogs() {
    ProducerTest.buildLogItem();
    int sizeInBytes = LogSizeCalculator.calculate(ProducerTest.buildLogItems(4));
    Assert.assertEquals(48, sizeInBytes);
  }
}
