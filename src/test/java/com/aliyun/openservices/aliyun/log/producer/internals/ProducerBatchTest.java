package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.Attempt;
import com.aliyun.openservices.aliyun.log.producer.ProducerTest;
import com.aliyun.openservices.aliyun.log.producer.Result;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProducerBatchTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTryAppendLog() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch = new ProducerBatch(groupKey, "id", 35, 10, 3, System.currentTimeMillis());
    LogItem logItem = new LogItem();
    logItem.PushBack("key1", "val1");
    logItem.PushBack("key2", "val2");
    logItem.PushBack("key3", "val3");
    logItem.PushBack("key4", "val4");
    int sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(36, sizeInBytes);
    ListenableFuture<Result> f = batch.tryAppend(logItem, sizeInBytes, null);
    Assert.assertNotNull(f);
    Assert.assertTrue(batch.isMeetSendCondition());
  }

  @Test
  public void testTryAppendLogsExceedBatchSizeThreshold() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch = new ProducerBatch(groupKey, "id", 20, 10, 3, System.currentTimeMillis());
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(ProducerTest.buildLogItem());
    logItems.add(ProducerTest.buildLogItem());
    logItems.add(ProducerTest.buildLogItem());
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    Assert.assertEquals(36, sizeInBytes);
    ListenableFuture<Result> f = batch.tryAppend(logItems, sizeInBytes, null);
    Assert.assertNotNull(f);
    Assert.assertTrue(batch.isMeetSendCondition());
  }

  @Test
  public void testTryAppendLogsExceedBatchCountThreshold() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch =
        new ProducerBatch(groupKey, "id", 10000, 1, 3, System.currentTimeMillis());
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    Assert.assertEquals(8, sizeInBytes);
    ListenableFuture<Result> f = batch.tryAppend(logItems, sizeInBytes, null);
    Assert.assertNotNull(f);
    Assert.assertTrue(batch.isMeetSendCondition());
  }

  @Test
  public void testIsMeetSendCondition() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch = new ProducerBatch(groupKey, "id", 8, 100, 3, System.currentTimeMillis());
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    Assert.assertEquals(8, sizeInBytes);
    ListenableFuture<Result> f = batch.tryAppend(logItems, sizeInBytes, null);
    Assert.assertNotNull(f);
    Assert.assertTrue(batch.isMeetSendCondition());
  }

  @Test
  public void testAppendAttempt() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch =
        new ProducerBatch(groupKey, "id", 100, 100, 3, System.currentTimeMillis());
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    ListenableFuture<Result> f = batch.tryAppend(logItems, sizeInBytes, null);
    Assert.assertNotNull(f);
    batch.appendAttempt(new Attempt(true, "xxx", "", "", System.currentTimeMillis()));
    batch.fireCallbacksAndSetFutures();
  }

  @Test
  public void testFireCallbacksAndSetFutures() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch =
        new ProducerBatch(groupKey, "id", 100, 100, 3, System.currentTimeMillis());
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    logItems.add(new LogItem());
    int sizeInBytes = LogSizeCalculator.calculate(logItems);
    ListenableFuture<Result> f = batch.tryAppend(logItems, sizeInBytes, null);
    Assert.assertNotNull(f);
    thrown.expect(NoSuchElementException.class);
    batch.fireCallbacksAndSetFutures();
  }

  @Test
  public void testRemainingMs() {
    GroupKey groupKey = new GroupKey("project", "logStore", "topic", "source", "shardHash");
    ProducerBatch batch = new ProducerBatch(groupKey, "id", 100, 100, 3, 1000);
    Assert.assertEquals(200, batch.remainingMs(0, 200));
    Assert.assertEquals(200, batch.remainingMs(1000, 200));
    Assert.assertEquals(50, batch.remainingMs(1150, 200));
    Assert.assertEquals(0, batch.remainingMs(1200, 200));
    Assert.assertEquals(-100, batch.remainingMs(1300, 200));
  }
}
