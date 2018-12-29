package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class RetryQueueTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPutAfterClose() {
    RetryQueue retryQueue = new RetryQueue();
    retryQueue.put(newProducerBatch("1"));
    retryQueue.close();
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("cannot put after the retry queue was closed");
    retryQueue.put(newProducerBatch("2"));
    retryQueue.remainingBatches();
  }

  @Test
  public void testExpiredBatches() throws InterruptedException {
    RetryQueue retryQueue = new RetryQueue();
    ProducerBatch b1 = newProducerBatch("1");
    ProducerBatch b2 = newProducerBatch("2");
    ProducerBatch b3 = newProducerBatch("3");
    ProducerBatch b4 = newProducerBatch("4");
    ProducerBatch b5 = newProducerBatch("5");
    long nowMs = System.currentTimeMillis();
    b1.setNextRetryMs(nowMs + 3000);
    b2.setNextRetryMs(nowMs + 5000);
    b3.setNextRetryMs(nowMs + 1000);
    b4.setNextRetryMs(nowMs + 4000);
    b5.setNextRetryMs(nowMs + 2000);
    retryQueue.put(b1);
    retryQueue.put(b2);
    retryQueue.put(b3);
    retryQueue.put(b4);
    retryQueue.put(b5);
    Thread.sleep(1000);
    List<ProducerBatch> expiredBatches = retryQueue.expiredBatches(10);
    Assert.assertEquals(1, expiredBatches.size());
    Assert.assertEquals("3", expiredBatches.get(0).getPackageId());
    Thread.sleep(1000);
    expiredBatches = retryQueue.expiredBatches(10);
    Assert.assertEquals(1, expiredBatches.size());
    Assert.assertEquals("5", expiredBatches.get(0).getPackageId());
    Thread.sleep(1000);
    expiredBatches = retryQueue.expiredBatches(10);
    Assert.assertEquals(1, expiredBatches.size());
    Assert.assertEquals("1", expiredBatches.get(0).getPackageId());
    Thread.sleep(1000);
    expiredBatches = retryQueue.expiredBatches(10);
    Assert.assertEquals(1, expiredBatches.size());
    Assert.assertEquals("4", expiredBatches.get(0).getPackageId());
    Thread.sleep(1000);
    expiredBatches = retryQueue.expiredBatches(10);
    Assert.assertEquals(1, expiredBatches.size());
    Assert.assertEquals("2", expiredBatches.get(0).getPackageId());
  }

  @Test
  public void testRemainingBatches() {
    RetryQueue retryQueue = new RetryQueue();
    ProducerBatch b1 = newProducerBatch("1");
    ProducerBatch b2 = newProducerBatch("2");
    ProducerBatch b3 = newProducerBatch("3");
    b1.setNextRetryMs(500);
    b2.setNextRetryMs(100);
    b3.setNextRetryMs(300);
    retryQueue.put(b1);
    retryQueue.put(b2);
    retryQueue.put(b3);
    retryQueue.close();
    List<ProducerBatch> remainingBatches = retryQueue.remainingBatches();
    Assert.assertEquals(3, remainingBatches.size());
    Assert.assertEquals("2", remainingBatches.get(0).getPackageId());
  }

  @Test
  public void testInvalidRemainingBatches() {
    RetryQueue retryQueue = new RetryQueue();
    retryQueue.put(newProducerBatch("1"));
    retryQueue.put(newProducerBatch("2"));
    retryQueue.put(newProducerBatch("3"));
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("cannot get the remaining batches before the retry queue closed");
    retryQueue.remainingBatches();
  }

  private ProducerBatch newProducerBatch(String packageId) {
    GroupKey groupKey = new GroupKey("project", "logStore", "", "", "");
    return new ProducerBatch(groupKey, packageId, 100, 100, 3, System.currentTimeMillis());
  }
}
