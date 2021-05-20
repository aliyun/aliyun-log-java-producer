package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.errors.ResultFailedException;
import com.aliyun.openservices.aliyun.log.producer.errors.RetriableErrors;
import com.aliyun.openservices.aliyun.log.producer.internals.LogSizeCalculator;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class ProducerTest {

  @Test
  public void testSend() throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    ListenableFuture<Result> f =
        producer.send(System.getenv("PROJECT"), System.getenv("LOG_STORE"), buildLogItem());
    Result result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    f =
        producer.send(
            System.getenv("PROJECT"), System.getenv("LOG_STORE"), null, null, buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    f = producer.send(System.getenv("PROJECT"), System.getenv("LOG_STORE"), "", "", buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    f =
        producer.send(
            System.getenv("PROJECT"),
            System.getenv("LOG_STORE"),
            "topic",
            "source",
            buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    producer.close();
    assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithCallback()
      throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    final AtomicInteger successCount = new AtomicInteger(0);
    ListenableFuture<Result> f =
        producer.send(
            System.getenv("PROJECT"),
            System.getenv("LOG_STORE"),
            buildLogItem(),
            new Callback() {
              @Override
              public void onCompletion(Result result) {
                if (result.isSuccessful()) {
                  successCount.incrementAndGet();
                }
              }
            });
    Result result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    f =
        producer.send(
            System.getenv("PROJECT"),
            System.getenv("LOG_STORE"),
            null,
            null,
            buildLogItem(),
            new Callback() {
              @Override
              public void onCompletion(Result result) {
                if (result.isSuccessful()) {
                  successCount.incrementAndGet();
                }
              }
            });
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("PROJECT"),
            System.getenv("LOG_STORE"),
            "",
            "",
            buildLogItem(),
            new Callback() {
              @Override
              public void onCompletion(Result result) {
                if (result.isSuccessful()) {
                  successCount.incrementAndGet();
                }
              }
            });
    result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    f =
        producer.send(
            System.getenv("PROJECT"),
            System.getenv("LOG_STORE"),
            "topic",
            "source",
            buildLogItem(),
            new Callback() {
              @Override
              public void onCompletion(Result result) {
                if (result.isSuccessful()) {
                  successCount.incrementAndGet();
                }
              }
            });
    result = f.get();
    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals("", result.getErrorCode());
    Assert.assertEquals("", result.getErrorMessage());
    Assert.assertEquals(1, result.getReservedAttempts().size());
    Assert.assertTrue(!result.getReservedAttempts().get(0).getRequestId().isEmpty());

    Assert.assertEquals(4, successCount.get());

    producer.close();
    assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithInvalidAccessKeyId() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setRetries(4);
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildInvalidAccessKeyIdProjectConfig());
    ListenableFuture<Result> f =
        producer.send(System.getenv("PROJECT"), System.getenv("LOG_STORE"), buildLogItem());
    Thread.sleep(1000 * 3);
    producer.putProjectConfig(buildProjectConfig());
    try {
      Result result = f.get();
      Assert.assertTrue(result.isSuccessful());
      Assert.assertTrue(result.getErrorCode().isEmpty());
      Assert.assertTrue(result.getErrorMessage().isEmpty());
      List<Attempt> attempts = result.getReservedAttempts();
      System.out.println(attempts.size());
      for (int i = 0; i < attempts.size(); ++i) {
        Attempt attempt = attempts.get(i);
        if (i == attempts.size() - 1) {
          Assert.assertTrue(attempt.isSuccess());
          Assert.assertTrue(result.getErrorCode().isEmpty());
          Assert.assertTrue(result.getErrorMessage().isEmpty());
        } else {
          Assert.assertFalse(attempt.isSuccess());
          Assert.assertEquals("Unauthorized", attempt.getErrorCode());
          Assert.assertFalse(attempt.getErrorMessage().isEmpty());
        }
      }

    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Result result = resultFailedException.getResult();
      Assert.assertFalse(result.isSuccessful());
      Assert.assertEquals("SignatureNotMatch", result.getErrorCode());
      Assert.assertTrue(!result.getErrorMessage().isEmpty());
      List<Attempt> attempts = result.getReservedAttempts();
      Assert.assertEquals(1, attempts.size());
      for (Attempt attempt : attempts) {
        Assert.assertFalse(attempt.isSuccess());
        Assert.assertEquals("SignatureNotMatch", attempt.getErrorCode());
        Assert.assertTrue(!attempt.getErrorMessage().isEmpty());
        Assert.assertTrue(!attempt.getRequestId().isEmpty());
      }
    }
  }

  @Test
  public void testSendWithInvalidAccessKeySecret() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildInvalidAccessKeySecretProjectConfig());
    ListenableFuture<Result> f =
        producer.send(System.getenv("PROJECT"), System.getenv("LOG_STORE"), buildLogItem());
    try {
      f.get();
    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Result result = resultFailedException.getResult();
      Assert.assertFalse(result.isSuccessful());
      Assert.assertEquals(RetriableErrors.SIGNATURE_NOT_MATCH, result.getErrorCode());
      Assert.assertTrue(!result.getErrorMessage().isEmpty());
      List<Attempt> attempts = result.getReservedAttempts();
      Assert.assertEquals(11, attempts.size());
      for (Attempt attempt : attempts) {
        Assert.assertFalse(attempt.isSuccess());
        Assert.assertEquals(RetriableErrors.SIGNATURE_NOT_MATCH, attempt.getErrorCode());
        Assert.assertTrue(!attempt.getErrorMessage().isEmpty());
        Assert.assertTrue(!attempt.getRequestId().isEmpty());
      }
    }
  }

  @Test
  public void testClose() throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    final AtomicInteger successCount = new AtomicInteger(0);
    int futureGetCount = 0;
    int n = 100000;
    List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
    for (int i = 0; i < n; ++i) {
      ListenableFuture<Result> f =
          producer.send(
              System.getenv("PROJECT"),
              System.getenv("LOG_STORE"),
              buildLogItem(),
              new Callback() {
                @Override
                public void onCompletion(Result result) {
                  if (result.isSuccessful()) {
                    successCount.incrementAndGet();
                  }
                }
              });
      futures.add(f);
    }
    producer.close();
    for (ListenableFuture<?> f : futures) {
      Result result = (Result) f.get();
      Assert.assertTrue(result.isSuccessful());
      futureGetCount++;
    }
    Assert.assertEquals(n, successCount.get());
    Assert.assertEquals(n, futureGetCount);
    assertProducerFinalState(producer);
  }

  @Test
  public void testCloseInCallback()
      throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    final AtomicInteger successCount = new AtomicInteger(0);
    int futureGetCount = 0;
    int n = 10000;
    List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
    for (int i = 0; i < n; ++i) {
      ListenableFuture<Result> f =
          producer.send(
              System.getenv("PROJECT"),
              System.getenv("LOG_STORE"),
              buildLogItem(),
              new Callback() {
                @Override
                public void onCompletion(Result result) {
                  if (result.isSuccessful()) {
                    successCount.incrementAndGet();
                  }
                  try {
                    producer.close();
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                }
              });
      futures.add(f);
    }
    producer.close();
    for (ListenableFuture<?> f : futures) {
      Result result = (Result) f.get();
      Assert.assertTrue(result.isSuccessful());
      futureGetCount++;
    }
    Assert.assertEquals(n, successCount.get());
    Assert.assertEquals(n, futureGetCount);
    assertProducerFinalState(producer);
  }

  @Test
  public void testMaxBatchSizeInBytes() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setBatchSizeThresholdInBytes(27);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    LogItem logItem = new LogItem();
    logItem.PushBack("key1", "val1");
    logItem.PushBack("key2", "val2");
    logItem.PushBack("key3", "val3");
    int sizeInBytes = LogSizeCalculator.calculate(logItem);
    Assert.assertEquals(28, sizeInBytes);
    producer.send("project", "logStore", new LogItem());
  }

  public static void assertProducerFinalState(Producer producer) {
    Assert.assertEquals(0, producer.getBatchCount());
    Assert.assertEquals(
        producer.getProducerConfig().getTotalSizeInBytes(), producer.availableMemoryInBytes());
  }

  public static LogItem buildLogItem() {
    LogItem logItem = new LogItem();
    logItem.PushBack("k1", "v1");
    logItem.PushBack("k2", "v2");
    return logItem;
  }

  public static List<LogItem> buildLogItems(int n) {
    List<LogItem> logItems = new ArrayList<LogItem>();
    for (int i = 0; i < n; ++i) {
      logItems.add(buildLogItem());
    }
    return logItems;
  }

  private ProjectConfig buildProjectConfig() {
    String project = System.getenv("PROJECT");
    String endpoint = System.getenv("ENDPOINT");
    String accessKeyId = System.getenv("ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ACCESS_KEY_SECRET");
    return new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret);
  }

  private ProjectConfig buildInvalidAccessKeyIdProjectConfig() {
    String project = System.getenv("PROJECT");
    String endpoint = System.getenv("ENDPOINT");
    String accessKeyId = System.getenv("ACCESS_KEY_ID") + "XXX";
    String accessKeySecret = System.getenv("ACCESS_KEY_SECRET");
    return new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret);
  }

  private ProjectConfig buildInvalidAccessKeySecretProjectConfig() {
    String project = System.getenv("PROJECT");
    String endpoint = System.getenv("ENDPOINT");
    String accessKeyId = System.getenv("ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ACCESS_KEY_SECRET") + "XXX";
    return new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret);
  }
}
