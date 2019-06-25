package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.Errors;
import com.aliyun.openservices.aliyun.log.producer.errors.LogSizeTooLargeException;
import com.aliyun.openservices.aliyun.log.producer.errors.MaxBatchCountExceedException;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.aliyun.log.producer.errors.ResultFailedException;
import com.aliyun.openservices.aliyun.log.producer.errors.TimeoutException;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProducerInvalidTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSendWithNullProject() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(
        new ProjectConfig("project", "endpoint", "accessKeyId", "accessKeySecret"));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("project cannot be null");
    producer.send(null, "logStore", new LogItem());
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithEmptyProject() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("project cannot be empty");
    producer.send("", "logStore", new LogItem());
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithNotExistProject() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    ListenableFuture<Result> f = producer.send("projectNotExist", "logStore", new LogItem(), null);
    try {
      f.get();
    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Result result = resultFailedException.getResult();
      Assert.assertFalse(result.isSuccessful());
      Assert.assertEquals(Errors.PROJECT_CONFIG_NOT_EXIST, result.getErrorCode());
      Assert.assertEquals(
          "Cannot get the projectConfig for project projectNotExist", result.getErrorMessage());
    }
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithNullLogStore() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("logStore cannot be null");
    producer.send("project", null, new LogItem());
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithEmptyLogStore() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("logStore cannot be empty");
    producer.send("project", "", new LogItem());
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithNullLogItem() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("logItem cannot be null");
    producer.send("project", "logStore", (LogItem) null);
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithNullLogItems() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("logItems cannot be null");
    producer.send("project", "logStore", (List<LogItem>) null);
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithEmptyLogItems() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("logItems cannot be empty");
    List<LogItem> logItems = new ArrayList<LogItem>();
    producer.send("project", "logStore", logItems);
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendLogsThrownMaxBatchCountExceedException()
      throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(MaxBatchCountExceedException.class);
    thrown.expectMessage(
        "the log list size is 40961 which exceeds the MAX_BATCH_COUNT "
            + ProducerConfig.MAX_BATCH_COUNT);
    List<LogItem> logItems = new ArrayList<LogItem>();
    for (int i = 0; i < ProducerConfig.MAX_BATCH_COUNT + 1; ++i) {
      logItems.add(ProducerTest.buildLogItem());
    }
    producer.send("project", "logStore", logItems);
  }

  @Test
  public void testSendLogThrownLogSizeTooLargeException()
      throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setTotalSizeInBytes(10);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(LogSizeTooLargeException.class);
    thrown.expectMessage(
        "the logs is 12 bytes which is larger than the totalSizeInBytes you specified");
    producer.send("project", "logStore", ProducerTest.buildLogItem());
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendLogsThrownLogSizeTooLargeException()
      throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setTotalSizeInBytes(30);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(LogSizeTooLargeException.class);
    thrown.expectMessage(
        "the logs is 36 bytes which is larger than the totalSizeInBytes you specified");
    List<LogItem> logItems = new ArrayList<LogItem>();
    logItems.add(ProducerTest.buildLogItem());
    logItems.add(ProducerTest.buildLogItem());
    logItems.add(ProducerTest.buildLogItem());
    producer.send("project", "logStore", logItems);
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendLogThrownTimeoutException() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setTotalSizeInBytes(20);
    producerConfig.setMaxBlockMs(3);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    thrown.expect(TimeoutException.class);
    producer.send("project", "logStore", ProducerTest.buildLogItem());
    producer.send("project", "logStore", ProducerTest.buildLogItem());
  }

  @Test
  public void testSendWithRequestError() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    int retries = 5;
    producerConfig.setRetries(retries);
    producerConfig.setMaxReservedAttempts(retries + 1);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    ListenableFuture<Result> f = producer.send("project", "logStore", ProducerTest.buildLogItem());
    try {
      f.get();
    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Result result = resultFailedException.getResult();
      Assert.assertFalse(result.isSuccessful());
      Assert.assertEquals("RequestError", result.getErrorCode());
      Assert.assertTrue(
          result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
      List<Attempt> attempts = result.getReservedAttempts();
      Assert.assertEquals(retries + 1, attempts.size());
      long t1;
      long t2 = -1;
      for (int i = 0; i < attempts.size(); ++i) {
        Attempt attempt = attempts.get(i);
        Assert.assertFalse(attempt.isSuccess());
        Assert.assertEquals("RequestError", attempt.getErrorCode());
        Assert.assertTrue(
            attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        Assert.assertEquals("", attempt.getRequestId());
        t1 = t2;
        t2 = attempt.getTimestampMs();
        if (i == 0) {
          continue;
        }
        long diff = t2 - t1;
        long retryBackoffMs = producerConfig.getBaseRetryBackoffMs() * LongMath.pow(2, i - 1);
        long low = retryBackoffMs - (long) (producerConfig.getBaseRetryBackoffMs() * 0.1);
        long high = retryBackoffMs + (long) (producerConfig.getBaseRetryBackoffMs() * 0.2);
        if (i == 1) {
          Assert.assertTrue(low <= diff);
        } else {
          Assert.assertTrue(low <= diff && diff <= high);
        }
      }
    }
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testSendWithRequestError2() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    int retries = 5;
    int maxReservedAttempts = 2;
    producerConfig.setRetries(retries);
    producerConfig.setMaxReservedAttempts(maxReservedAttempts);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    ListenableFuture<Result> f = producer.send("project", "logStore", ProducerTest.buildLogItem());
    try {
      f.get();
    } catch (ExecutionException e) {
      ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
      Result result = resultFailedException.getResult();
      Assert.assertFalse(result.isSuccessful());
      Assert.assertEquals("RequestError", result.getErrorCode());
      Assert.assertTrue(
          result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
      List<Attempt> attempts = result.getReservedAttempts();
      Assert.assertEquals(maxReservedAttempts, attempts.size());
      Assert.assertEquals(retries + 1, result.getAttemptCount());
      for (Attempt attempt : attempts) {
        Assert.assertFalse(attempt.isSuccess());
        Assert.assertEquals("RequestError", attempt.getErrorCode());
        Assert.assertTrue(
            attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        Assert.assertEquals("", attempt.getRequestId());
      }
    }
    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testCloseMultiTimes() throws InterruptedException, ProducerException {
    ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setRetries(3);
    Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    int n = 1000000;
    int futureGetCount = 0;
    List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
    for (int i = 0; i < n; ++i) {
      ListenableFuture<Result> f =
          producer.send("project", "logStore", ProducerTest.buildLogItem());
      futures.add(f);
    }
    for (int i = 0; i < 1000; ++i) {
      closeInMultiThreads(producer, 1);
    }
    for (ListenableFuture<?> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        futureGetCount++;
        ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
        Result result = resultFailedException.getResult();
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals("RequestError", result.getErrorCode());
        Assert.assertTrue(
            result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        List<Attempt> attempts = result.getReservedAttempts();
        Assert.assertTrue(attempts.size() >= 1);
        for (Attempt attempt : attempts) {
          Assert.assertFalse(attempt.isSuccess());
          Assert.assertEquals("RequestError", attempt.getErrorCode());
          Assert.assertTrue(
              attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        }
      }
    }
    Assert.assertEquals(n, futureGetCount);
    ProducerTest.assertProducerFinalState(producer);
  }

  @Test
  public void testCloseInMultiThreads() throws InterruptedException, ProducerException {
    final ProducerConfig producerConfig = new ProducerConfig();
    producerConfig.setRetries(3);
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(buildProjectConfig());
    int n = 1000000;
    int futureGetCount = 0;
    List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
    for (int i = 0; i < n; ++i) {
      ListenableFuture<Result> f =
          producer.send("project", "logStore", ProducerTest.buildLogItem());
      futures.add(f);
    }
    closeInMultiThreads(producer, 100);
    for (ListenableFuture<?> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        futureGetCount++;
        ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
        Result result = resultFailedException.getResult();
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals("RequestError", result.getErrorCode());
        Assert.assertTrue(
            result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        List<Attempt> attempts = result.getReservedAttempts();
        Assert.assertTrue(attempts.size() >= 1);
        for (Attempt attempt : attempts) {
          Assert.assertFalse(attempt.isSuccess());
          Assert.assertEquals("RequestError", attempt.getErrorCode());
          Assert.assertTrue(
              attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
        }
      }
    }
    Assert.assertEquals(n, futureGetCount);
  }

  private void closeInMultiThreads(final Producer producer, int nTasks) {
    ExecutorService executorService = Executors.newFixedThreadPool(nTasks);
    List<Future> closeFutures = new ArrayList<Future>();
    for (int i = 0; i < nTasks; ++i) {
      Future f =
          executorService.submit(
              new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                  producer.close();
                  ProducerTest.assertProducerFinalState(producer);
                  return null;
                }
              });
      closeFutures.add(f);
    }
    for (Future<?> f : closeFutures) {
      try {
        f.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    executorService.shutdown();
  }

  private ProjectConfig buildProjectConfig() {
    return new ProjectConfig("project", "endpoint", "accessKeyId", "accessKeySecret");
  }
}
