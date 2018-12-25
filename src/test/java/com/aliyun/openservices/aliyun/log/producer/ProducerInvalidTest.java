package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ResultFailedException;
import com.aliyun.openservices.log.common.LogItem;
import com.google.common.math.LongMath;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ProducerInvalidTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSendWithNullProject() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("project cannot be null");
        producer.send(null, "logStore", new LogItem());
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithEmptyProject() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("project cannot be empty");
        producer.send("", "logStore", new LogItem());
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithNotExistProject() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        ListenableFuture<Result> f = producer.send("projectNotExist", "logStore", new LogItem(), null);
        try {
            f.get();
        } catch (ExecutionException e) {
            ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
            Result result = resultFailedException.getResult();
            Assert.assertEquals("projectNotExist", result.getProject());
            Assert.assertEquals("logStore", result.getLogStore());
            Assert.assertFalse(result.isSuccessful());
            Assert.assertEquals("ProjectConfigNotExist", result.getErrorCode());
            Assert.assertEquals("Cannot get the projectConfig for project projectNotExist", result.getErrorMessage());
        }
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithNullLogStore() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("logStore cannot be null");
        producer.send("project", null, new LogItem());
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithEmptyLogStore() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("logStore cannot be empty");
        producer.send("project", "", new LogItem());
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithNullLogItem() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        Producer producer = new LogProducer(producerConfig);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("logItem cannot be null");
        producer.send("project", "logStore", null);
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithRequestError() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        int retries = 5;
        producerConfig.setRetries(retries);
        producerConfig.setMaxReservedAttempts(retries + 1);
        Producer producer = new LogProducer(producerConfig);
        ListenableFuture<Result> f = producer.send("project", "logStore", ProducerTest.buildLogItem());
        try {
            f.get();
        } catch (ExecutionException e) {
            ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
            Result result = resultFailedException.getResult();
            Assert.assertEquals("project", result.getProject());
            Assert.assertEquals("logStore", result.getLogStore());
            Assert.assertFalse(result.isSuccessful());
            Assert.assertEquals("RequestError", result.getErrorCode());
            Assert.assertTrue(result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
            List<Attempt> attempts = result.getReservedAttempts();
            Assert.assertEquals(retries + 1, attempts.size());
            long t1;
            long t2 = -1;
            for (int i = 0; i < attempts.size(); ++i) {
                Attempt attempt = attempts.get(i);
                Assert.assertFalse(attempt.isSuccess());
                Assert.assertEquals("RequestError", attempt.getErrorCode());
                Assert.assertTrue(attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                Assert.assertEquals("", attempt.getRequestId());
                t1 = t2;
                t2 = attempt.getTimestampMs();
                if (i == 0)
                    continue;
                long diff = t2 - t1;
                long retryBackoffMs = producerConfig.getBaseRetryBackoffMs() * LongMath.pow(2, i - 1);
                long low = retryBackoffMs - (long) (producerConfig.getBaseRetryBackoffMs() * 0.1);
                long high = retryBackoffMs + (long) (producerConfig.getBaseRetryBackoffMs() * 0.2);
                if (i == 1)
                    Assert.assertTrue(low <= diff);
                else
                    Assert.assertTrue(low <= diff && diff <= high);
            }
        }
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithRequestError2() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        int retries = 5;
        int maxReservedAttempts = 2;
        producerConfig.setRetries(retries);
        producerConfig.setMaxReservedAttempts(maxReservedAttempts);
        Producer producer = new LogProducer(producerConfig);
        ListenableFuture<Result> f = producer.send("project", "logStore", ProducerTest.buildLogItem());
        try {
            f.get();
        } catch (ExecutionException e) {
            ResultFailedException resultFailedException = (ResultFailedException) e.getCause();
            Result result = resultFailedException.getResult();
            Assert.assertEquals("project", result.getProject());
            Assert.assertEquals("logStore", result.getLogStore());
            Assert.assertFalse(result.isSuccessful());
            Assert.assertEquals("RequestError", result.getErrorCode());
            Assert.assertTrue(result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
            List<Attempt> attempts = result.getReservedAttempts();
            Assert.assertEquals(maxReservedAttempts, attempts.size());
            Assert.assertEquals(retries + 1, result.getAttemptCount());
            for (Attempt attempt : attempts) {
                Assert.assertFalse(attempt.isSuccess());
                Assert.assertEquals("RequestError", attempt.getErrorCode());
                Assert.assertTrue(attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                Assert.assertEquals("", attempt.getRequestId());
            }
        }
        producer.close();
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testCloseMultiTimes() throws InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        producerConfig.setRetries(3);
        Producer producer = new LogProducer(producerConfig);
        int n = 1000000;
        int futureGetCount = 0;
        List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
        for (int i = 0; i < n; ++i) {
            ListenableFuture<Result> f = producer.send(
                    "project",
                    "logStore",
                    ProducerTest.buildLogItem());
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
                Assert.assertEquals("project", result.getProject());
                Assert.assertEquals("logStore", result.getLogStore());
                Assert.assertFalse(result.isSuccessful());
                Assert.assertEquals("RequestError", result.getErrorCode());
                Assert.assertTrue(result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                List<Attempt> attempts = result.getReservedAttempts();
                Assert.assertTrue(attempts.size() >= 1);
                for (Attempt attempt : attempts) {
                    Assert.assertFalse(attempt.isSuccess());
                    Assert.assertEquals("RequestError", attempt.getErrorCode());
                    Assert.assertTrue(attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                }
            }
        }
        Assert.assertEquals(n, futureGetCount);
        ProducerTest.assertProducerFinalState(producer);
    }

    @Test
    public void testCloseInMultiThreads() throws InterruptedException {
        final ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        producerConfig.setRetries(3);
        final Producer producer = new LogProducer(producerConfig);
        int n = 1000000;
        int futureGetCount = 0;
        List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
        for (int i = 0; i < n; ++i) {
            ListenableFuture<Result> f = producer.send(
                    "project",
                    "logStore",
                    ProducerTest.buildLogItem());
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
                Assert.assertEquals("project", result.getProject());
                Assert.assertEquals("logStore", result.getLogStore());
                Assert.assertFalse(result.isSuccessful());
                Assert.assertEquals("RequestError", result.getErrorCode());
                Assert.assertTrue(result.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                List<Attempt> attempts = result.getReservedAttempts();
                Assert.assertTrue(attempts.size() >= 1);
                for (Attempt attempt : attempts) {
                    Assert.assertFalse(attempt.isSuccess());
                    Assert.assertEquals("RequestError", attempt.getErrorCode());
                    Assert.assertTrue(attempt.getErrorMessage().startsWith("Web request failed: project.endpoint"));
                }
            }
        }
        Assert.assertEquals(n, futureGetCount);
    }

    private void closeInMultiThreads(final Producer producer, int nTasks) {
        ExecutorService executorService = Executors.newFixedThreadPool(nTasks);
        List<Future> closeFutures = new ArrayList<Future>();
        for (int i = 0; i < nTasks; ++i) {
            Future f = executorService.submit(new Callable<Object>() {
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

    private ProjectConfigs buildProjectConfigs() {
        ProjectConfigs projectConfigs = new ProjectConfigs();
        projectConfigs.put(
                new ProjectConfig(
                        "project",
                        "endpoint",
                        "accessKeyId",
                        "accessKeySecret")
        );
        return projectConfigs;
    }

}
