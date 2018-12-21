package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.common.LogItem;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class ProducerTest {

    @Test
    public void testSend() throws InterruptedException, ExecutionException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        final Producer producer = new LogProducer(producerConfig);
        ListenableFuture<Result> f = producer.send(
                System.getenv("PROJECT"),
                System.getenv("LOG_STORE"),
                buildLogItem());
        Result result = f.get();
        Assert.assertTrue(result.isSuccessful());

        f = producer.send(
                System.getenv("PROJECT"),
                System.getenv("LOG_STORE"),
                null,
                null,
                buildLogItem());
        result = f.get();
        Assert.assertTrue(result.isSuccessful());

        f = producer.send(
                System.getenv("PROJECT"),
                System.getenv("LOG_STORE"),
                "",
                "",
                buildLogItem());
        result = f.get();
        Assert.assertTrue(result.isSuccessful());

        f = producer.send(
                System.getenv("PROJECT"),
                System.getenv("LOG_STORE"),
                "topic",
                "source",
                buildLogItem());
        result = f.get();
        Assert.assertTrue(result.isSuccessful());

        producer.close();
        assertProducerFinalState(producer);
    }

    @Test
    public void testSendWithCallback() throws InterruptedException, ExecutionException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        final Producer producer = new LogProducer(producerConfig);
        final AtomicInteger successCount = new AtomicInteger(0);
        ListenableFuture<Result> f = producer.send(
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

        f = producer.send(
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

        f = producer.send(
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

        f = producer.send(
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
        Assert.assertEquals(4, successCount.get());

        producer.close();
        assertProducerFinalState(producer);
    }

    @Test
    public void testClose() throws InterruptedException, ExecutionException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        final Producer producer = new LogProducer(producerConfig);
        final AtomicInteger successCount = new AtomicInteger(0);
        int futureGetCount = 0;
        int n = 100000;
        List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
        for (int i = 0; i < n; ++i) {
            ListenableFuture<Result> f = producer.send(
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
    public void testCloseInCallback() throws InterruptedException, ExecutionException {
        ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
        final Producer producer = new LogProducer(producerConfig);
        final AtomicInteger successCount = new AtomicInteger(0);
        int futureGetCount = 0;
        int n = 10000;
        List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
        for (int i = 0; i < n; ++i) {
            ListenableFuture<Result> f = producer.send(
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
                            } catch (InterruptedException e) {
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

    public static void assertProducerFinalState(Producer producer) {
        Assert.assertEquals(0, producer.getBatchCount());
        Assert.assertEquals(producer.getProducerConfig().getTotalSizeInBytes(), producer.availableMemoryInBytes());
    }

    public static LogItem buildLogItem() {
        LogItem logItem = new LogItem();
        logItem.PushBack("k1", "v1");
        logItem.PushBack("k2", "v2");
        return logItem;
    }

    private ProjectConfigs buildProjectConfigs() {
        ProjectConfigs projectConfigs = new ProjectConfigs();
        projectConfigs.put(buildProjectConfig());
        return projectConfigs;
    }

    private ProjectConfig buildProjectConfig() {
        String project = System.getenv("PROJECT");
        String endpoint = System.getenv("ENDPOINT");
        String accessKeyId = System.getenv("ACCESS_KEY_ID");
        String accessKeySecret = System.getenv("ACCESS_KEY_SECRET");
        return new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret);
    }

}
