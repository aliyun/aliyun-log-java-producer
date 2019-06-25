package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProducerMultiProjectTest {

  private final Random random = new Random();

  private final List<String> shardHashList = new ArrayList<String>();

  @Before
  public void setUp() {
    shardHashList.add("5F");
    shardHashList.add("8C");
  }

  @Test
  public void testSend() throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig();
    final Producer producer = new LogProducer(producerConfig);
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    producer.putProjectConfig(
        new ProjectConfig(
            System.getenv("OTHER_PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    final AtomicInteger successCount = new AtomicInteger();
    int futureGetCount = 0;
    int n = 10000;
    List<ListenableFuture> futures = new ArrayList<ListenableFuture>();
    for (int i = 0; i < n; ++i) {
      ListenableFuture<Result> f =
          producer.send(
              System.getenv("PROJECT"),
              System.getenv("LOG_STORE"),
              "topic",
              null,
              getShardHash(),
              ProducerTest.buildLogItem(),
              new Callback() {
                @Override
                public void onCompletion(Result result) {
                  if (result.isSuccessful()) {
                    successCount.incrementAndGet();
                  }
                }
              });
      futures.add(f);
      f =
          producer.send(
              System.getenv("OTHER_PROJECT"),
              System.getenv("OTHER_LOG_STORE"),
              null,
              "source",
              getShardHash(),
              ProducerTest.buildLogItem(),
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
    for (ListenableFuture<?> f : futures) {
      Result result = (Result) f.get();
      Assert.assertTrue(result.isSuccessful());
      futureGetCount++;
    }
    producer.close();
    Assert.assertEquals(n * 2, successCount.get());
    Assert.assertEquals(n * 2, futureGetCount);
    ProducerTest.assertProducerFinalState(producer);
  }

  private String getShardHash() {
    return shardHashList.get(random.nextInt(shardHashList.size()));
  }
}
