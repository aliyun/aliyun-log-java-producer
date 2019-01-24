package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;

public class ProducerMultiShardTest {

  @Test
  public void testSend() throws InterruptedException, ProducerException, ExecutionException {
    ProducerConfig producerConfig = new ProducerConfig(buildProjectConfigs());
    final Producer producer = new LogProducer(producerConfig);
    ListenableFuture<Result> f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "",
            "shard0_0",
            "&0000000000000000000000000000000",
            ProducerTest.buildLogItem());
    Result result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            null,
            "shard0_1",
            "00000000000000000000000000000000",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            null,
            "shard0_2",
            "39999999999999999999999999999999",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            null,
            "shard1_0",
            "40000000000000000000000000000000",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard1_1",
            "79999999999999999999999999999999",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard2_0",
            "80000000000000000000000000000000",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard2_1",
            "b9999999999999999999999999999999",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard3_0",
            "c0000000000000000000000000000000",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard3_1",
            "ffffffffffffffffffffffffffffffff",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard3_2",
            "fffffffffffffffffffffffffffffffg",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    f =
        producer.send(
            System.getenv("OTHER_PROJECT"),
            System.getenv("OTHER_LOG_STORE"),
            "topic",
            "shard3_3",
            "zzz",
            ProducerTest.buildLogItem());
    result = f.get();
    Assert.assertTrue(result.isSuccessful());

    producer.close();
    ProducerTest.assertProducerFinalState(producer);
  }

  private ProjectConfigs buildProjectConfigs() {
    ProjectConfigs projectConfigs = new ProjectConfigs();
    projectConfigs.put(
        new ProjectConfig(
            System.getenv("PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    projectConfigs.put(
        new ProjectConfig(
            System.getenv("OTHER_PROJECT"),
            System.getenv("ENDPOINT"),
            System.getenv("ACCESS_KEY_ID"),
            System.getenv("ACCESS_KEY_SECRET")));
    return projectConfigs;
  }
}
