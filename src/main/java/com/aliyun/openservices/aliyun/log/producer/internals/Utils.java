package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.log.util.NetworkUtils;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Utils {

  public static void assertArgumentNotNull(Object argument, String argumentName) {
    if (argument == null) {
      throw new IllegalArgumentException(argumentName + " cannot be null");
    }
  }

  public static void assertArgumentNotNullOrEmpty(String argument, String argumentName) {
    assertArgumentNotNull(argument, argumentName);
    if (argument.isEmpty()) {
      throw new IllegalArgumentException(argumentName + " cannot be empty");
    }
  }

  public static String generateProducerHash(int instanceId) {
    String ip = NetworkUtils.getLocalMachineIP();
    if (ip == null) {
      throw new IllegalStateException("failed to get local machine ip");
    }
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String input = ip + "-" + name + "-" + instanceId;
    return Hashing.farmHashFingerprint64().hashString(input, Charsets.US_ASCII).toString();
  }

  public static String generatePackageId(String producerHash, AtomicLong batchId) {
    return producerHash + "-" + Long.toHexString(batchId.getAndIncrement());
  }

}
