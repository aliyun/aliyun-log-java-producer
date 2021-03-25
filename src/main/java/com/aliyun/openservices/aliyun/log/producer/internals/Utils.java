package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.log.util.NetworkUtils;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

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
      LOGGER.warn("Failed to get local machine ip, set ip to 127.0.0.1");
      ip = "127.0.0.1";
    }
    String name = ManagementFactory.getRuntimeMXBean().getName();
    String input = ip + "-" + name + "-" + instanceId;
    return Hashing.farmHashFingerprint64().hashString(input, Charsets.US_ASCII).toString();
  }

  public static String generatePackageId(String producerHash, AtomicLong batchId) {
    return producerHash + "-" + Long.toHexString(batchId.getAndIncrement());
  }
}
