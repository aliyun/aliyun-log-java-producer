package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

  @Test
  public void testGenerateProducerHash() {
    String producerHash1 = Utils.generateProducerHash(3);
    String producerHash2 = Utils.generateProducerHash(4);
    Assert.assertTrue(!producerHash1.equals(producerHash2));
  }

  @Test
  public void testGeneratePackageId() {
    AtomicLong batchId = new AtomicLong(0);
    String producerHash = Utils.generateProducerHash(3);
    for (int i = 0; i < 100; ++i) {
      String packageId = Utils.generatePackageId(producerHash, batchId);
      String[] pieces = packageId.split("-", 2);
      Assert.assertEquals(producerHash, pieces[0]);
      Assert.assertEquals(Long.toHexString(i), pieces[1]);
    }
  }
}
