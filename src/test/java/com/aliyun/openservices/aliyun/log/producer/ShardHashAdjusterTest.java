package com.aliyun.openservices.aliyun.log.producer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShardHashAdjusterTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAdjust() {
    ShardHashAdjuster adjuster = new ShardHashAdjuster(1);
    String adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("00000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("00000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(2);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("80000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("00000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(4);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("c0000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("40000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(8);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("e0000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("60000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(16);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("f0000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("60000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(32);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("f0000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("68000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(64);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("f4000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("6c000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(128);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("f4000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("6e000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(256);
    adjustedShardHash = adjuster.adjust("127.0.0.1");
    Assert.assertEquals("f5000000000000000000000000000000", adjustedShardHash);
    adjustedShardHash = adjuster.adjust("192.168.0.2");
    Assert.assertEquals("6f000000000000000000000000000000", adjustedShardHash);
  }

  @Test
  public void testAdjustEmptyStr() {
    ShardHashAdjuster adjuster = new ShardHashAdjuster(1);
    String adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("00000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(2);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("80000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(4);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("c0000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(8);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("c0000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(16);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("d0000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(32);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("d0000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(64);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("d4000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(128);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("d4000000000000000000000000000000", adjustedShardHash);

    adjuster = new ShardHashAdjuster(256);
    adjustedShardHash = adjuster.adjust("");
    Assert.assertEquals("d4000000000000000000000000000000", adjustedShardHash);
  }

  @Test
  public void testConstructInvalidShardHashAdjusterWith9() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("buckets must be a power of 2, got 9");
    new ShardHashAdjuster(9);
  }

  @Test
  public void testConstructInvalidShardHashAdjusterWith62() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("buckets must be a power of 2, got 63");
    new ShardHashAdjuster(63);
  }
}
