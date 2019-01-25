package com.aliyun.openservices.aliyun.log.producer;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.math.BigInteger;

public class ShardHashAdjuster {

  private static final int HEX_LENGTH = 32;

  private static final int BINARY_LENGTH = 128;

  private int reservedBits;

  public ShardHashAdjuster(int buckets) {
    if (!isPowerOfTwo(buckets)) {
      throw new IllegalArgumentException("buckets must be a power of 2, got " + buckets);
    }
    reservedBits = Integer.bitCount(buckets - 1);
  }

  public String adjust(String shardHash) {
    HashCode hashCode = Hashing.md5().hashBytes(shardHash.getBytes());
    String binary =
        Strings.padStart(new BigInteger(1, hashCode.asBytes()).toString(2), BINARY_LENGTH, '0');
    String adjustedBinary = Strings.padEnd(binary.substring(0, reservedBits), BINARY_LENGTH, '0');
    return Strings.padStart(new BigInteger(adjustedBinary, 2).toString(16), HEX_LENGTH, '0');
  }

  public static boolean isPowerOfTwo(int number) {
    return number > 0 && ((number & (number - 1)) == 0);
  }
}
