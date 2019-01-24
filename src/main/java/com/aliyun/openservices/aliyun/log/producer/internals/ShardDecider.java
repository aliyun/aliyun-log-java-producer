package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ShardDecider {

  private final ProducerConfig producerConfig;

  private final ConcurrentHashMap<String, ShardInfo> logStoreShardInfo =
      new ConcurrentHashMap<String, ShardInfo>();

  public ShardDecider(ProducerConfig producerConfig) {
    this.producerConfig = producerConfig;
  }

  public String getHashKey(String project, String logStore, String shardHash) {
    Client client = producerConfig.getProjectConfigs().getClient(project);
    if (client == null) {
      throw new IllegalStateException("cannot get client for project " + project);
    }
    ShardInfo shardInfo = getOrCreateShardInfo(project, logStore);
    List<String> beginKeys = shardInfo.beginKeys;
    return getHashKey(beginKeys, shardHash);
  }

  public void updateLogStoreShardInfo() {
    long nowMs = System.currentTimeMillis();
    for (Entry<String, ShardInfo> entry : logStoreShardInfo.entrySet()) {
      String key = entry.getKey();
      ShardInfo shardInfo = entry.getValue();
      if (nowMs - shardInfo.createdMs > producerConfig.getShardHashUpdateIntervalMS()) {
        String[] result = key.split("\\|", 2);
        String project = result[0];
        String logStore = result[1];
        shardInfo = createShardInfo(project, logStore);
        logStoreShardInfo.put(key, shardInfo);
      }
    }
  }

  private ShardInfo getOrCreateShardInfo(String project, String logStore) {
    String key = project + "|" + logStore;
    ShardInfo shardInfo = logStoreShardInfo.get(key);
    if (shardInfo != null) {
      return shardInfo;
    }
    shardInfo = createShardInfo(project, logStore);
    ShardInfo previous = logStoreShardInfo.putIfAbsent(key, shardInfo);
    if (previous == null) {
      return shardInfo;
    } else {
      return previous;
    }
  }

  private ShardInfo createShardInfo(String project, String logStore) {
    Client client = producerConfig.getProjectConfigs().getClient(project);
    if (client == null) {
      throw new IllegalStateException("cannot get client for project " + project);
    }
    List<Shard> shards;
    try {
      shards = client.ListShard(project, logStore).GetShards();
    } catch (LogException e) {
      throw new IllegalStateException(
          "cannot get shards for project " + project + " logStore " + logStore);
    }
    List<String> beginKeys = new ArrayList<String>();
    for (Shard shard : shards) {
      if (shard.getStatus().compareToIgnoreCase("readonly") != 0) {
        beginKeys.add(shard.getInclusiveBeginKey());
      }
    }
    Collections.sort(beginKeys);
    return new ShardInfo(beginKeys, System.currentTimeMillis());
  }

  private String getHashKey(List<String> beginKeys, String shardHash) {
    int low = 0, high = beginKeys.size() - 1, mid = 0;
    int action = 0;
    while (low <= high) {
      mid = (low + high) / 2;
      int cmp = beginKeys.get(mid).compareTo(shardHash);
      if (cmp == 0) {
        action = 0;
        break;
      } else if (cmp < 0) {
        action = 1;
        low = mid + 1;
      } else {
        action = -1;
        high = mid - 1;
      }
    }
    if (action == -1 && mid > 0) {
      --mid;
    }
    return beginKeys.get(mid);
  }

  private static final class ShardInfo {

    List<String> beginKeys;

    long createdMs;

    ShardInfo(List<String> beginKeys, long nowMs) {
      this.beginKeys = beginKeys;
      this.createdMs = nowMs;
    }
  }
}
