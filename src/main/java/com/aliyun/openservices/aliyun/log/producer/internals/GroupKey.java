package com.aliyun.openservices.aliyun.log.producer.internals;

public final class GroupKey {

  private static final String DELIMITER = "|";

  private final String key;

  private final String project;

  private final String logStore;


  private final String shardHash;

  public GroupKey(String project, String logStore, String shardHash) {
    this.project = project;
    this.logStore = logStore;
    this.shardHash = shardHash;
    this.key =
        project + DELIMITER + logStore + DELIMITER 
            + shardHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GroupKey groupKey = (GroupKey) o;

    return key.equals(groupKey.key);
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return key;
  }

  public String getKey() {
    return key;
  }

  public String getProject() {
    return project;
  }

  public String getLogStore() {
    return logStore;
  }

  public String getShardHash() {
    return shardHash;
  }
}
