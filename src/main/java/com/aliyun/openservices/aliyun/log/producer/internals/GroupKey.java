package com.aliyun.openservices.aliyun.log.producer.internals;

public final class GroupKey {

  private static final String DELIMITER = "|";

  private final String key;

  private final String project;

  private final String logStore;

  private final String topic;

  private final String source;

  private final String shardHash;

  public GroupKey(String project, String logStore, String topic, String source, String shardHash) {
    this.project = project;
    this.logStore = logStore;
    this.topic = topic;
    this.source = source;
    this.shardHash = shardHash;
    this.key =
        project + DELIMITER + logStore + DELIMITER + topic + DELIMITER + source + DELIMITER
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

  public String getTopic() {
    return topic;
  }

  public String getSource() {
    return source;
  }

  public String getShardHash() {
    return shardHash;
  }
}
