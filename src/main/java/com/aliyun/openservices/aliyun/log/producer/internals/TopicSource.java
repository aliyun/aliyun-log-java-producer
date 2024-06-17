package com.aliyun.openservices.aliyun.log.producer.internals;

import javax.annotation.Nullable;

public class TopicSource {
    private String topic = "";
    private String source = "";
    private static final String DELIMITER = "|";

    public TopicSource(@Nullable String topic, @Nullable String source) {
        if (topic != null) {
            this.topic = topic;
        }
        if (source != null) {
            this.source = source;
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getSource() {
        return source;
    }

    @Override
    public String toString() {
        return topic + DELIMITER + source;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof TopicSource) {
            TopicSource other = (TopicSource) obj;
            return other.topic.equals(topic) && other.source.equals(source);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return topic.hashCode() ^ source.hashCode();
    }
}
