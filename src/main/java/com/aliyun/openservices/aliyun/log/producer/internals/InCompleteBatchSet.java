package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class InCompleteBatchSet {
    private final ConcurrentHashMap<ProducerBatch, Boolean> inCompleteBatches;
    public InCompleteBatchSet() {
        this.inCompleteBatches = new ConcurrentHashMap<ProducerBatch, Boolean>();
    }
    public void add(ProducerBatch producerBatch) {
        this.inCompleteBatches.put(producerBatch, Boolean.TRUE);
    }

    public void remove(ProducerBatch producerBatch) {
        this.inCompleteBatches.remove(producerBatch);
    }

    public Set<ProducerBatch> all() {
        return this.inCompleteBatches.keySet();
    }
}
