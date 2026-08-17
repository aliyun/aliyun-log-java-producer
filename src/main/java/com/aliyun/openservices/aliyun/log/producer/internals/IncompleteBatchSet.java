package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IncompleteBatchSet {
    private final ConcurrentHashMap<ProducerBatch, Boolean> incompleteBatches;

    public IncompleteBatchSet() {
        this.incompleteBatches = new ConcurrentHashMap<ProducerBatch, Boolean>();
    }

    public void add(ProducerBatch producerBatch) {
        this.incompleteBatches.put(producerBatch, Boolean.TRUE);
    }

    public void remove(ProducerBatch producerBatch) {
        this.incompleteBatches.remove(producerBatch);
    }

    public Set<ProducerBatch> all() {
        return this.incompleteBatches.keySet();
    }
}
