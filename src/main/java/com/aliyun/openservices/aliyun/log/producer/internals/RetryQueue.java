package com.aliyun.openservices.aliyun.log.producer.internals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryQueue {

    private static final int DEFAULT_INITIAL_CAPACITY = 11;

    private final PriorityBlockingQueue<ProducerBatch> retryBatches;

    private final long baseRetryBackoffMs;

    private final AtomicInteger putsInProgress;

    private volatile boolean closed;

    public RetryQueue(long baseRetryBackoffMs) {
        this.retryBatches = new PriorityBlockingQueue<ProducerBatch>(
                DEFAULT_INITIAL_CAPACITY,
                new Comparator<ProducerBatch>() {
                    @Override
                    public int compare(ProducerBatch p1, ProducerBatch p2) {
                        return (int) (p1.getNextRetryMs() - p2.getNextRetryMs());
                    }
                });
        this.baseRetryBackoffMs = baseRetryBackoffMs;
        this.putsInProgress = new AtomicInteger(0);
        this.closed = false;
    }

    public void put(ProducerBatch batch) {
        putsInProgress.incrementAndGet();
        try {
            if (closed)
                throw new IllegalStateException("cannot put after the retry queue was closed");
            retryBatches.put(batch);
        } finally {
            putsInProgress.decrementAndGet();
        }
    }

    public ExpiredBatches expiredBatches(long nowMs) {
        ExpiredBatches expiredBatches = new ExpiredBatches();
        long remainingMs = baseRetryBackoffMs;
        while (!retryBatches.isEmpty()) {
            ProducerBatch b = retryBatches.peek();
            if (b == null) {
                throw new IllegalStateException("got a null reference from retryBatches");
            }
            long curRemainingMs = b.retryRemainingMs(nowMs);
            if (curRemainingMs <= 0) {
                expiredBatches.add(b);
                retryBatches.remove(b);
            } else {
                remainingMs = Math.min(remainingMs, curRemainingMs);
                break;
            }
        }
        expiredBatches.setRemainingMs(remainingMs);
        return expiredBatches;
    }

    public List<ProducerBatch> remainingBatches() {
        List<ProducerBatch> batches = new ArrayList<ProducerBatch>();
        while (putsInProgress()) {
            retryBatches.drainTo(batches);
        }
        retryBatches.drainTo(batches);
        return batches;
    }

    public boolean isClosed() {
        return closed;
    }

    public void close() {
        this.closed = true;
    }

    private boolean putsInProgress() {
        return putsInProgress.get() > 0;
    }

}
