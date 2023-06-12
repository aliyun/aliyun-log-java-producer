package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.aliyun.log.producer.Attempt;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.errors.Errors;
import com.aliyun.openservices.aliyun.log.producer.errors.RetriableErrors;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.TagContent;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PutLogsRequest;
import com.aliyun.openservices.log.response.PutLogsResponse;
import com.google.common.math.LongMath;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendProducerBatchTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerBatch.class);

  private static final String TAG_PACK_ID = "__pack_id__";

  private final ProducerBatch batch;

  private final ProducerConfig producerConfig;

  private final Map<String, Client> clientPool;

  private final RetryQueue retryQueue;

  private final BlockingQueue<ProducerBatch> successQueue;

  private final BlockingQueue<ProducerBatch> failureQueue;

  private final AtomicInteger batchCount;

  public SendProducerBatchTask(
      ProducerBatch batch,
      ProducerConfig producerConfig,
      Map<String, Client> clientPool,
      RetryQueue retryQueue,
      BlockingQueue<ProducerBatch> successQueue,
      BlockingQueue<ProducerBatch> failureQueue,
      AtomicInteger batchCount) {
    this.batch = batch;
    this.producerConfig = producerConfig;
    this.clientPool = clientPool;
    this.retryQueue = retryQueue;
    this.successQueue = successQueue;
    this.failureQueue = failureQueue;
    this.batchCount = batchCount;
  }

  @Override
  public void run() {
    try {
      sendProducerBatch(System.currentTimeMillis());
    } catch (Throwable t) {
      LOGGER.error(
          "Uncaught error in send producer batch task, project="
              + batch.getProject()
              + ", logStore="
              + batch.getLogStore()
              + ", e=",
          t);
    }
  }

  private void sendProducerBatch(long nowMs) throws InterruptedException {
    LOGGER.trace("Prepare to send producer batch, batch={}", batch);
    String project = batch.getProject();
    Client client = getClient(project);
    if (client == null) {
      LOGGER.error("Failed to get client, project={}", project);
      Attempt attempt =
          new Attempt(
              false,
              "",
              Errors.PROJECT_CONFIG_NOT_EXIST,
              "Cannot get the projectConfig for project " + project,
              nowMs);
      batch.appendAttempt(attempt);
      failureQueue.put(batch);
    } else {
      PutLogsResponse response;
      try {
        PutLogsRequest request = buildPutLogsRequest(batch);
        response = client.PutLogs(request);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to put logs, project="
                + batch.getProject()
                + ", logStore="
                + batch.getLogStore()
                + ", e=",
            e);
        Attempt attempt = buildAttempt(e, nowMs);
        batch.appendAttempt(attempt);
        if (meetFailureCondition(e)) {
          LOGGER.debug("Prepare to put batch to the failure queue");
          failureQueue.put(batch);
        } else {
          LOGGER.debug("Prepare to put batch to the retry queue");
          long retryBackoffMs = calculateRetryBackoffMs();
          LOGGER.debug(
              "Calculate the retryBackoffMs successfully, retryBackoffMs=" + retryBackoffMs);
          batch.setNextRetryMs(System.currentTimeMillis() + retryBackoffMs);
          try {
            retryQueue.put(batch);
          } catch (IllegalStateException e1) {
            LOGGER.error(
                "Failed to put batch to the retry queue, project="
                    + batch.getProject()
                    + ", logStore="
                    + batch.getLogStore()
                    + ", e=",
                e);
            if (retryQueue.isClosed()) {
              LOGGER.info(
                  "Prepare to put batch to the failure queue since the retry queue was closed");
              failureQueue.put(batch);
            }
          }
        }
        return;
      }
      Attempt attempt = new Attempt(true, response.GetRequestId(), "", "", nowMs);
      batch.appendAttempt(attempt);
      successQueue.put(batch);
      LOGGER.trace("Send producer batch successfully, batch={}", batch);
    }
  }

  private Client getClient(String project) {
    return clientPool.get(project);
  }

  private PutLogsRequest buildPutLogsRequest(ProducerBatch batch) {
    PutLogsRequest request;
    if (batch.getShardHash() != null && !batch.getShardHash().isEmpty()) {
      request =
          new PutLogsRequest(
              batch.getProject(),
              batch.getLogStore(),
              batch.getTopic(),
              batch.getSource(),
              batch.getLogItems(),
              batch.getShardHash());
    } else {
      request =
          new PutLogsRequest(
              batch.getProject(),
              batch.getLogStore(),
              batch.getTopic(),
              batch.getSource(),
              batch.getLogItems());
    }
    List<TagContent> tags = new ArrayList<TagContent>();
    tags.add(new TagContent(TAG_PACK_ID, batch.getPackageId()));
    request.SetTags(tags);
    if (producerConfig.getLogFormat() == ProducerConfig.LogFormat.PROTOBUF) {
      request.setContentType(Consts.CONST_PROTO_BUF);
    } else {
      request.setContentType(Consts.CONST_SLS_JSON);
    }
    return request;
  }

  private Attempt buildAttempt(Exception e, long nowMs) {
    if (e instanceof LogException) {
      LogException logException = (LogException) e;
      return new Attempt(
          false,
          logException.GetRequestId(),
          logException.GetErrorCode(),
          logException.GetErrorMessage(),
          nowMs);
    } else {
      return new Attempt(false, "", Errors.PRODUCER_EXCEPTION, e.getMessage(), nowMs);
    }
  }

  private boolean meetFailureCondition(Exception e) {
    if (!isRetriableException(e)) {
      return true;
    }
    if (retryQueue.isClosed()) {
      return true;
    }
    return (batch.getRetries() >= producerConfig.getRetries()
        && failureQueue.size() <= batchCount.get() / 2);
  }

  private boolean isRetriableException(Exception e) {
    if (e instanceof LogException) {
      LogException logException = (LogException) e;
      return (logException.GetErrorCode().equals(RetriableErrors.REQUEST_ERROR)
          || logException.GetErrorCode().equals(RetriableErrors.UNAUTHORIZED)
          || logException.GetErrorCode().equals(RetriableErrors.WRITE_QUOTA_EXCEED)
          || logException.GetErrorCode().equals(RetriableErrors.SHARD_WRITE_QUOTA_EXCEED)
          || logException.GetErrorCode().equals(RetriableErrors.EXCEED_QUOTA)
          || logException.GetErrorCode().equals(RetriableErrors.INTERNAL_SERVER_ERROR)
          || logException.GetErrorCode().equals(RetriableErrors.SERVER_BUSY)
          || logException.GetErrorCode().equals(RetriableErrors.BAD_RESPONSE)
          || logException.GetErrorCode().equals(RetriableErrors.PROJECT_NOT_EXISTS)
          || logException.GetErrorCode().equals(RetriableErrors.LOGSTORE_NOT_EXISTS)
          || logException.GetErrorCode().equals(RetriableErrors.SOCKET_TIMEOUT)
          || logException.GetErrorCode().equals(RetriableErrors.SIGNATURE_NOT_MATCH));
    }
    return false;
  }

  private long calculateRetryBackoffMs() {
    int retry = batch.getRetries();
    long retryBackoffMs = producerConfig.getBaseRetryBackoffMs() * LongMath.pow(2, retry);
    if (retryBackoffMs <= 0) {
      retryBackoffMs = producerConfig.getMaxRetryBackoffMs();
    }
    return Math.min(retryBackoffMs, producerConfig.getMaxRetryBackoffMs());
  }
}
