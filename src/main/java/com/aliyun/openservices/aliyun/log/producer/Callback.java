package com.aliyun.openservices.aliyun.log.producer;

/**
 * A callback interface that the user can implement to allow code to execute when the request is
 * complete. This callback will generally execute in the background batch handler thread and it
 * should be fast.
 */
public interface Callback {

  /**
   * A callback method the user can implement to provide asynchronous handling of request
   * completion. This method will be called when the log(s) sent to the server has been
   * acknowledged.
   *
   * @param result The result of a {@link LogProducer#send} operation.
   */
  void onCompletion(Result result);
}
