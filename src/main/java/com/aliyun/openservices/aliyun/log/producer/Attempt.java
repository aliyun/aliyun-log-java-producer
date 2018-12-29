package com.aliyun.openservices.aliyun.log.producer;

/**
 * Represents one attempt at writing a batch of logs to the backend. The attempt may or may not be
 * successful. If unsuccessful, an error code and error message are provided. Each batch may have
 * multiple attempts.
 *
 * @see Result
 */
public class Attempt {

  private final boolean success;

  private final String requestId;

  private final String errorCode;

  private final String errorMessage;

  private final long timestampMs;

  public Attempt(
      boolean success, String requestId, String errorCode, String errorMessage, long timestampMs) {
    this.success = success;
    this.requestId = requestId;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.timestampMs = timestampMs;
  }

  /**
   * @return Whether the attempt was successful. If true, then the batch has been confirmed by the
   *     backend.
   */
  public boolean isSuccess() {
    return success;
  }

  /**
   * @return Request id associated with this attempt. Empty string if the request did not reach the
   *     backend.
   */
  public String getRequestId() {
    return requestId;
  }

  /**
   * @return Error code associated with this attempt. Empty string if no error (i.e. successful).
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @return Error message associated with this attempt. Empty string if no error (i.e. successful).
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** @return The time when this attempt happened. */
  public long getTimestampMs() {
    return timestampMs;
  }

  @Override
  public String toString() {
    return "Attempt{"
        + "success="
        + success
        + ", requestId='"
        + requestId
        + '\''
        + ", errorCode='"
        + errorCode
        + '\''
        + ", errorMessage='"
        + errorMessage
        + '\''
        + ", timestampMs="
        + timestampMs
        + '}';
  }
}
