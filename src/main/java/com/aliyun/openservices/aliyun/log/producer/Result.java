package com.aliyun.openservices.aliyun.log.producer;

import com.google.common.collect.Iterables;
import java.util.List;

/**
 * The result of a {@link LogProducer#send} operation. A list of {@link Attempt}s is provided with
 * details about each attempt made.
 *
 * @see Attempt
 */
public class Result {

  private final boolean successful;

  private final List<Attempt> reservedAttempts;

  private final int attemptCount;

  public Result(boolean successful, List<Attempt> reservedAttempts, int attemptCount) {
    this.successful = successful;
    this.reservedAttempts = reservedAttempts;
    this.attemptCount = attemptCount;
  }

  /**
   * @return Whether the send operation was successful. If true, then the log(s) has been confirmed
   *     by the backend.
   */
  public boolean isSuccessful() {
    return successful;
  }

  /**
   * @return List of {@link Attempt}s, in the order they were made. If the attempts exceed {@link
   *     ProducerConfig#getMaxReservedAttempts()}, the oldest one will be removed.
   */
  public List<Attempt> getReservedAttempts() {
    return reservedAttempts;
  }

  /** @return Attempt count for the log(s) being sent. */
  public int getAttemptCount() {
    return attemptCount;
  }

  /** @return Error code of the last attempt. Empty string if no error (i.e. successful). */
  public String getErrorCode() {
    Attempt lastAttempt = Iterables.getLast(reservedAttempts);
    return lastAttempt.getErrorCode();
  }

  /** @return Error message of the last attempt. Empty string if no error (i.e. successful). */
  public String getErrorMessage() {
    Attempt lastAttempt = Iterables.getLast(reservedAttempts);
    return lastAttempt.getErrorMessage();
  }

  @Override
  public String toString() {
    return "Result{"
        + "successful="
        + successful
        + ", reservedAttempts="
        + reservedAttempts
        + ", attemptCount="
        + attemptCount
        + '}';
  }
}
