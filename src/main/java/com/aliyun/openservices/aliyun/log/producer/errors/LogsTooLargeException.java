package com.aliyun.openservices.aliyun.log.producer.errors;

/** The logs is larger than the maximum allowable size. */
public class LogsTooLargeException extends ProducerException {

  public LogsTooLargeException() {
    super();
  }

  public LogsTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogsTooLargeException(String message) {
    super(message);
  }

  public LogsTooLargeException(Throwable cause) {
    super(cause);
  }
}
