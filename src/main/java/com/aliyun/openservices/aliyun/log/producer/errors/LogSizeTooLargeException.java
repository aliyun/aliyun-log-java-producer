package com.aliyun.openservices.aliyun.log.producer.errors;

/** The log's size is larger than the maximum allowable size. */
public class LogSizeTooLargeException extends ProducerException {

  public LogSizeTooLargeException() {
    super();
  }

  public LogSizeTooLargeException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogSizeTooLargeException(String message) {
    super(message);
  }

  public LogSizeTooLargeException(Throwable cause) {
    super(cause);
  }
}
