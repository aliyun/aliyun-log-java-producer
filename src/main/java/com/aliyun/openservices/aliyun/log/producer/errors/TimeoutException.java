package com.aliyun.openservices.aliyun.log.producer.errors;

public class TimeoutException extends ProducerException {

  public TimeoutException() {
    super();
  }

  public TimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public TimeoutException(String message) {
    super(message);
  }

  public TimeoutException(Throwable cause) {
    super(cause);
  }
}
