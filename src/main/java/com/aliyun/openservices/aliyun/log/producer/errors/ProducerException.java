package com.aliyun.openservices.aliyun.log.producer.errors;

/** The base class of all other producer exceptions. */
public class ProducerException extends Exception {

  public ProducerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProducerException(String message) {
    super(message);
  }

  public ProducerException(Throwable cause) {
    super(cause);
  }

  public ProducerException() {
    super();
  }
}
