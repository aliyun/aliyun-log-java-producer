package com.aliyun.openservices.aliyun.log.producer.errors;

/** Send logs after producer is closed. */
public class SendAfterCloseException extends ProducerException {
  public SendAfterCloseException() {
    super();
  }

  public SendAfterCloseException(String message, Throwable cause) {
    super(message, cause);
  }

  public SendAfterCloseException(String message) {
    super(message);
  }

  public SendAfterCloseException(Throwable cause) {
    super(cause);
  }
}
