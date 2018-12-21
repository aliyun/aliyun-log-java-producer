package com.aliyun.openservices.aliyun.log.producer.errors;

public class ProducerException extends RuntimeException {

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
