package com.aliyun.openservices.aliyun.log.producer.errors;

/** Retries for the errors below are useless. */
public class Errors {

  public static final String PROJECT_CONFIG_NOT_EXIST = "ProjectConfigNotExist";

  public static final String PROJECT_NOT_EXIST = "ProjectNotExist";

  public static final String SIGNATURE_NOT_MATCH = "SignatureNotMatch";

  public static final String MISS_ACCESS_KEY_ID = "MissAccessKeyId";

  public static final String REQUEST_TIME_TOO_SKEWED = "RequestTimeTooSkewed";

  public static final String PRODUCER_EXCEPTION = "ProducerException";
}
