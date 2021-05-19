package com.aliyun.openservices.aliyun.log.producer.errors;

/** The errors below are transient exceptions that if retried may succeed. */
public class RetriableErrors {

  public static final String REQUEST_ERROR = "RequestError";

  public static final String UNAUTHORIZED = "Unauthorized";

  public static final String WRITE_QUOTA_EXCEED = "WriteQuotaExceed";

  public static final String SHARD_WRITE_QUOTA_EXCEED = "ShardWriteQuotaExceed";

  public static final String EXCEED_QUOTA = "ExceedQuota";

  public static final String INTERNAL_SERVER_ERROR = "InternalServerError";

  public static final String SERVER_BUSY = "ServerBusy";

  public static final String BAD_RESPONSE = "BadResponse";

  public static final String PROJECT_NOT_EXISTS = "ProjectNotExists";

  public static final String LOGSTORE_NOT_EXISTS = "LogstoreNotExists";

  public static final String SOCKET_TIMEOUT = "SocketTimeout";

  public static final String SIGNATURE_NOT_MATCH = "SignatureNotMatch";
}
