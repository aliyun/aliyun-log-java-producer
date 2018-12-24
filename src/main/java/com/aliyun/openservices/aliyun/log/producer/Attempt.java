package com.aliyun.openservices.aliyun.log.producer;

public class Attempt {

    private final boolean success;

    private final String requestId;

    private final String errorCode;

    private final String errorMessage;

    private final long timestampMs;

    public Attempt(boolean success, String requestId, String errorCode, String errorMessage, long timestampMs) {
        this.success = success;
        this.requestId = requestId;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.timestampMs = timestampMs;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    @Override
    public String toString() {
        return "Attempt{" +
                "success=" + success +
                ", requestId='" + requestId + '\'' +
                ", errorCode='" + errorCode + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                ", timestampMs=" + timestampMs +
                '}';
    }
    
}
