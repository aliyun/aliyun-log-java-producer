package com.aliyun.openservices.aliyun.log.producer;

import java.util.List;

public class Result {

    private final String project;

    private final String logStore;

    private final boolean successful;

    private final String errorCode;

    private final String errorMessage;

    private final List<Attempt> attempts;

    public Result(String project,
                  String logStore,
                  boolean successful,
                  String errorCode,
                  String errorMessage,
                  List<Attempt> attempts) {
        this.project = project;
        this.logStore = logStore;
        this.successful = successful;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.attempts = attempts;
    }

    public String getProject() {
        return project;
    }

    public String getLogStore() {
        return logStore;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public List<Attempt> getAttempts() {
        return attempts;
    }

    @Override
    public String toString() {
        return "Result{" +
                "project='" + project + '\'' +
                ", logStore='" + logStore + '\'' +
                ", successful=" + successful +
                ", errorCode='" + errorCode + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                ", attempts=" + attempts +
                '}';
    }

}
