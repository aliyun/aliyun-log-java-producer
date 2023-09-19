package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.common.auth.Credentials;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;

import javax.annotation.Nullable;

/**
 * Configuration for project. It contains the service entry information of the target project and
 * access credentials representing the identity of the caller.
 */
public class ProjectConfig {

    public static final String DEFAULT_USER_AGENT = "aliyun-log-java-producer";

    private final String project;

    private final String endpoint;

    private final String accessKeyId;

    private final String accessKeySecret;

    private final String stsToken;

    private final CredentialsProvider credentialsProvider;

    private final String userAgent;

    public ProjectConfig(
            String project, String endpoint, CredentialsProvider provider, @Nullable String userAgent) {
        if (project == null) {
            throw new NullPointerException("project cannot be null");
        }
        if (endpoint == null) {
            throw new NullPointerException("endpoint cannot be null");
        }
        if (provider == null) {
            throw new NullPointerException("credentialsProvider cannot be null");
        }
        this.project = project;
        this.endpoint = endpoint;
        this.credentialsProvider = provider;
        this.accessKeyId = null;
        this.accessKeySecret = null;
        this.stsToken = null;
        this.userAgent = userAgent;
    }

    public ProjectConfig(
            String project, String endpoint, String accessKeyId, String accessKeySecret) {
        this(project, endpoint, accessKeyId, accessKeySecret, null, DEFAULT_USER_AGENT);
    }

    public ProjectConfig(
            String project,
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String stsToken) {
        this(project, endpoint, accessKeyId, accessKeySecret, stsToken, DEFAULT_USER_AGENT);
    }

    public ProjectConfig(
            String project,
            String endpoint,
            String accessKeyId,
            String accessKeySecret,
            String stsToken,
            String userAgent) {
        if (project == null) {
            throw new NullPointerException("project cannot be null");
        }
        if (endpoint == null) {
            throw new NullPointerException("endpoint cannot be null");
        }
        if (accessKeyId == null) {
            throw new NullPointerException("accessKeyId cannot be null");
        }
        if (accessKeySecret == null) {
            throw new NullPointerException("accessKeySecret cannot be null");
        }
        this.project = project;
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.stsToken = stsToken;
        this.userAgent = userAgent;
        this.credentialsProvider = new StaticCredentialsProvider(new DefaultCredentials(accessKeyId, accessKeySecret, stsToken));
    }

    public String getProject() {
        return project;
    }

    public String getEndpoint() {
        return endpoint;
    }

    /**
     * use getCredentials instead
     */
    @Deprecated
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * use getCredentials instead
     */
    @Deprecated
    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    /**
     * use getCredentials instead
     */
    @Deprecated
    public String getStsToken() {
        return stsToken;
    }

    public Credentials getCredentials() {
        return credentialsProvider.getCredentials();
    }

    public CredentialsProvider getCredentialsProvider() {
      return credentialsProvider;
    }

    public String getUserAgent() {
        return userAgent;
    }
}
