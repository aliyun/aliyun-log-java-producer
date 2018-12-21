package com.aliyun.openservices.aliyun.log.producer;

public class ProjectConfig {

    private final String project;

    private final String endpoint;

    private final String accessKeyId;

    private final String accessKeySecret;

    private final String stsToken;

    public ProjectConfig(String project,
                         String endpoint,
                         String accessKeyId,
                         String accessKeySecret) {
        this(project, endpoint, accessKeyId, accessKeySecret, null);
    }

    public ProjectConfig(String project,
                         String endpoint,
                         String accessKeyId,
                         String accessKeySecret,
                         String stsToken) {
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
    }

    public String getProject() {
        return project;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getStsToken() {
        return stsToken;
    }

}

