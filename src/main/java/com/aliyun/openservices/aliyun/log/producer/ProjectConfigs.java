package com.aliyun.openservices.aliyun.log.producer;

import com.aliyun.openservices.log.Client;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A collections of {@link ProjectConfig}s. All public methods are thread-safe. */
public class ProjectConfigs {

  private final Map<String, Client> clientPool = new ConcurrentHashMap<String, Client>();

  public void put(ProjectConfig projectConfig) {
    Client client = buildClient(projectConfig);
    clientPool.put(projectConfig.getProject(), client);
  }

  public void remove(ProjectConfig projectConfig) {
    clientPool.remove(projectConfig.getProject());
  }

  public void clear() {
    clientPool.clear();
  }

  public Client getClient(String project) {
    return clientPool.get(project);
  }

  private Client buildClient(ProjectConfig projectConfig) {
    Client client =
        new Client(
            projectConfig.getEndpoint(),
            projectConfig.getAccessKeyId(),
            projectConfig.getAccessKeySecret());
    String userAgent = projectConfig.getUserAgent();
    if (userAgent != null) {
      client.setUserAgent(userAgent);
    }
    String stsToken = projectConfig.getStsToken();
    if (stsToken != null) {
      client.SetSecurityToken(stsToken);
    }
    return client;
  }
}
