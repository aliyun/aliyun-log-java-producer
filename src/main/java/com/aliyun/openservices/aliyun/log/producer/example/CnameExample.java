package com.aliyun.openservices.aliyun.log.producer.example;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.Producer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CnameExample {
  public static void main(String[] args)
      throws ProducerException, InterruptedException, ExecutionException {

    String project = "your project";
    String logStore = "your logstore";
    String endpoint = "your endpoint";

    Producer producer = new LogProducer(new ProducerConfig());
    ProjectConfig projectConfig = new ProjectConfig(project, endpoint, getCredentialsProvider(), null);

    // set cname to true
    projectConfig.setCname(true);
    producer.putProjectConfig(projectConfig);

    producer.send(project, logStore, buildLogItem());
    producer.send(project, logStore, buildLogItem());
    Thread.sleep(10 * 1000);

    producer.close();
  }

  private static CredentialsProvider getCredentialsProvider() {
    String accessKeyId = System.getenv("ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ACCESS_KEY_SECRET");
    return new StaticCredentialsProvider(new DefaultCredentials(accessKeyId, accessKeySecret));
  }

  public static LogItem buildLogItem() {
    LogItem logItem = new LogItem();
    logItem.PushBack("k1", "v1");
    logItem.PushBack("k2", "v2");
    return logItem;
  }

  public static List<LogItem> buildLogItems(int n) {
    List<LogItem> logItems = new ArrayList<LogItem>();
    for (int i = 0; i < n; ++i) {
      logItems.add(buildLogItem());
    }
    return logItems;
  }
}
