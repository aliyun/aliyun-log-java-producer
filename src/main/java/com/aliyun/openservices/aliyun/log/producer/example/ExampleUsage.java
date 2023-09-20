package com.aliyun.openservices.aliyun.log.producer.example;

import com.aliyun.openservices.aliyun.log.producer.*;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.auth.CredentialsProvider;
import com.aliyun.openservices.log.common.auth.DefaultCredentials;
import com.aliyun.openservices.log.common.auth.StaticCredentialsProvider;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ExampleUsage {

    public static void main(String[] args) throws ProducerException, InterruptedException, ExecutionException {
        String project = System.getenv("PROJECT");
        String endpoint = System.getenv("ENDPOINT");
        String logStore = System.getenv("LOG_STORE");
        // init producer
        Producer producer = new LogProducer(new ProducerConfig());
        ProjectConfig projectConfig = new ProjectConfig(project, endpoint,
                getCredentialsProvider(),
                null);
        producer.putProjectConfig(projectConfig);

        // send logs
        ListenableFuture<Result> f =
                producer.send(project, logStore, buildLogItem());

        Result result = f.get();
        System.out.println(result.isSuccessful());
        System.out.println(result.getErrorMessage());

        producer.send(
                project, logStore, null, null, buildLogItem());
        producer.send(System.getenv("PROJECT"), System.getenv("LOG_STORE"), "", "", buildLogItem());

        f =
                producer.send(
                        project, logStore,
                        "topic",
                        "source",
                        buildLogItem());
        result = f.get();
        System.out.println(result.isSuccessful());
        System.out.println(result.getErrorMessage());

        producer.close();
    }

    // support customized credentials provider
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
