# 异常诊断

如果您发现数据没有写入日志服务，可通过如下步骤诊断问题。
1. 检查您项目中引入的`aliyun-log-producer`、`aliyun-log`、`protobuf-java`这三个 jar 包的版本是否和文档中[安装](https://github.com/aliyun/aliyun-log-producer#%E5%AE%89%E8%A3%85)部分列出的 jar 包版本一致，如果不一致请进行升级。
2. Producer 接口的 send 方法异步发送数据，请通过 Callback 接口或返回的 Future 获取数据发送失败的原因。
3. 如果您发现并没有回调 Callback 接口的 onCompletion 方法，请检查在您的程序退出之前是否有调用 producer.close() 方法。因为数据发送是由后台线程异步完成的，为了防止缓存在内存里的少量数据丢失，请在程序退出之前务必调用 producer.close() 方法。
4. Producer 会把运行过程中的关键行为通过日志框架 slf4j 进行输出，您可以在程序中配置好相应的日志实现框架实现并打开 DEBUG 级别的日志。重点检查是否输出了 ERROR 级别的日志。
5. 如果通过上述步骤仍然没有解决您的问题请联系我们。
