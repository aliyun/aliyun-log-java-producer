# Aliyun LOG Java Producer 新版本发布流程

## 前提条件
确保 [master](https://github.com/aliyun/aliyun-log-producer) 分支最新一次提交的单元测试全部通过([链接](https://travis-ci.org/aliyun/aliyun-log-producer))。

## 发布
1. 进入`aliyun-log-producer`项目的根目录。
2. 运行命令`make release`。
3. 根据提示输入新版本信息，等待命令执行完成。
4. 登陆 [stagingRepositories](https://oss.sonatype.org/#stagingRepositories)，close 提交的 repository。
5. 将 close 的 repository release。

## 验证
进入 [nexus-search](https://oss.sonatype.org/index.html#nexus-search;quick~aliyun-log-producer) 查看新版本是否成功发布。
