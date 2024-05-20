package com.aliyun.openservices.aliyun.log.producer.internals;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import java.util.List;

public abstract class LogSizeCalculator {

  public static int calculate(LogItem logItem) {
    int sizeInBytes = 8;
    for (LogContent content : logItem.GetLogContents()) {
      sizeInBytes += 8;
      if (content.mKey != null) {
        sizeInBytes += content.mKey.length();
      }
      if (content.mValue != null) {
        sizeInBytes += content.mValue.length();
      }
    }
    return sizeInBytes;
  }

  public static int calculate(List<LogItem> logItems) {
    int sizeInBytes = 0;
    for (LogItem logItem : logItems) {
      sizeInBytes += calculate(logItem);
    }
    return sizeInBytes;
  }
}
