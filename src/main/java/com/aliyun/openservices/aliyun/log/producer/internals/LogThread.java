package com.aliyun.openservices.aliyun.log.producer.internals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogThread extends Thread {

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());

  public static LogThread daemon(final String name, Runnable runnable) {
    return new LogThread(name, runnable, true);
  }

  public static LogThread nonDaemon(final String name, Runnable runnable) {
    return new LogThread(name, runnable, false);
  }

  public LogThread(final String name, boolean daemon) {
    super(name);
    configureThread(name, daemon);
  }

  public LogThread(final String name, Runnable runnable, boolean daemon) {
    super(runnable, name);
    configureThread(name, daemon);
  }

  private void configureThread(final String name, boolean daemon) {
    setDaemon(daemon);
    setUncaughtExceptionHandler(
        new UncaughtExceptionHandler() {
          public void uncaughtException(Thread t, Throwable e) {
            LOGGER.error("Uncaught error in thread, name={}, e=", name, e);
          }
        });
  }
}
