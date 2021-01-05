package org.apache.hadoop.fs.azurebfs.utils;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public interface Listener {
  void verifyThreadCount(AtomicInteger numTasks, ThreadPoolExecutor executor,
      int maxThreads);

  void checkShutdown(int activeCount);

  void checkAllTasksComplete(AtomicInteger numTasks, int activeCount);

  boolean shouldInterrupt();

  void setInterrupted();

  boolean isInterrupted();

  void checkInterrupt();
}
