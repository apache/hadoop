package org.apache.hadoop.fs.azurebfs.utils;

import org.assertj.core.api.Assertions;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorServiceTestUtils implements Listener {
  boolean interrupt;
  boolean hasInterrupted = false;
  AtomicInteger numInterrupts = new AtomicInteger(0);
  public ExecutorServiceTestUtils(boolean testWithInterrupt) {
    interrupt = testWithInterrupt;
  }
  @Override
  public void verifyThreadCount(AtomicInteger numTasks, ThreadPoolExecutor executor,
      int maxThreads) {
    int activeThreads = executor.getActiveCount();
    int taskQueueSize = executor.getQueue().size();
    Assertions.assertThat(activeThreads).isLessThanOrEqualTo(maxThreads);
    Assertions.assertThat(activeThreads).isGreaterThanOrEqualTo(Math.min(numTasks.get() - 2, maxThreads))
        .isLessThanOrEqualTo(numTasks.get() + 2);
    if (taskQueueSize > maxThreads) {
      Assertions.assertThat(activeThreads)
          .describedAs("all threads should be active")
          .isEqualTo(maxThreads);
    }
  }

  @Override
  public void checkShutdown(int activeCount) {
    Assertions.assertThat(activeCount)
        .describedAs("All tasks should be halted").isEqualTo(0);
  }

  @Override
  public void checkAllTasksComplete(AtomicInteger numTasks, int activeCount) {
    Assertions.assertThat(numTasks.get())
        .describedAs("Number of tasks should be 0")
        .isEqualTo(0);
    Assertions.assertThat(activeCount)
        .describedAs("All threads should be halted").isEqualTo(0);
  }

  @Override
  public boolean shouldInterrupt() {
    return interrupt;
  }

  @Override
  public void setInterrupted() {
    hasInterrupted = true;
  }

  @Override
  public boolean isInterrupted() {
    return hasInterrupted;
  }

  @Override
  public void checkInterrupt() {
    Assertions.assertThat(isInterrupted())
        .describedAs("Thread should have been interrupted")
        .isEqualTo(false);
  }
}
