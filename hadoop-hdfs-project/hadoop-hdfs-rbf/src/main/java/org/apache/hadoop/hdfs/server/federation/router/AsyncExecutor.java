package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.hdfs.server.federation.metrics.AsyncWorkerMetrics;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncExecutor implements Executor {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncExecutor.class);
  private final ScheduledExecutorService scheduledExecutorService;
  private final int metricsUpdaterInterval;
  private final BlockingQueue<Task> taskQueue;
  private final Thread[] workers;
  private long taskEnqueuePerSecond = 0;
  private long taskOutqueuePerSecond = 0;
  private final AsyncWorkerMetrics asyncWorkerMetrics;

  public AsyncExecutor(int count, String name) {
    this.metricsUpdaterInterval = 1000;
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Hadoop-Metrics-Updater-" + name)
            .build());
    this.scheduledExecutorService.scheduleWithFixedDelay(new MetricsUpdateRunner(),
        metricsUpdaterInterval, metricsUpdaterInterval, TimeUnit.MILLISECONDS);
    this.taskQueue = new LinkedBlockingQueue<>();
    this.workers = new Thread[count];
    for (int i = 0; i< count; i++) {
      Worker worker = new Worker(name + "-" + i);
      workers[i] = worker;
      worker.start();
    }
    this.asyncWorkerMetrics = AsyncWorkerMetrics.create(name, this);
  }

  public void stop() {
    if (workers != null) {
      for(Thread worker : workers) {
        worker.interrupt();
      }
    }
  }

  @Override
  public void execute(Runnable command) {
    try {
      taskQueue.put(new Task(command));
      asyncWorkerMetrics.incrEnQueue();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }


  class Worker extends Thread {

    Worker(String name) {
      setDaemon(true);
      setName(name);
    }

    @Override
    public void run() {
      LOG.info("{} start!", Thread.currentThread().getName());
      while (!isInterrupted()) {
        Task task = null;
        try {
          task = taskQueue.take();
          asyncWorkerMetrics.incrOutQueue();
        } catch (InterruptedException e) {
          break;
        }
        long outqueueTime = Time.monotonicNow();
        asyncWorkerMetrics.addQueueTime(outqueueTime - task.getEnqueueTime());
        task.run();
        asyncWorkerMetrics.addtaskProcess(Time.monotonicNow() - outqueueTime);
      }
      LOG.info("{} stop!", Thread.currentThread().getName());
    }
  }

  class MetricsUpdateRunner implements Runnable {

    private long lastExecuted = 0;
    private long lastSeenEnqueue = 0;
    private long lastSeenOutqueue = 0;

    @Override
    public synchronized void run() {
      long currentTime = Time.monotonicNow();
      if (lastExecuted == 0) {
        lastExecuted = currentTime - metricsUpdaterInterval;
      }
      long currentEnqueue = asyncWorkerMetrics.getEnTaskTotal();
      long currentOutqueue = asyncWorkerMetrics.getOutTaskTotal();

      long totalEnqueueDiff = currentEnqueue - lastSeenEnqueue;
      long totalOutqueueDiff = currentOutqueue - lastSeenOutqueue;
      lastSeenEnqueue = currentEnqueue;
      lastSeenOutqueue = currentOutqueue;
      if ((currentTime - lastExecuted) > 0) {
        double totalEnqueuePerSecInDouble =
            (double) totalEnqueueDiff / TimeUnit.MILLISECONDS.toSeconds(
                currentTime - lastExecuted);
        double totalOutqueuePerSecInDouble =
            (double) totalOutqueueDiff / TimeUnit.MILLISECONDS.toSeconds(
                currentTime - lastExecuted);
        taskEnqueuePerSecond = (long) totalEnqueuePerSecInDouble;
        taskOutqueuePerSecond = (long) totalOutqueuePerSecInDouble;
      }
      lastExecuted = currentTime;
    }
  }

  public long getTaskOutqueuePerSecond() {
    return taskOutqueuePerSecond;
  }

  public long getTaskEnqueuePerSecond() {
    return taskEnqueuePerSecond;
  }

  static class Task implements Runnable {
    private final Runnable runnable;

    private final long enqueueTime;

    Task(Runnable runnable) {
      this.runnable = runnable;
      this.enqueueTime = Time.monotonicNow();
    }

    @Override
    public void run() {
      runnable.run();
    }

    public long getEnqueueTime() {
      return enqueueTime;
    }
  }
}
