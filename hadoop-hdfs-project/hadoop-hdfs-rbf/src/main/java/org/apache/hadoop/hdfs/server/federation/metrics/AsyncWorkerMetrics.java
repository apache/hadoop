package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.AsyncExecutor;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(name = "AsyncWorkerActivity", about = "Async Worker Activity",
    context = "dfs")
public class AsyncWorkerMetrics implements AsyncWorkerMBean{
  private final MetricsRegistry registry = new MetricsRegistry("AsyncWorkerActivity");
  private AsyncExecutor asyncExecutor;

  @Metric("task queue")
  private MutableRate taskQueue;
  @Metric("task process")
  private MutableRate taskProcess;
  @Metric("task en queue")
  private MutableCounterLong taskEnQueueOp;
  @Metric("task out queue")
  private MutableCounterLong taskOutQueueOp;

  public AsyncWorkerMetrics(String name, AsyncExecutor asyncExecutor) {
    registry.tag("worker", "worker name", name);
    this.asyncExecutor = asyncExecutor;
  }

  public static AsyncWorkerMetrics create(String name, AsyncExecutor asyncExecutor) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(name + "-Activity",
        "HDFS Federation AsyncWorker", new AsyncWorkerMetrics(name, asyncExecutor));
  }

  public void incrEnQueue() {
    taskEnQueueOp.incr();
  }

  public void incrOutQueue() {
    taskOutQueueOp.incr();
  }

  public void addQueueTime(long time) {
    taskQueue.add(time);
  }

  public void addtaskProcess(long time) {
    taskQueue.add(time);
  }

  public double getTaskQueueAvg() {
    return taskQueue.lastStat().mean();
  }

  public double getTaskProcessAvg() {
    return taskProcess.lastStat().mean();
  }

  @Override
  public long getEnTaskTotal() {
    return taskEnQueueOp.value();
  }

  @Override
  @Metric("en queue")
  public long getEnTaskPerSecond() {
    return asyncExecutor.getTaskEnqueuePerSecond();
  }

  @Override
  public long getOutTaskTotal() {
    return taskOutQueueOp.value();
  }

  @Override
  @Metric("out queue")
  public long getOutTaskPerSecond() {
    return asyncExecutor.getTaskOutqueuePerSecond();
  }
}
