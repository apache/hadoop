package org.apache.hadoop.hdfs.server.federation.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface AsyncWorkerMBean {
  long getEnTaskTotal();

  long getEnTaskPerSecond();

  long getOutTaskTotal();

  long getOutTaskPerSecond();

  double getTaskQueueAvg();
  double getTaskProcessAvg();
}
