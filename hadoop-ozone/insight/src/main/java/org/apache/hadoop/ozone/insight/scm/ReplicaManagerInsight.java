package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;

/**
 * Insight definition to chech the replication manager internal state.
 */
public class ReplicaManagerInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(new LoggerSource(Type.SCM, ReplicationManager.class,
        defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> display = new ArrayList<>();
    return display;
  }

  @Override
  public List<Class> getConfigurationClasses() {
    List<Class> result = new ArrayList<>();
    result.add(ReplicationManager.ReplicationManagerConfiguration.class);
    return result;
  }

  @Override
  public String getDescription() {
    return "SCM closed container replication manager";
  }

}
