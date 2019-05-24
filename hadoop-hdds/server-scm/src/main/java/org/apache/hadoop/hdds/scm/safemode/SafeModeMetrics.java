package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

public class SafeModeMetrics {
  private static final String SOURCE_NAME =
      SafeModeMetrics.class.getSimpleName();


  // These all values will be set to some values when safemode is enabled.
  private @Metric MutableCounterLong
      numContainerWithOneReplicaReportedThreshold;
  private @Metric MutableCounterLong
      currentContainersWithOneReplicaReportedCount;

  // When hdds.scm.safemode.pipeline-availability.check is set then only
  // below metrics will have some values, otherwise they will be zero.
  private @Metric MutableCounterLong numHealthyPipelinesThreshold;
  private @Metric MutableCounterLong currentHealthyPipelinesCount;
  private @Metric MutableCounterLong
      numPipelinesWithAtleastOneReplicaReportedThreshold;
  private @Metric MutableCounterLong
      currentPipelinesWithAtleastOneReplicaReportedCount;

  public static SafeModeMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "SCM Safemode Metrics",
        new SafeModeMetrics());
  }

  public void setNumHealthyPipelinesThreshold(long val) {
    this.numHealthyPipelinesThreshold.incr(val);
  }

  public void incCurrentHealthyPipelinesCount() {
    this.currentHealthyPipelinesCount.incr();
  }

  public void setNumPipelinesWithAtleastOneReplicaReportedThreshold(long val) {
    this.numPipelinesWithAtleastOneReplicaReportedThreshold.incr(val);
  }

  public void incCurrentHealthyPipelinesWithAtleastOneReplicaReportedCount() {
    this.currentPipelinesWithAtleastOneReplicaReportedCount.incr();
  }

  public void setNumContainerWithOneReplicaReportedThreshold(long val) {
    this.numContainerWithOneReplicaReportedThreshold.incr(val);
  }

  public void incCurrentContainersWithOneReplicaReportedCount() {
    this.currentContainersWithOneReplicaReportedCount.incr();
  }

  public MutableCounterLong getNumHealthyPipelinesThreshold() {
    return numHealthyPipelinesThreshold;
  }

  public MutableCounterLong getCurrentHealthyPipelinesCount() {
    return currentHealthyPipelinesCount;
  }

  public MutableCounterLong
  getNumPipelinesWithAtleastOneReplicaReportedThreshold() {
    return numPipelinesWithAtleastOneReplicaReportedThreshold;
  }

  public MutableCounterLong getCurrentPipelinesWithAtleastOneReplicaCount() {
    return currentPipelinesWithAtleastOneReplicaReportedCount;
  }

  public MutableCounterLong getNumContainerWithOneReplicaReportedThreshold() {
    return numContainerWithOneReplicaReportedThreshold;
  }

  public MutableCounterLong getCurrentContainersWithOneReplicaReportedCount() {
    return currentContainersWithOneReplicaReportedCount;
  }


  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
