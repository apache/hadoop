package org.apache.hadoop.hdds.scm.container.metrics;


import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Class contains metrics related to ContainerManager.
 */
@Metrics(about = "SCM ContainerManager metrics", context = "ozone")
public final class SCMContainerManagerMetrics {

  private static final String SOURCE_NAME =
      SCMContainerManagerMetrics.class.getSimpleName();

  // These are the metrics which will be reset to zero after restart.
  // These metrics capture count of number of successful/failure operations
  // of create/delete containers in SCM.

  private @Metric MutableCounterLong numSuccessfulCreateContainers;
  private @Metric MutableCounterLong numFailureCreateContainers;
  private @Metric MutableCounterLong numSuccessfulDeleteContainers;
  private @Metric MutableCounterLong numFailureDeleteContainers;
  private @Metric MutableCounterLong numListContainerOps;


  private @Metric MutableCounterLong numContainerReportsProcessedSuccessful;
  private @Metric MutableCounterLong numContainerReportsProcessedFailed;
  private @Metric MutableCounterLong numICRReportsProcessedSuccessful;
  private @Metric MutableCounterLong numICRReportsProcessedFailed;

  private SCMContainerManagerMetrics() {
  }

  /**
   * Create and return metrics instance.
   * @return SCMContainerManagerMetrics
   */
  public static SCMContainerManagerMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "SCM ContainerManager Metrics",
        new SCMContainerManagerMetrics());
  }

  /**
   * Unregister metrics.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incNumSuccessfulCreateContainers() {
    this.numSuccessfulCreateContainers.incr();
  }

  public void incNumFailureCreateContainers() {
    this.numFailureCreateContainers.incr();
  }

  public void incNumSuccessfulDeleteContainers() {
    this.numSuccessfulDeleteContainers.incr();
  }

  public void incNumFailureDeleteContainers() {
    this.numFailureDeleteContainers.incr();
  }

  public void incNumListContainersOps() {
    this.numListContainerOps.incr();
  }

  public void incNumContainerReportsProcessedSuccessful() {
    this.numContainerReportsProcessedSuccessful.incr();
  }

  public void incNumContainerReportsProcessedFailed() {
    this.numContainerReportsProcessedFailed.incr();
  }

  public void incNumICRReportsProcessedSuccessful() {
    this.numICRReportsProcessedSuccessful.incr();
  }

  public void incNumICRReportsProcessedFailed() {
    this.numICRReportsProcessedFailed.incr();
  }

  public long getNumContainerReportsProcessedSuccessful() {
    return numContainerReportsProcessedSuccessful.value();
  }

  public long getNumContainerReportsProcessedFailed() {
    return numContainerReportsProcessedFailed.value();
  }

  public long getNumICRReportsProcessedSuccessful() {
    return numICRReportsProcessedSuccessful.value();
  }

  public long getNumICRReportsProcessedFailed() {
    return numICRReportsProcessedFailed.value();
  }

  public long getNumSuccessfulCreateContainers() {
    return numSuccessfulCreateContainers.value();
  }

  public long getNumFailureCreateContainers() {
    return numFailureCreateContainers.value();
  }

  public long getNumSuccessfulDeleteContainers() {
    return numSuccessfulDeleteContainers.value();
  }

  public long getNumFailureDeleteContainers() {
    return numFailureDeleteContainers.value();
  }

  public long getNumListContainersOps() {
    return numListContainerOps.value();
  }

}
