package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ContainerStartMonitoringEvent extends ContainersMonitorEvent {

  private final long vmemLimit;
  private final long pmemLimit;

  public ContainerStartMonitoringEvent(ContainerId containerId,
      long vmemLimit, long pmemLimit) {
    super(containerId, ContainersMonitorEventType.START_MONITORING_CONTAINER);
    this.vmemLimit = vmemLimit;
    this.pmemLimit = pmemLimit;
  }

  public long getVmemLimit() {
    return this.vmemLimit;
  }

  public long getPmemLimit() {
    return this.pmemLimit;
  }

}
