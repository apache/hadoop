package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ContainerStopMonitoringEvent extends ContainersMonitorEvent {

  public ContainerStopMonitoringEvent(ContainerId containerId) {
    super(containerId, ContainersMonitorEventType.STOP_MONITORING_CONTAINER);
  }

}
