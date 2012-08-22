package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerStatus;

public class AMContainerEventReleased extends AMContainerEvent {

  private final ContainerStatus containerStatus;

  public AMContainerEventReleased(ContainerStatus containerStatus) {
    super(containerStatus.getContainerId(), AMContainerEventType.C_COMPLETED);
    this.containerStatus = containerStatus;
  }

  public ContainerStatus getContainerStatus() {
    return this.containerStatus;
  }

}
