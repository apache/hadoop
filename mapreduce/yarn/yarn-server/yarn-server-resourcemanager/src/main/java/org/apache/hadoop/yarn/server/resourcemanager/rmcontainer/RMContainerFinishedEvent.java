package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

public class RMContainerFinishedEvent extends RMContainerEvent {

  private final ContainerStatus remoteContainerStatus;

  public RMContainerFinishedEvent(ContainerId containerId,
      ContainerStatus containerStatus) {
    super(containerId, RMContainerEventType.FINISHED);
    this.remoteContainerStatus = containerStatus;
  }

  public ContainerStatus getRemoteContainerStatus() {
    return this.remoteContainerStatus;
  }
}
