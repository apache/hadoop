package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class RMCommunicatorContainerDeAllocateRequestEvent extends
    RMCommunicatorEvent {

  private final ContainerId containerId;
  
  public RMCommunicatorContainerDeAllocateRequestEvent(ContainerId containerId) {
    super(RMCommunicatorEventType.CONTAINER_DEALLOCATE);
    this.containerId = containerId;
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }

}
