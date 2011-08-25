package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

public class ContainerFailedEvent extends ContainerAllocatorEvent {

  private final String contMgrAddress;
  
  public ContainerFailedEvent(TaskAttemptId attemptID, String contMgrAddr) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_FAILED);
    this.contMgrAddress = contMgrAddr;
  }

  public String getContMgrAddress() {
    return contMgrAddress;
  }

}
