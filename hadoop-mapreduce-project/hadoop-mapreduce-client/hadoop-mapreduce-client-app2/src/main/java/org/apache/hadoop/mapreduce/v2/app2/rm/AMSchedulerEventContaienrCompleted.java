package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMSchedulerEventContaienrCompleted extends AMSchedulerEvent {

  private final ContainerId containerId;
  
  public AMSchedulerEventContaienrCompleted(ContainerId containerId) {
    super(AMSchedulerEventType.S_CONTAINER_COMPLETED);;
    this.containerId = containerId;
    // TODO Auto-generated constructor stub
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }

}
