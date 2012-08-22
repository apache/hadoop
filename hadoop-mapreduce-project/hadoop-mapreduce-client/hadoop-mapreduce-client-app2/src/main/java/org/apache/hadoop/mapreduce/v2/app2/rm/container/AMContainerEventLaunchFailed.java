package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerEventLaunchFailed extends AMContainerEvent {

  private final String message;
  
  public AMContainerEventLaunchFailed(ContainerId containerId,
      String message) {
    super(containerId, AMContainerEventType.C_LAUNCH_FAILED);
    this.message = message;
  }
  
  public String getMessage() {
    return this.message;
  }

}
