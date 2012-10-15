package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerEventStopFailed extends AMContainerEvent {

  // TODO XXX Not being used for anything. May be useful if we rely less on
  // the RM informing the job about container failure.
  
  private final String message;

  public AMContainerEventStopFailed(ContainerId containerId, String message) {
    super(containerId, AMContainerEventType.C_NM_STOP_FAILED);
    this.message = message;
  }

  public String getMessage() {
    return this.message;
  }
}
