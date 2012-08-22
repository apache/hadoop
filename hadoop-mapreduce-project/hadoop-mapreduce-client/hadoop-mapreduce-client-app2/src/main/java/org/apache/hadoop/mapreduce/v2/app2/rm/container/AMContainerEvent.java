package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

// TODO: Implement.

public class AMContainerEvent extends AbstractEvent<AMContainerEventType> {


  private final ContainerId containerId;
  
  public AMContainerEvent(ContainerId containerId, AMContainerEventType type) {
    super(type);
    this.containerId = containerId;
  }
  
  public ContainerId getContainerId() {
    return this.containerId;
  }
}
