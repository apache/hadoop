package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMContainerEvent extends AbstractEvent<RMContainerEventType> {

  private final ContainerId containerId;

  public RMContainerEvent(ContainerId containerId, RMContainerEventType type) {
    super(type);
    this.containerId = containerId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }
}
