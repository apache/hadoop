package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;

/**
 * The {@link SchedulerEvent} which notifies that a {@link ContainerId}
 * has expired, sent by {@link ContainerAllocationExpirer} 
 *
 */
public class ContainerExpiredSchedulerEvent extends SchedulerEvent {

  private final ContainerId containerId;
  
  public ContainerExpiredSchedulerEvent(ContainerId containerId) {
    super(SchedulerEventType.CONTAINER_EXPIRED);
    this.containerId = containerId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

}
