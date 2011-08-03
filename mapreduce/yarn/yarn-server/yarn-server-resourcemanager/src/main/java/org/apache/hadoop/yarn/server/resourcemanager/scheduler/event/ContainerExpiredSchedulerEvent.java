package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;

/**
 * The {@link SchedulerEvent} which notifies that a {@link Container}
 * has expired, sent by {@link ContainerAllocationExpirer} 
 *
 */
public class ContainerExpiredSchedulerEvent extends SchedulerEvent {

  private final Container container;
  
  public ContainerExpiredSchedulerEvent(Container container) {
    super(SchedulerEventType.CONTAINER_EXPIRED);
    this.container = container;
  }

  public Container getContainer() {
    return container;
  }

}
