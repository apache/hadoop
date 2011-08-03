package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.Container;

public class ContainerFinishedSchedulerEvent extends SchedulerEvent {

  private final Container container;

  public ContainerFinishedSchedulerEvent(Container container) {
    super(SchedulerEventType.CONTAINER_FINISHED);
    this.container = container;
  }

  public Container getContainer() {
    return container;
  }

}
