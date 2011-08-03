package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;

public class ContainerFinishedSchedulerEvent extends SchedulerEvent {

  private final Container container;
  private final RMContainerEventType cause;

  public ContainerFinishedSchedulerEvent(Container container, RMContainerEventType cause) {
    super(SchedulerEventType.CONTAINER_FINISHED);
    this.container = container;
    this.cause = cause;
  }

  public Container getContainer() {
    return container;
  }

  public RMContainerEventType getCause() {
    return cause;
  }

}
