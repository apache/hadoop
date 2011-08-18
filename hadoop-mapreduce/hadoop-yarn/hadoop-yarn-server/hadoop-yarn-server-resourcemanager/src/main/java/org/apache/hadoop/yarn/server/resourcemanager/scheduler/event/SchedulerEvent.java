package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class SchedulerEvent extends AbstractEvent<SchedulerEventType> {
  public SchedulerEvent(SchedulerEventType type) {
    super(type);
  }
}
