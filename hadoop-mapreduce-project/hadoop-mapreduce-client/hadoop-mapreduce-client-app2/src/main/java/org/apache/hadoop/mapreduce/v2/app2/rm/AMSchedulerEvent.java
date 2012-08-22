package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.event.AbstractEvent;


public class AMSchedulerEvent extends AbstractEvent<AMSchedulerEventType> {

  // TODO Not a very useful class...
  public AMSchedulerEvent(AMSchedulerEventType type) {
    super(type);
  }
}
