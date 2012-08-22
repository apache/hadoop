package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMCommunicatorEvent extends AbstractEvent<RMCommunicatorEventType> {

  public RMCommunicatorEvent(RMCommunicatorEventType type) {
    super(type);
  }

}
