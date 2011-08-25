package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class NodeAddedSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;

  public NodeAddedSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_ADDED);
    this.rmNode = rmNode;
  }

  public RMNode getAddedRMNode() {
    return rmNode;
  }

}
