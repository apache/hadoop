package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class NodeRemovedSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;

  public NodeRemovedSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_REMOVED);
    this.rmNode = rmNode;
  }

  public RMNode getRemovedRMNode() {
    return rmNode;
  }

}
