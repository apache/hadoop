package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class RMNodeEvent extends AbstractEvent<RMNodeEventType> {

  private final NodeId nodeId;

  public RMNodeEvent(NodeId nodeId, RMNodeEventType type) {
    super(type);
    this.nodeId = nodeId;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }
}
