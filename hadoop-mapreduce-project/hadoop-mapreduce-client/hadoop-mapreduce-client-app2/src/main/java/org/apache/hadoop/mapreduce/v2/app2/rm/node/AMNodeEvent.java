package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;

// TODO: Implement.

public class AMNodeEvent extends AbstractEvent<AMNodeEventType> {

  private final NodeId nodeId;

  public AMNodeEvent(NodeId nodeId, AMNodeEventType type) {
    super(type);
    this.nodeId = nodeId;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }
}
