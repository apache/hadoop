package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeCleanContainerEvent extends RMNodeEvent {

  private ContainerId contId;

  public RMNodeCleanContainerEvent(NodeId nodeId, ContainerId contId) {
    super(nodeId, RMNodeEventType.CLEANUP_CONTAINER);
    this.contId = contId;
  }

  public ContainerId getContainerId() {
    return this.contId;
  }
}
