package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class AMNodeEventContainerAllocated extends AMNodeEvent {

  private final ContainerId containerId;

  public AMNodeEventContainerAllocated(NodeId nodeId, ContainerId containerId) {
    super(nodeId, AMNodeEventType.N_CONTAINER_ALLOCATED);
    this.containerId = containerId;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

}
