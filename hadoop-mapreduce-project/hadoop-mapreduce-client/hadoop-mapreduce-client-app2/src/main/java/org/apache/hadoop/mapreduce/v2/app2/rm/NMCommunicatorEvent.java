package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class NMCommunicatorEvent extends AbstractEvent<NMCommunicatorEventType> {

  private final ContainerId containerId;
  private final NodeId nodeId;
  private final ContainerToken containerToken;

  public NMCommunicatorEvent(ContainerId containerId, NodeId nodeId,
      ContainerToken containerToken, NMCommunicatorEventType type) {
    super(type);
    this.containerId = containerId;
    this.nodeId = nodeId;
    this.containerToken = containerToken;
  }

  public ContainerId getContainerId() {
    return this.containerId;
  }

  public NodeId getNodeId() {
    return this.nodeId;
  }

  public ContainerToken getContainerToken() {
    return this.containerToken;
  }
  
  public String toSrting() {
    return super.toString() + " for container " + containerId + ", nodeId: "
        + nodeId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((containerId == null) ? 0 : containerId.hashCode());
    result = prime * result
        + ((containerToken == null) ? 0 : containerToken.hashCode());
    result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NMCommunicatorEvent other = (NMCommunicatorEvent) obj;
    if (containerId == null) {
      if (other.containerId != null)
        return false;
    } else if (!containerId.equals(other.containerId))
      return false;
    if (containerToken == null) {
      if (other.containerToken != null)
        return false;
    } else if (!containerToken.equals(other.containerToken))
      return false;
    if (nodeId == null) {
      if (other.nodeId != null)
        return false;
    } else if (!nodeId.equals(other.nodeId))
      return false;
    return true;
  }
}
