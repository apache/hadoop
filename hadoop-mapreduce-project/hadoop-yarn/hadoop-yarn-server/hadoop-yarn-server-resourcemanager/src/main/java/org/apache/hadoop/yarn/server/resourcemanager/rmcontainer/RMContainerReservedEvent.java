package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * The event signifying that a container has been reserved.
 * 
 * The event encapsulates information on the amount of reservation
 * and the node on which the reservation is in effect.
 */
public class RMContainerReservedEvent extends RMContainerEvent {

  private final Resource reservedResource;
  private final NodeId reservedNode;
  private final Priority reservedPriority;
  
  public RMContainerReservedEvent(ContainerId containerId,
      Resource reservedResource, NodeId reservedNode, 
      Priority reservedPriority) {
    super(containerId, RMContainerEventType.RESERVED);
    this.reservedResource = reservedResource;
    this.reservedNode = reservedNode;
    this.reservedPriority = reservedPriority;
  }

  public Resource getReservedResource() {
    return reservedResource;
  }

  public NodeId getReservedNode() {
    return reservedNode;
  }

  public Priority getReservedPriority() {
    return reservedPriority;
  }

}
