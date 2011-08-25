package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.yarn.server.api.records.NodeStatus;


public interface NodeHeartbeatRequest {
  public abstract NodeStatus getNodeStatus();
  
  public abstract void setNodeStatus(NodeStatus status);
}
