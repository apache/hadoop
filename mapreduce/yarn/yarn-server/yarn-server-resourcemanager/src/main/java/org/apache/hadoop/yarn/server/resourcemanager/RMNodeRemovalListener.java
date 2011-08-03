package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.NodeId;

public interface RMNodeRemovalListener {
  void RMNodeRemoved(NodeId nodeId);
}
