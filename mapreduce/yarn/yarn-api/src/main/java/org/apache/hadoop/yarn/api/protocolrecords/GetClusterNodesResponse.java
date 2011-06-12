package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeManagerInfo;

public interface GetClusterNodesResponse {
  List<NodeManagerInfo> getNodeManagerList();
  void setNodeManagerList(List<NodeManagerInfo> nodeManagers);
}
