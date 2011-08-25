package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeReport;

public interface GetClusterNodesResponse {
  List<NodeReport> getNodeReports();
  void setNodeReports(List<NodeReport> nodeReports);
}
