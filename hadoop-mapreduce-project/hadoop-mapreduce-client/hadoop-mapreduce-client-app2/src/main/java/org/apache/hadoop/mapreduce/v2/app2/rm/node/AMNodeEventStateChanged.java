package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import org.apache.hadoop.yarn.api.records.NodeReport;

public class AMNodeEventStateChanged extends AMNodeEvent {

  private NodeReport nodeReport;

  public AMNodeEventStateChanged(NodeReport nodeReport) {
    super(nodeReport.getNodeId(), nodeReport.getNodeHealthStatus()
        .getIsNodeHealthy() ? AMNodeEventType.N_TURNED_HEALTHY
        : AMNodeEventType.N_TURNED_UNHEALTHY);
    this.nodeReport = nodeReport;
  }

  public NodeReport getNodeReport() {
    return this.nodeReport;
  }

}
