package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeCleanAppEvent extends RMNodeEvent {

  private ApplicationId appId;

  public RMNodeCleanAppEvent(NodeId nodeId, ApplicationId appId) {
    super(nodeId, RMNodeEventType.CLEANUP_APP);
    this.appId = appId;
  }

  public ApplicationId getAppId() {
    return this.appId;
  }
}
