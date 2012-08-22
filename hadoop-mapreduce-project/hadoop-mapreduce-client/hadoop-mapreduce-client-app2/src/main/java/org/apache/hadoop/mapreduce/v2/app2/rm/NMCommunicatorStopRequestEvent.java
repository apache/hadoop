package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;

public class NMCommunicatorStopRequestEvent extends NMCommunicatorEvent {

  public NMCommunicatorStopRequestEvent(ContainerId containerId, NodeId nodeId,
      ContainerToken containerToken) {
    super(containerId, nodeId, containerToken,
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST);
  }

}
