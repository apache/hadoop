package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public class NMCommunicatorLaunchRequestEvent extends NMCommunicatorEvent {

  private final ContainerLaunchContext clc;

  public NMCommunicatorLaunchRequestEvent(ContainerLaunchContext clc,
      Container container) {
    super(clc.getContainerId(), container.getNodeId(), container
        .getContainerToken(), NMCommunicatorEventType.CONTAINER_LAUNCH_REQUEST);
    this.clc = clc;
  }

  public ContainerLaunchContext getContainerLaunchContext() {
    return this.clc;
  }

}
