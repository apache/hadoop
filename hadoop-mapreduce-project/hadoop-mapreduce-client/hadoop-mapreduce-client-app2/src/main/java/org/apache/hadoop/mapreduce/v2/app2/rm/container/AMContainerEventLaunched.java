package org.apache.hadoop.mapreduce.v2.app2.rm.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMContainerEventLaunched extends AMContainerEvent {

  private final int shufflePort;

  public AMContainerEventLaunched(ContainerId containerId, int shufflePort) {
    super(containerId, AMContainerEventType.C_LAUNCHED);
    this.shufflePort = shufflePort;
  }

  public int getShufflePort() {
    return this.shufflePort;
  }

}
