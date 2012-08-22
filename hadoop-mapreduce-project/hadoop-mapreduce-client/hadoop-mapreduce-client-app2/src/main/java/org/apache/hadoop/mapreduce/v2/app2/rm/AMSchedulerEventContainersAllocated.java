package org.apache.hadoop.mapreduce.v2.app2.rm;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class AMSchedulerEventContainersAllocated extends AMSchedulerEvent {

  private final List<ContainerId> containerIds;
  private final boolean headRoomChanged;

  public AMSchedulerEventContainersAllocated(List<ContainerId> containerIds,
      boolean headRoomChanged) {
    super(AMSchedulerEventType.S_CONTAINERS_ALLOCATED);
    this.containerIds = containerIds;
    this.headRoomChanged = headRoomChanged;
  }

  public List<ContainerId> getContainerIds() {
    return this.containerIds;
  }

  public boolean didHeadroomChange() {
    return headRoomChanged;
  }
}
