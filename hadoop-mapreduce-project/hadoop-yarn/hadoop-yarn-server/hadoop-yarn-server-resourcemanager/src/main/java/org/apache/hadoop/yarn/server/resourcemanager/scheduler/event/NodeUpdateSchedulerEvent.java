package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class NodeUpdateSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;
  private final Map<ApplicationId, List<Container>> containers;

  public NodeUpdateSchedulerEvent(RMNode rmNode,
      Map<ApplicationId, List<Container>> containers) {
    super(SchedulerEventType.NODE_UPDATE);
    this.rmNode = rmNode;
    this.containers = containers;
  }

  public RMNode getRMNode() {
    return rmNode;
  }

  public Map<ApplicationId, List<Container>> getContainers() {
    return containers;
  }

}
