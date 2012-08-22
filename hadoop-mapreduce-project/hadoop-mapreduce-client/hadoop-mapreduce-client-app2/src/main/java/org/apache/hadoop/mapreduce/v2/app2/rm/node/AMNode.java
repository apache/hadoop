package org.apache.hadoop.mapreduce.v2.app2.rm.node;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;

public interface AMNode extends EventHandler<AMNodeEvent> {
  
  public NodeId getNodeId();
  public AMNodeState getState();
  public List<ContainerId> getContainers();

  public boolean isUsable();
  public boolean isBlacklisted();
}
