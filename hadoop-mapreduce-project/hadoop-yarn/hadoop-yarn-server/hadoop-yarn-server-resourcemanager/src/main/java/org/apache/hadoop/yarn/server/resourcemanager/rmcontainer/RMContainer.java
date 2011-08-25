package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;

public interface RMContainer extends EventHandler<RMContainerEvent> {

  ContainerId getContainerId();

  ApplicationAttemptId getApplicationAttemptId();

  RMContainerState getState();

  Container getContainer();

  Resource getReservedResource();

  NodeId getReservedNode();
  
  Priority getReservedPriority();

}
