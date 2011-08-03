package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NodeStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public interface RMContext {

  Dispatcher getDispatcher();

  NodeStore getNodeStore();

  ApplicationsStore getApplicationsStore();

  ConcurrentMap<ApplicationId, RMApp> getRMApps();

  ConcurrentMap<ContainerId, RMContainer> getRMContainers();

  ConcurrentMap<NodeId, RMNode> getRMNodes();

  AMLivelinessMonitor getAMLivelinessMonitor();

  ContainerAllocationExpirer getContainerAllocationExpirer();
}