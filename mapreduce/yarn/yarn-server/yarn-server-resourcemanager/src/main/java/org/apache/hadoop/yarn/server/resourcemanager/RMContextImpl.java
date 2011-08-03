package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NodeStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class RMContextImpl implements RMContext {

  private final Dispatcher rmDispatcher;
  private final Store store;

  private final ConcurrentMap<ApplicationId, RMApp> applications
    = new ConcurrentHashMap<ApplicationId, RMApp>();

  private final ConcurrentMap<ContainerId, RMContainer> containers
    = new ConcurrentHashMap<ContainerId, RMContainer>();

  private final ConcurrentMap<NodeId, RMNode> nodes
    = new ConcurrentHashMap<NodeId, RMNode>();

  private AMLivelinessMonitor amLivelinessMonitor;
  private ContainerAllocationExpirer containerAllocationExpirer;

  public RMContextImpl(Store store, Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor) {
    this.store = store;
    this.rmDispatcher = rmDispatcher;
    this.containerAllocationExpirer = containerAllocationExpirer;
    this.amLivelinessMonitor = amLivelinessMonitor;
  }
  
  @Override
  public Dispatcher getDispatcher() {
    return this.rmDispatcher;
  }

  @Override
  public NodeStore getNodeStore() {
   return store;
  }

  @Override
  public ApplicationsStore getApplicationsStore() {
    return store;
  }

  @Override
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return this.applications;
  }

  @Override
  public ConcurrentMap<ContainerId, RMContainer> getRMContainers() {
    return this.containers;
  }

  @Override
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return this.nodes;
  }

  @Override
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return this.containerAllocationExpirer;
  }

  @Override
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }
}