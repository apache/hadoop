/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.DistributedSchedulerProtocol;
import org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl;


import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistSchedRegisterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ApplicationMasterProtocol.ApplicationMasterProtocolService;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed
    .TopKNodeSelector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The DistributedSchedulingService is started instead of the
 * ApplicationMasterService if DistributedScheduling is enabled for the YARN
 * cluster.
 * It extends the functionality of the ApplicationMasterService by servicing
 * clients (AMs and AMRMProxy request interceptors) that understand the
 * DistributedSchedulingProtocol.
 */
public class DistributedSchedulingService extends ApplicationMasterService
    implements DistributedSchedulerProtocol, EventHandler<SchedulerEvent> {

  private static final Log LOG =
      LogFactory.getLog(DistributedSchedulingService.class);

  private final TopKNodeSelector clusterMonitor;

  private final ConcurrentHashMap<String, Set<NodeId>> rackToNode =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Set<NodeId>> hostToNode =
      new ConcurrentHashMap<>();

  public DistributedSchedulingService(RMContext rmContext,
      YarnScheduler scheduler) {
    super(DistributedSchedulingService.class.getName(), rmContext, scheduler);
    int k = rmContext.getYarnConfiguration().getInt(
        YarnConfiguration.DIST_SCHEDULING_TOP_K,
        YarnConfiguration.DIST_SCHEDULING_TOP_K_DEFAULT);
    long topKComputationInterval = rmContext.getYarnConfiguration().getLong(
        YarnConfiguration.DIST_SCHEDULING_TOP_K_COMPUTE_INT_MS,
        YarnConfiguration.DIST_SCHEDULING_TOP_K_COMPUTE_INT_MS_DEFAULT);
    TopKNodeSelector.TopKComparator comparator =
        TopKNodeSelector.TopKComparator.valueOf(
            rmContext.getYarnConfiguration().get(
                YarnConfiguration.DIST_SCHEDULING_TOP_K_COMPARATOR,
                YarnConfiguration.DIST_SCHEDULING_TOP_K_COMPARATOR_DEFAULT));
    TopKNodeSelector topKSelector =
        new TopKNodeSelector(k, topKComputationInterval, comparator);
    this.clusterMonitor = topKSelector;
  }

  @Override
  public Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    Server server = rpc.getServer(DistributedSchedulerProtocol.class, this,
        addr, serverConf, secretManager,
        serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
    // To support application running on NMs that DO NOT support
    // Dist Scheduling... The server multiplexes both the
    // ApplicationMasterProtocol as well as the DistributedSchedulingProtocol
    ((RPC.Server) server).addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        ApplicationMasterProtocolPB.class,
        ApplicationMasterProtocolService.newReflectiveBlockingService(
            new ApplicationMasterProtocolPBServiceImpl(this)));
    return server;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    return super.registerApplicationMaster(request);
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    return super.finishApplicationMaster(request);
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    return super.allocate(request);
  }

  @Override
  public DistSchedRegisterResponse
  registerApplicationMasterForDistributedScheduling(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    RegisterApplicationMasterResponse response =
        registerApplicationMaster(request);
    DistSchedRegisterResponse dsResp = recordFactory
        .newRecordInstance(DistSchedRegisterResponse.class);
    dsResp.setRegisterResponse(response);
    dsResp.setMinAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MIN_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_MIN_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MIN_VCORES,
                YarnConfiguration.DIST_SCHEDULING_MIN_VCORES_DEFAULT)
        )
    );
    dsResp.setMaxAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MAX_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_MAX_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_MAX_VCORES,
                YarnConfiguration.DIST_SCHEDULING_MAX_VCORES_DEFAULT)
        )
    );
    dsResp.setIncrAllocatableCapabilty(
        Resource.newInstance(
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_INCR_MEMORY,
                YarnConfiguration.DIST_SCHEDULING_INCR_MEMORY_DEFAULT),
            getConfig().getInt(
                YarnConfiguration.DIST_SCHEDULING_INCR_VCORES,
                YarnConfiguration.DIST_SCHEDULING_INCR_VCORES_DEFAULT)
        )
    );
    dsResp.setContainerTokenExpiryInterval(
        getConfig().getInt(
            YarnConfiguration.DIST_SCHEDULING_CONTAINER_TOKEN_EXPIRY_MS,
            YarnConfiguration.
                DIST_SCHEDULING_CONTAINER_TOKEN_EXPIRY_MS_DEFAULT));
    dsResp.setContainerIdStart(
        this.rmContext.getEpoch() << ResourceManager.EPOCH_BIT_SHIFT);

    // Set nodes to be used for scheduling
    dsResp.setNodesForScheduling(
        new ArrayList<>(this.clusterMonitor.selectNodes()));
    return dsResp;
  }

  @Override
  public DistSchedAllocateResponse allocateForDistributedScheduling
      (AllocateRequest request) throws YarnException, IOException {
    AllocateResponse response = allocate(request);
    DistSchedAllocateResponse dsResp = recordFactory.newRecordInstance
        (DistSchedAllocateResponse.class);
    dsResp.setAllocateResponse(response);
    dsResp.setNodesForScheduling(
        new ArrayList<>(this.clusterMonitor.selectNodes()));
    return dsResp;
  }

  private void addToMapping(ConcurrentHashMap<String, Set<NodeId>> mapping,
      String rackName, NodeId nodeId) {
    if (rackName != null) {
      mapping.putIfAbsent(rackName, new HashSet<NodeId>());
      Set<NodeId> nodeIds = mapping.get(rackName);
      synchronized (nodeIds) {
        nodeIds.add(nodeId);
      }
    }
  }

  private void removeFromMapping(ConcurrentHashMap<String, Set<NodeId>> mapping,
      String rackName, NodeId nodeId) {
    if (rackName != null) {
      Set<NodeId> nodeIds = mapping.get(rackName);
      synchronized (nodeIds) {
        nodeIds.remove(nodeId);
      }
    }
  }

  @Override
  public void handle(SchedulerEvent event) {
    switch (event.getType()) {
      case NODE_ADDED:
        if (!(event instanceof NodeAddedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
        clusterMonitor.addNode(nodeAddedEvent.getContainerReports(),
            nodeAddedEvent.getAddedRMNode());
        addToMapping(rackToNode, nodeAddedEvent.getAddedRMNode().getRackName(),
            nodeAddedEvent.getAddedRMNode().getNodeID());
        addToMapping(hostToNode, nodeAddedEvent.getAddedRMNode().getHostName(),
            nodeAddedEvent.getAddedRMNode().getNodeID());
        break;
      case NODE_REMOVED:
        if (!(event instanceof NodeRemovedSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeRemovedSchedulerEvent nodeRemovedEvent =
            (NodeRemovedSchedulerEvent)event;
        clusterMonitor.removeNode(nodeRemovedEvent.getRemovedRMNode());
        removeFromMapping(rackToNode,
            nodeRemovedEvent.getRemovedRMNode().getRackName(),
            nodeRemovedEvent.getRemovedRMNode().getNodeID());
        removeFromMapping(hostToNode,
            nodeRemovedEvent.getRemovedRMNode().getHostName(),
            nodeRemovedEvent.getRemovedRMNode().getNodeID());
        break;
      case NODE_UPDATE:
        if (!(event instanceof NodeUpdateSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
        clusterMonitor.nodeUpdate(nodeUpdatedEvent.getRMNode());
        break;
      case NODE_RESOURCE_UPDATE:
        if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
          throw new RuntimeException("Unexpected event type: " + event);
        }
        NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
            (NodeResourceUpdateSchedulerEvent)event;
        clusterMonitor.updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
            nodeResourceUpdatedEvent.getResourceOption());
        break;

      // <-- IGNORED EVENTS : START -->
      case APP_ADDED:
        break;
      case APP_REMOVED:
        break;
      case APP_ATTEMPT_ADDED:
        break;
      case APP_ATTEMPT_REMOVED:
        break;
      case CONTAINER_EXPIRED:
        break;
      case NODE_LABELS_UPDATE:
        break;
      // <-- IGNORED EVENTS : END -->
      default:
        LOG.error("Unknown event arrived at DistributedSchedulingService: "
            + event.toString());
    }
  }

}
