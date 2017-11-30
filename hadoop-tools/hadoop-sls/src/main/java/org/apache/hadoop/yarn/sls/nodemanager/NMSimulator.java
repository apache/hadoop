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

package org.apache.hadoop.yarn.sls.nodemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
@Unstable
public class NMSimulator extends TaskRunner.Task {
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private DelayQueue<ContainerSimulator> containerQueue;
  private Map<ContainerId, ContainerSimulator> runningContainers;
  private List<ContainerId> amContainerList;
  // resource manager
  private ResourceManager rm;
  // heart beat response id
  private int RESPONSE_ID = 1;
  private final static Logger LOG = LoggerFactory.getLogger(NMSimulator.class);
  
  public void init(String nodeIdStr, Resource nodeResource,
          int dispatchTime, int heartBeatInterval, ResourceManager rm)
          throws IOException, YarnException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
            heartBeatInterval);
    // create resource
    String rackHostName[] = SLSUtils.getRackHostName(nodeIdStr);
    this.node = NodeInfo.newNodeInfo(rackHostName[0], rackHostName[1],
        Resources.clone(nodeResource));
    this.rm = rm;
    // init data structures
    completedContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers =
            new ConcurrentHashMap<ContainerId, ContainerSimulator>();
    // register NM with RM
    RegisterNodeManagerRequest req =
            Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    RegisterNodeManagerResponse response = rm.getResourceTrackerService()
            .registerNodeManager(req);
    masterKey = response.getNMTokenMasterKey();
  }

  @Override
  public void firstStep() {
    // do nothing
  }

  @Override
  public void middleStep() throws Exception {
    // we check the lifetime for each running containers
    ContainerSimulator cs = null;
    synchronized(completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        LOG.debug("Container {} has completed", cs.getId());
      }
    }
    
    // send heart beat
    NodeHeartbeatRequest beatRequest =
            Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);
    
    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID ++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    beatRequest.setNodeStatus(ns);
    NodeHeartbeatResponse beatResponse =
        rm.getResourceTrackerService().nodeHeartbeat(beatRequest);
    if (! beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized(releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()){
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized(amContainerList) {
              amContainerList.remove(containerId);
            }
            LOG.debug("NodeManager {} releases an AM ({}).",
                node.getNodeID(), containerId);
          } else {
            cs = runningContainers.remove(containerId);
            containerQueue.remove(cs);
            releasedContainerList.add(containerId);
            LOG.debug("NodeManager {} releases a container ({}).",
                node.getNodeID(), containerId);
          }
        }
      }
    }
    if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
      lastStep();
    }
  }

  @Override
  public void lastStep() {
    // do nothing
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
    // add running containers
    for (ContainerSimulator container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(),
        ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
    }
    synchronized(amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId,
            ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
      }
    }
    // add complete containers
    synchronized(completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        LOG.debug("NodeManager {} completed container ({}).",
            node.getNodeID(), cId);
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized(releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        LOG.debug("NodeManager {} released container ({}).",
            node.getNodeID(), cId);
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED));
      }
      releasedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId, 
                                             ContainerState state,
                                             int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public RMNode getNode() {
    return node;
  }

  /**
   * launch a new container with the given life time
   */
  public void addNewContainer(Container container, long lifeTimeMS) {
    LOG.debug("NodeManager {} launches a new container ({}).",
        node.getNodeID(), container.getId());
    if (lifeTimeMS != -1) {
      // normal container
      ContainerSimulator cs = new ContainerSimulator(container.getId(),
              container.getResource(), lifeTimeMS + System.currentTimeMillis(),
              lifeTimeMS);
      containerQueue.add(cs);
      runningContainers.put(cs.getId(), cs);
    } else {
      // AM container
      // -1 means AMContainer
      synchronized(amContainerList) {
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized(amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized(completedContainerList) {
      completedContainerList.add(containerId);
    }
  }

  @VisibleForTesting
  Map<ContainerId, ContainerSimulator> getRunningContainers() {
    return runningContainers;
  }

  @VisibleForTesting
  List<ContainerId> getAMContainers() {
    return amContainerList;
  }

  @VisibleForTesting
  List<ContainerId> getCompletedContainers() {
    return completedContainerList;
  }
}
