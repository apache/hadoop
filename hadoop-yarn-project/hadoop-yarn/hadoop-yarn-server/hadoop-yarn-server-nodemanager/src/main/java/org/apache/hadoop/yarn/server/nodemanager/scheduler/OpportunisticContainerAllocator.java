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

package org.apache.hadoop.yarn.server.nodemanager.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.scheduler.LocalScheduler.DistSchedulerParams;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>The OpportunisticContainerAllocator allocates containers on a given list
 * of Nodes after it modifies the container sizes to within allowable limits
 * specified by the <code>ClusterManager</code> running on the RM. It tries to
 * distribute the containers as evenly as possible. It also uses the
 * <code>NMTokenSecretManagerInNM</code> to generate the required NM tokens for
 * the allocated containers</p>
 */
public class OpportunisticContainerAllocator {

  private static final Log LOG =
      LogFactory.getLog(OpportunisticContainerAllocator.class);

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DominantResourceCalculator();

  static class ContainerIdCounter {
    final AtomicLong containerIdCounter = new AtomicLong(1);

    void resetContainerIdCounter(long containerIdStart) {
      this.containerIdCounter.set(containerIdStart);
    }

    long generateContainerId() {
      return this.containerIdCounter.decrementAndGet();
    }
  }

  private final NodeStatusUpdater nodeStatusUpdater;
  private final Context context;
  private int webpagePort;

  public OpportunisticContainerAllocator(NodeStatusUpdater nodeStatusUpdater,
      Context context, int webpagePort) {
    this.nodeStatusUpdater = nodeStatusUpdater;
    this.context = context;
    this.webpagePort = webpagePort;
  }

  public Map<Resource, List<Container>> allocate(DistSchedulerParams appParams,
      ContainerIdCounter idCounter, Collection<ResourceRequest> resourceAsks,
      Set<String> blacklist, ApplicationAttemptId appAttId,
      Map<String, NodeId> allNodes, String userName) throws YarnException {
    Map<Resource, List<Container>> containers = new HashMap<>();
    Set<String> nodesAllocated = new HashSet<>();
    for (ResourceRequest anyAsk : resourceAsks) {
      allocateOpportunisticContainers(appParams, idCounter, blacklist, appAttId,
          allNodes, userName, containers, nodesAllocated, anyAsk);
      LOG.info("Opportunistic allocation requested for ["
          + "priority=" + anyAsk.getPriority()
          + ", num_containers=" + anyAsk.getNumContainers()
          + ", capability=" + anyAsk.getCapability() + "]"
          + " allocated = " + containers.get(anyAsk.getCapability()).size());
    }
    return containers;
  }

  private void allocateOpportunisticContainers(DistSchedulerParams appParams,
      ContainerIdCounter idCounter, Set<String> blacklist,
      ApplicationAttemptId id, Map<String, NodeId> allNodes, String userName,
      Map<Resource, List<Container>> containers, Set<String> nodesAllocated,
      ResourceRequest anyAsk) throws YarnException {
    int toAllocate = anyAsk.getNumContainers()
        - (containers.isEmpty() ?
        0 : containers.get(anyAsk.getCapability()).size());

    List<String> topKNodesLeft = new ArrayList<>();
    for (String s : allNodes.keySet()) {
      // Bias away from whatever we have already allocated and respect blacklist
      if (nodesAllocated.contains(s) || blacklist.contains(s)) {
        continue;
      }
      topKNodesLeft.add(s);
    }
    int numAllocated = 0;
    int nextNodeToAllocate = 0;
    for (int numCont = 0; numCont < toAllocate; numCont++) {
      String topNode = topKNodesLeft.get(nextNodeToAllocate);
      nextNodeToAllocate++;
      nextNodeToAllocate %= topKNodesLeft.size();
      NodeId nodeId = allNodes.get(topNode);
      Container container = buildContainer(appParams, idCounter, anyAsk, id,
          userName, nodeId);
      List<Container> cList = containers.get(anyAsk.getCapability());
      if (cList == null) {
        cList = new ArrayList<>();
        containers.put(anyAsk.getCapability(), cList);
      }
      cList.add(container);
      numAllocated++;
      LOG.info("Allocated [" + container.getId() + "] as opportunistic.");
    }
    LOG.info("Allocated " + numAllocated + " opportunistic containers.");
  }

  private Container buildContainer(DistSchedulerParams appParams,
      ContainerIdCounter idCounter, ResourceRequest rr, ApplicationAttemptId id,
      String userName, NodeId nodeId) throws YarnException {
    ContainerId cId =
        ContainerId.newContainerId(id, idCounter.generateContainerId());

    // Normalize the resource asks (Similar to what the the RM scheduler does
    // before accepting an ask)
    Resource capability = normalizeCapability(appParams, rr);

    long currTime = System.currentTimeMillis();
    ContainerTokenIdentifier containerTokenIdentifier =
        new ContainerTokenIdentifier(
            cId, nodeId.getHost() + ":" + nodeId.getPort(), userName,
            capability, currTime + appParams.containerTokenExpiryInterval,
            context.getContainerTokenSecretManager().getCurrentKey().getKeyId(),
            nodeStatusUpdater.getRMIdentifier(), rr.getPriority(), currTime,
            null, CommonNodeLabelsManager.NO_LABEL, ContainerType.TASK,
            ExecutionType.OPPORTUNISTIC);
    byte[] pwd =
        context.getContainerTokenSecretManager().createPassword(
            containerTokenIdentifier);
    Token containerToken = newContainerToken(nodeId, pwd,
        containerTokenIdentifier);
    Container container = BuilderUtils.newContainer(
        cId, nodeId, nodeId.getHost() + ":" + webpagePort,
        capability, rr.getPriority(), containerToken,
        containerTokenIdentifier.getExecutionType());
    return container;
  }

  private Resource normalizeCapability(DistSchedulerParams appParams,
      ResourceRequest ask) {
    return Resources.normalize(RESOURCE_CALCULATOR,
        ask.getCapability(), appParams.minResource, appParams.maxResource,
        appParams.incrementResource);
  }

  public static Token newContainerToken(NodeId nodeId, byte[] password,
      ContainerTokenIdentifier tokenIdentifier) {
    // RPC layer client expects ip:port as service for tokens
    InetSocketAddress addr = NetUtils.createSocketAddrForHost(nodeId.getHost(),
        nodeId.getPort());
    // NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
    Token containerToken = Token.newInstance(tokenIdentifier.getBytes(),
        ContainerTokenIdentifier.KIND.toString(), password, SecurityUtil
            .buildTokenService(addr).toString());
    return containerToken;
  }

}
