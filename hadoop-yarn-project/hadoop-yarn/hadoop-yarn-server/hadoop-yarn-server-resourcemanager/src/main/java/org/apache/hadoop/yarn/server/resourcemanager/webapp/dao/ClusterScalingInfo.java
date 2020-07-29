/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;


import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement(name = "clusterScaling")
@XmlAccessorType(XmlAccessType.NONE)
public class ClusterScalingInfo {

  private static final Logger LOG =
        LoggerFactory.getLogger(ClusterScalingInfo.class.getName());

  public ClusterScalingInfo(){}

  @XmlElement
  public String apiVersion = RMWSConsts.SCALING_CUSTOM_HEADER_VERSION_V1;

  @XmlElement
  public String consideredResourceTypes;

  @XmlElement
  public AutoScalerInfo autoScalerInfo;

  @XmlElement
  public NodeInstanceTypeList nodeInstanceTypeList;

  @XmlElement
  public NewNMCandidates newNMCandidates = new NewNMCandidates();

  @XmlElement
  public DecommissionCandidates decommissionCandidates =
      new DecommissionCandidates();

  public ClusterScalingInfo(final ResourceManager rm,
      String resourceTypes,
      int downscalingFactorInNodeCount,
      NodeInstanceTypeList niTypeList) {
    this(rm, rm.getResourceScheduler(), resourceTypes,
        downscalingFactorInNodeCount,
        niTypeList);
  }

  public ClusterScalingInfo(final ResourceManager rm,
      final ResourceScheduler rs,
      String resTypes,
      int downscalingFactorInNodeCount, NodeInstanceTypeList niTypeList) {
    if (rs == null) {
      throw new NotFoundException("Null ResourceScheduler instance");
    }

    if (!(rs instanceof CapacityScheduler)) {
      throw new BadRequestException("Only Capacity Scheduler is supported!");
    }

    this.nodeInstanceTypeList = niTypeList;

    // If not specified, only consider memory when upscaling
    this.consideredResourceTypes =
        resTypes == null ? ResourceInformation.MEMORY_URI : resTypes;

    CapacityScheduler cs = (CapacityScheduler) rs;
    List<FiCaSchedulerNode> rmNodes = cs.getAllNodes();
    if (rmNodes.size() == 0) {
      return;
    }

    // The upscaling
    boolean upScale = recommendUpscaling(cs, consideredResourceTypes,
        nodeInstanceTypeList, newNMCandidates);

    // The downscaling
    Collection<RMNode> inActiveNodes = cs.getRMContext()
        .getInactiveRMNodes().values();
    recommendDownscaling(inActiveNodes, rmNodes,
        decommissionCandidates, downscalingFactorInNodeCount, upScale);

    this.autoScalerInfo = new AutoScalerInfo(rs);
  }

  public static class DownscalingNodeComparator implements Comparator<RMNode> {
    HashMap<RMNode, Integer> nodeToDecommissionTimeout;
    HashMap<RMNode, Integer> nodeToAMCount;
    HashMap<RMNode, Integer> nodeToRunningAppCount;

    public DownscalingNodeComparator(HashMap<RMNode, Integer> nToD,
        HashMap<RMNode, Integer> nToAM, HashMap<RMNode, Integer> nToR) {
      this.nodeToDecommissionTimeout = nToD;
      this.nodeToAMCount = nToAM;
      this.nodeToRunningAppCount = nToR;
    }

    /**
     * Sort the node, put smaller value ahead if apply:
     * 1. The decommissioning timeout (if in decommissioning states)
     * 2. The node state. Put unheathly, lost status nodes ahead
     * 3. The AM count
     * 4. The running application count
     * */
    @Override
    public int compare(RMNode o1, RMNode o2) {
      // Compare node states. Put higher score ahead
      if (o1.getState() != o2.getState()) {
        return scoreStatus(o1) > scoreStatus(o2) ? -1 : 1;
      }
      // Compare decommisoning timeout. Put smaller value ahead
      int timeout1 = nodeToDecommissionTimeout.get(o1);
      timeout1 = timeout1 == -1 ? Integer.MAX_VALUE : timeout1;
      int timeout2 = nodeToDecommissionTimeout.get(o2);
      timeout2 = timeout2 == -1 ? Integer.MAX_VALUE : timeout2;
      if (timeout1 != timeout2) {
        return timeout1 > timeout2 ? 1 : -1;
      }
      // Compare am count. Put smaller value ahead
      int amCount1 = nodeToAMCount.get(o1);
      int amCount2 = nodeToAMCount.get(o2);
      if (amCount1 != amCount2) {
        return amCount1 > amCount2 ? 1 : -1;
      }
      // Compare running application count
      int appCount1 = nodeToRunningAppCount.get(o1);
      int appCount2 = nodeToRunningAppCount.get(o2);
      if (appCount1 != appCount2) {
        return appCount1 > appCount2 ? 1 : -1;
      }
      return o1.getNodeID().compareTo(o2.getNodeID());
    }

    /** Manually return a score for state.
     *  The reason of put REBOOTED state ahead of RUNNING is because
     *  it is more likely to indicate a unhealthy state.
     * @param node
     * @return A score for the node state
     */
    private int scoreStatus(RMNode node) {
      switch (node.getState()) {
        case LOST:
          return 10;
        case UNHEALTHY:
          return 9;
        case REBOOTED:
          return 8;
        case RUNNING:
          return 7;
        case NEW:
          return 6;
        case DECOMMISSIONING:
          return 5;
        case DECOMMISSIONED:
          return 4;
        default:
          return 0;
      }
    }
  }

  /**
   * Recommend downscaling candidates based on input.
   * If the "downscalingFactorInNodeCount" is negative, it means let the engine
   * decide how many nodes to recommend. Otherwise, the nodes might not be
   * recommended by the engine, but is required by the caller like time-based
   * autoscaler asking for a fixed number.
   *
   * @param cs CapacityScheduler
   * @param decommissionCandidates structure to populate
   * @param downscalingFactorInNodeCount query param indicate node count
   * @param upScale if UpScale has recommended new candidates
   */
  public static void recommendDownscaling(Collection<RMNode> inActiveNodes,
      List<FiCaSchedulerNode> rmNodes,
      DecommissionCandidates decommissionCandidates,
      int downscalingFactorInNodeCount, boolean upScale) {
    boolean upToEngine = false;
    if (downscalingFactorInNodeCount == 0) {
      return;
    }
    // if negative value, it means let the engine decide.
    if (downscalingFactorInNodeCount < 0) {
      upToEngine = true;
    }

    HashMap<RMNode, Integer> nodeToDecommissionTimeout = new HashMap<>();
    HashMap<RMNode, Integer> nodeToAMCount = new HashMap<>();
    HashMap<RMNode, Integer> nodeToRunningAppCount = new HashMap<>();

    DownscalingNodeComparator comparator = new DownscalingNodeComparator(
        nodeToDecommissionTimeout, nodeToAMCount, nodeToRunningAppCount
    );
    TreeSet<RMNode> sortedNodes = new TreeSet<>(comparator);

    // Inactive nodes including LOST are included always
    for (RMNode rmNode : inActiveNodes) {
      nodeToAMCount.put(rmNode, 0);
      nodeToRunningAppCount.put(rmNode, 0);
      nodeToDecommissionTimeout.put(rmNode, 0);
      sortedNodes.add(rmNode);
    }

    // Downscale Idle nodes only when downscalingFactorInNodeCount is
    // set with a positive value or there is no upscale need.
    if (!upToEngine || !upScale) {
      for (FiCaSchedulerNode node : rmNodes) {
        RMNode rmNode = node.getRMNode();
        Integer deTimeout = rmNode.getDecommissioningTimeout();
        if (deTimeout == null) {
          deTimeout = -1;
        }
        // Workaround the timeout inaccuracy
        if (deTimeout > 0 && rmNode.getState() != NodeState.DECOMMISSIONING) {
          deTimeout = -1;
        }
        nodeToDecommissionTimeout.put(rmNode, deTimeout);
        int amCount = 0;
        for (RMContainer rmContainer : node.getCopiedListOfRunningContainers()) {
          // calculate AM count
          if (rmContainer.isAMContainer()) {
            amCount++;
          }
        }
        nodeToAMCount.put(rmNode, amCount);
        nodeToRunningAppCount.put(rmNode,
            node.getRMNode().getRunningApps().size());
      }

      // Sort all nodes
      for (FiCaSchedulerNode node : rmNodes) {
        RMNode rmNode = node.getRMNode();
        sortedNodes.add(rmNode);
      }
    }

    // Fetch needed count of nodes
    int neededCount = downscalingFactorInNodeCount;
    int foundCount = 0;
    if (downscalingFactorInNodeCount > sortedNodes.size()) {
      LOG.warn("Requested downscaling candidates count is larger than" +
          "cluster node count!");
      neededCount = sortedNodes.size();
    }
    for (RMNode node : sortedNodes) {
      int amCount = nodeToAMCount.get(node);
      int runningAppCount = nodeToRunningAppCount.get(node);
      boolean recommendFlag = getDownscalingRecommendFlag(amCount,
          runningAppCount,
          node.getState());
      DecommissionCandidateNodeInfo dcni = new DecommissionCandidateNodeInfo(
          nodeToAMCount.get(node),
          nodeToRunningAppCount.get(node),
          nodeToDecommissionTimeout.get(node),
          node.getState(),
          node.getNodeID().toString(),
          recommendFlag
      );

      if (upToEngine) {
        if (recommendFlag == true) {
          decommissionCandidates.add(dcni);
        }
      } else {
        decommissionCandidates.add(dcni);
        foundCount++;
        if (foundCount == neededCount) {
          return;
        }
      }
    }
  }

  public static boolean getDownscalingRecommendFlag(int amCount,
      int runningAppCount, NodeState state) {
    if (state == NodeState.LOST ||
        state == NodeState.UNHEALTHY ||
        state == NodeState.REBOOTED ) {
      return true;
    }
    // Recommend running node without running app
    if (state == NodeState.RUNNING && amCount == 0 && runningAppCount == 0) {
      return true;
    }
    // Ignore other state like DECOMMISSIONED, DECOMMISSIONING, SHUTDOWN, NEW
    return false;
  }

  // Fetches the pending containers which cannot be fit in
  // available space of any running nodes
  public static class UpScaleArbiter {

    private Map<Resource, Integer> getContainerAskToCount(
        CapacityScheduler cs) {
      NavigableMap<Resource, Integer> containerAskToCount = new TreeMap<>(
          new Comparator<Resource>() {
              @Override
              public int compare(Resource o1, Resource o2) {
                return o2.compareTo(o1);
              }
           });

      // Step 1: Get all Pending Containers
      QueueMetrics metrics = cs.getRootQueueMetrics();
      containerAskToCount.putAll(metrics.getContainerAskToCount());

      ResourceCalculator rc = cs.getResourceCalculator();
      List<FiCaSchedulerNode> rmNodes = cs.getAllNodes();
      Resource minimumAllocation = cs.getMinimumAllocation();
      Resource clusterResource = cs.getClusterResource();

      // Step 2: Sort the available resources from existing nodes
      LinkedList<Resource> availableResources = new LinkedList<>();
      for (FiCaSchedulerNode rmNode : rmNodes) {
        Resource nodeAvailableResource = Resources.add(
            rmNode.getUnallocatedResource(),
            rmNode.getTotalKillableResources());
        if (rc.computeAvailableContainers(nodeAvailableResource,
            minimumAllocation) > 0) {
          availableResources.add(nodeAvailableResource);
        }
      }

      if (availableResources.isEmpty()) {
        return containerAskToCount;
      }
      Collections.sort(availableResources);

      // Step 3: Order the Pending Containers by placing smaller capacity ahead
      NavigableMap<Resource, Integer> revMap = containerAskToCount.descendingMap();

      // Step 4: Find the Pending Containers which cannot be fit with
      // available resources.
      for (Map.Entry<Resource, Integer> pendingContainer :
           revMap.entrySet()) {
        Resource pending = Resources.clone(pendingContainer.getKey());
        int count = pendingContainer.getValue();

        // No Available Resources to fit the pending resources
        if (availableResources.isEmpty() || Resources.lessThan(rc,
            clusterResource, availableResources.getLast(), pending)) {
          break;
        }

        ListIterator<Resource> listItr = availableResources.listIterator();
        while (listItr.hasNext() && count > 0) {
          Resource curavailable = listItr.next();
          Resource available = Resources.clone(curavailable);
          while (Resources.greaterThanOrEqual(rc,
              clusterResource, available, pending) && count > 0) {
            Resources.subtractFrom(available, pending);
            count--;
          }
          if (count > 0) {
            listItr.remove();
          }
        }

        if (count == 0) {
          containerAskToCount.remove(pendingContainer.getKey());
        } else {
          containerAskToCount.put(pendingContainer.getKey(), count);
        }
      }
      return containerAskToCount;
    }
  }

  public static boolean recommendUpscaling(CapacityScheduler cs,
      String resourceTypes, NodeInstanceTypeList nodeInstanceTypeList,
      NewNMCandidates newNMCandidates) {

    QueueMetrics metrics = cs.getRootQueueMetrics();
    int pendingAppCount = metrics.getAppsPending();
    int pendingContainersCount = metrics.getPendingContainers();

    if (pendingAppCount > 0 || pendingContainersCount > 0) {
      // Choose resource calculator based on specified resource types
      ResourceCalculator rc = chooseResCalculator(resourceTypes);
      UpScaleArbiter upScaleArbiter = new UpScaleArbiter();
      Map<Resource, Integer> containerAskToCount =
          upScaleArbiter.getContainerAskToCount(cs);

      if (containerAskToCount.size() > 0) {
        // Given existing node types, found the maximum count of instance
        // that can serve the pending resource. Generally, the more instance,
        // the more opportunity for future scale down
        recommendNewInstances(containerAskToCount, newNMCandidates,
            nodeInstanceTypeList.getInstanceTypes(), rc);
        return true;
      }
    }
    return false;
  }

  // If only contains "memory-mb", use default resource calculator
  public static ResourceCalculator chooseResCalculator(String resourceTypes) {
    String lowerCaseRes = resourceTypes.toLowerCase();
    if (lowerCaseRes.isEmpty()) {
      return new DefaultResourceCalculator();
    }
    String[] types = lowerCaseRes.split(",");
    if (types == null) {
      return new DefaultResourceCalculator();
    }
    if (types.length > 1) {
      return new DominantResourceCalculator();
    }
    return new DefaultResourceCalculator();
  }

  /**
   * It in essence recommend a minimum instances that can satisfy the pending
   * containers.
   * */
  public static void recommendNewInstances(
      Map<Resource, Integer> pendingContainers,
      NewNMCandidates newNMCandidates, List<NodeInstanceType> allTypes,
      ResourceCalculator rc) {
    int[] suitableInstanceRet = null;

    Iterator<Map.Entry<Resource, Integer>> it =
        pendingContainers.entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<Resource, Integer> entry = it.next();

      // What if the existing new instance have headroom
      // to allocate for some containers?
      // Try allocate on existing nodes' headroom
      scheduleBasedOnRecommendedNewInstance(entry.getKey(), entry.getValue(),
          newNMCandidates, entry, rc);
      if (entry.getValue() == 0) {
        // this means we can allocate on existing new instance's headroom
        continue;
      }
      suitableInstanceRet = NodeInstanceType.getSuitableInstanceType(
          entry.getKey(), allTypes, rc);
      int ti = suitableInstanceRet[0];
      if (ti == -1) {
        LOG.warn("No capable instance type for container resource: "
            + entry.getKey() + ", count: " + entry.getValue());
      } else {
        Resource containerResource = entry.getKey();
        int containerCount = entry.getValue();
        NodeInstanceType t = allTypes.get(ti);
        int buckets = suitableInstanceRet[1];
        int instanceCount = (int)Math.ceil(
            (double)containerCount / (double)buckets);
        Resource planToUseResourceInThisNodeType = Resources.multiplyAndRoundUp(
            containerResource, containerCount);
        newNMCandidates.add(t, instanceCount, planToUseResourceInThisNodeType);
        newNMCandidates.setRecommendActionTime("Now");
      }
    }
  }

  public static void scheduleBasedOnRecommendedNewInstance(
      Resource containerRes,
      int count, NewNMCandidates newNMCandidates,
      Map.Entry<Resource, Integer> entry, ResourceCalculator rc) {
    for (NewSingleTypeNMCandidate singleTypeNMCandidate :
        newNMCandidates.getCandidates()) {
      Resource headroom = singleTypeNMCandidate.getPlanRemaining()
          .getResource();
      Resource headroomInEveryNode = rc.divideAndCeil(headroom,
          singleTypeNMCandidate.getCount());
      long bucketsInExistingOneNode = rc.computeAvailableContainers(
          headroomInEveryNode, containerRes);
      if (bucketsInExistingOneNode > 0) {
        int prev = count;
        // we can allocate #buckets such container in existing one node
        count -= bucketsInExistingOneNode;
        if (count < 0) {
          count = 0;
        }
        entry.setValue(count);
        // update existing node's headroom
        singleTypeNMCandidate.addPlanToUse(
            Resources.multiplyAndRoundUp(containerRes, prev));
      } else {
        return;
      }
    }
  }

  public DecommissionCandidates getDecommissionCandidates() {
    return decommissionCandidates;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public AutoScalerInfo getAutoScalerInfo() {
    return autoScalerInfo;
  }

  public NewNMCandidates getNewNMCandidates() {
    return newNMCandidates;
  }

  public String getConsideredResourceTypes() {
    return consideredResourceTypes;
  }
}
