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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProportionalCapacityPreemptionPolicyMockFramework {
  static final Log LOG =
      LogFactory.getLog(TestProportionalCapacityPreemptionPolicyForNodePartitions.class);
  final String ROOT = CapacitySchedulerConfiguration.ROOT;

  Map<String, CSQueue> nameToCSQueues = null;
  Map<String, Resource> partitionToResource = null;
  Map<NodeId, FiCaSchedulerNode> nodeIdToSchedulerNodes = null;
  RMNodeLabelsManager nlm = null;
  RMContext rmContext = null;

  ResourceCalculator rc = new DefaultResourceCalculator();
  Clock mClock = null;
  CapacitySchedulerConfiguration conf = null;
  CapacityScheduler cs = null;
  EventHandler<SchedulerEvent> mDisp = null;
  ProportionalCapacityPreemptionPolicy policy = null;
  Resource clusterResource = null;

  @SuppressWarnings("unchecked")
  @Before
  public void setup() {
    org.apache.log4j.Logger.getRootLogger().setLevel(
        org.apache.log4j.Level.DEBUG);

    conf = new CapacitySchedulerConfiguration(new Configuration(false));
    conf.setLong(
        CapacitySchedulerConfiguration.PREEMPTION_WAIT_TIME_BEFORE_KILL, 10000);
    conf.setLong(CapacitySchedulerConfiguration.PREEMPTION_MONITORING_INTERVAL,
        3000);
    // report "ideal" preempt
    conf.setFloat(CapacitySchedulerConfiguration.TOTAL_PREEMPTION_PER_ROUND,
        (float) 1.0);
    conf.setFloat(
        CapacitySchedulerConfiguration.PREEMPTION_NATURAL_TERMINATION_FACTOR,
        (float) 1.0);

    mClock = mock(Clock.class);
    cs = mock(CapacityScheduler.class);
    when(cs.getResourceCalculator()).thenReturn(rc);
    when(cs.getPreemptionManager()).thenReturn(new PreemptionManager());
    when(cs.getConfiguration()).thenReturn(conf);

    nlm = mock(RMNodeLabelsManager.class);
    mDisp = mock(EventHandler.class);

    rmContext = mock(RMContext.class);
    when(rmContext.getNodeLabelManager()).thenReturn(nlm);
    Dispatcher disp = mock(Dispatcher.class);
    when(rmContext.getDispatcher()).thenReturn(disp);
    when(disp.getEventHandler()).thenReturn(mDisp);
    when(cs.getRMContext()).thenReturn(rmContext);

    partitionToResource = new HashMap<>();
    nodeIdToSchedulerNodes = new HashMap<>();
    nameToCSQueues = new HashMap<>();
  }

  public void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig) throws IOException {
    buildEnv(labelsConfig, nodesConfig, queuesConfig, appsConfig, false);
  }

  public void buildEnv(String labelsConfig, String nodesConfig,
      String queuesConfig, String appsConfig,
      boolean useDominantResourceCalculator) throws IOException {
    if (useDominantResourceCalculator) {
      when(cs.getResourceCalculator()).thenReturn(
          new DominantResourceCalculator());
    }
    mockNodeLabelsManager(labelsConfig);
    mockSchedulerNodes(nodesConfig);
    for (NodeId nodeId : nodeIdToSchedulerNodes.keySet()) {
      when(cs.getSchedulerNode(nodeId)).thenReturn(
          nodeIdToSchedulerNodes.get(nodeId));
    }
    List<FiCaSchedulerNode> allNodes = new ArrayList<>(
        nodeIdToSchedulerNodes.values());
    when(cs.getAllNodes()).thenReturn(allNodes);
    ParentQueue root = mockQueueHierarchy(queuesConfig);
    when(cs.getRootQueue()).thenReturn(root);
    when(cs.getClusterResource()).thenReturn(clusterResource);
    mockApplications(appsConfig);

    policy = new ProportionalCapacityPreemptionPolicy(rmContext, cs,
        mClock);
  }

  private void mockContainers(String containersConfig, FiCaSchedulerApp app,
      ApplicationAttemptId attemptId, String queueName,
      List<RMContainer> reservedContainers, List<RMContainer> liveContainers) {
    int containerId = 1;
    int start = containersConfig.indexOf("=") + 1;
    int end = -1;

    Resource used = Resource.newInstance(0, 0);
    Resource pending = Resource.newInstance(0, 0);
    Priority pri = Priority.newInstance(0);

    while (start < containersConfig.length()) {
      while (start < containersConfig.length()
          && containersConfig.charAt(start) != '(') {
        start++;
      }
      if (start >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error containers specification, line=" + containersConfig);
      }
      end = start + 1;
      while (end < containersConfig.length()
          && containersConfig.charAt(end) != ')') {
        end++;
      }
      if (end >= containersConfig.length()) {
        throw new IllegalArgumentException(
            "Error containers specification, line=" + containersConfig);
      }

      // now we found start/end, get container values
      String[] values = containersConfig.substring(start + 1, end).split(",");
      if (values.length < 6 || values.length > 8) {
        throw new IllegalArgumentException("Format to define container is:"
            + "(priority,resource,host,expression,repeat,reserved, pending)");
      }
      pri.setPriority(Integer.valueOf(values[0]));
      Resource res = parseResourceFromString(values[1]);
      NodeId host = NodeId.newInstance(values[2], 1);
      String label = values[3];
      String userName = "user";
      int repeat = Integer.valueOf(values[4]);
      boolean reserved = Boolean.valueOf(values[5]);
      if (values.length >= 7) {
        Resources.addTo(pending, parseResourceFromString(values[6]));
      }
      if (values.length == 8) {
        userName = values[7];
      }

      for (int i = 0; i < repeat; i++) {
        Container c = mock(Container.class);
        Resources.addTo(used, res);
        when(c.getResource()).thenReturn(res);
        when(c.getPriority()).thenReturn(pri);
        SchedulerRequestKey sk = SchedulerRequestKey.extractFrom(c);
        RMContainerImpl rmc = mock(RMContainerImpl.class);
        when(rmc.getAllocatedSchedulerKey()).thenReturn(sk);
        when(rmc.getAllocatedNode()).thenReturn(host);
        when(rmc.getNodeLabelExpression()).thenReturn(label);
        when(rmc.getAllocatedResource()).thenReturn(res);
        when(rmc.getContainer()).thenReturn(c);
        when(rmc.getApplicationAttemptId()).thenReturn(attemptId);
        when(rmc.getQueueName()).thenReturn(queueName);
        final ContainerId cId = ContainerId.newContainerId(attemptId,
            containerId);
        when(rmc.getContainerId()).thenReturn(cId);
        doAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) throws Throwable {
            return cId.compareTo(
                ((RMContainer) invocation.getArguments()[0]).getContainerId());
          }
        }).when(rmc).compareTo(any(RMContainer.class));

        if (containerId == 1) {
          when(rmc.isAMContainer()).thenReturn(true);
          when(app.getAMResource(label)).thenReturn(res);
        }

        if (reserved) {
          reservedContainers.add(rmc);
          when(rmc.getReservedResource()).thenReturn(res);
        } else {
          liveContainers.add(rmc);
        }

        // Add container to scheduler-node
        addContainerToSchedulerNode(host, rmc, reserved);

        // If this is a non-exclusive allocation
        String partition = null;
        if (label.isEmpty()
            && !(partition = nodeIdToSchedulerNodes.get(host).getPartition())
                .isEmpty()) {
          LeafQueue queue = (LeafQueue) nameToCSQueues.get(queueName);
          Map<String, TreeSet<RMContainer>> ignoreExclusivityContainers = queue
              .getIgnoreExclusivityRMContainers();
          if (!ignoreExclusivityContainers.containsKey(partition)) {
            ignoreExclusivityContainers.put(partition,
                new TreeSet<RMContainer>());
          }
          ignoreExclusivityContainers.get(partition).add(rmc);
        }
        LOG.debug("add container to app=" + attemptId + " res=" + res + " node="
            + host + " nodeLabelExpression=" + label + " partition="
            + partition);

        containerId++;
      }

      // Some more app specific aggregated data can be better filled here.
      when(app.getPriority()).thenReturn(pri);
      when(app.getUser()).thenReturn(userName);
      when(app.getCurrentConsumption()).thenReturn(used);
      when(app.getCurrentReservation())
          .thenReturn(Resources.createResource(0, 0));

      Map<String, Resource> pendingForDefaultPartition =
          new HashMap<String, Resource>();
      // Add for default partition for now.
      pendingForDefaultPartition.put(label, pending);
      when(app.getTotalPendingRequestsPerPartition())
          .thenReturn(pendingForDefaultPartition);

      // need to set pending resource in resource usage as well
      ResourceUsage ru = new ResourceUsage();
      ru.setUsed(label, used);
      when(app.getAppAttemptResourceUsage()).thenReturn(ru);

      start = end + 1;
    }
  }

  /**
   * Format is:
   * <pre>
   * queueName\t  // app1
   * (priority,resource,host,expression,#repeat,reserved)
   * (priority,resource,host,expression,#repeat,reserved);
   * queueName\t  // app2
   * </pre>
   */
  private void mockApplications(String appsConfig) {
    int id = 1;
    HashMap<String, HashSet<String>> userMap = new HashMap<String, HashSet<String>>();
    LeafQueue queue = null;
    for (String a : appsConfig.split(";")) {
      String[] strs = a.split("\t");
      String queueName = strs[0];

      // get containers
      List<RMContainer> liveContainers = new ArrayList<RMContainer>();
      List<RMContainer> reservedContainers = new ArrayList<RMContainer>();
      ApplicationId appId = ApplicationId.newInstance(0L, id);
      ApplicationAttemptId appAttemptId = ApplicationAttemptId
          .newInstance(appId, 1);

      FiCaSchedulerApp app = mock(FiCaSchedulerApp.class);
      when(app.getAMResource(anyString()))
          .thenReturn(Resources.createResource(0, 0));
      mockContainers(strs[1], app, appAttemptId, queueName, reservedContainers,
          liveContainers);
      LOG.debug("Application mock: queue: " + queueName + ", appId:" + appId);

      when(app.getLiveContainers()).thenReturn(liveContainers);
      when(app.getReservedContainers()).thenReturn(reservedContainers);
      when(app.getApplicationAttemptId()).thenReturn(appAttemptId);
      when(app.getApplicationId()).thenReturn(appId);

      // add to LeafQueue
      queue = (LeafQueue) nameToCSQueues.get(queueName);
      queue.getApplications().add(app);
      queue.getAllApplications().add(app);

      HashSet<String> users = userMap.get(queueName);
      if (null == users) {
        users = new HashSet<String>();
        userMap.put(queueName, users);
      }

      users.add(app.getUser());
      id++;
    }

    for (String queueName : userMap.keySet()) {
      queue = (LeafQueue) nameToCSQueues.get(queueName);
      // Currently we have user-limit test support only for default label.
      Resource totResoucePerPartition = partitionToResource.get("");
      Resource capacity = Resources.multiply(totResoucePerPartition,
          queue.getQueueCapacities().getAbsoluteCapacity());
      HashSet<String> users = userMap.get(queue.getQueueName());
      Resource userLimit = Resources.divideAndCeil(rc, capacity, users.size());
      for (String user : users) {
        when(queue.getUserLimitPerUser(eq(user), any(Resource.class),
            anyString())).thenReturn(userLimit);
      }
    }
  }

  private void addContainerToSchedulerNode(NodeId nodeId, RMContainer container,
      boolean isReserved) {
    SchedulerNode node = nodeIdToSchedulerNodes.get(nodeId);
    assert node != null;

    if (isReserved) {
      when(node.getReservedContainer()).thenReturn(container);
    } else {
      node.getCopiedListOfRunningContainers().add(container);
      Resources.subtractFrom(node.getUnallocatedResource(),
          container.getAllocatedResource());
    }
  }

  /**
   * Format is:
   * host1=partition[ res=resource];
   * host2=partition[ res=resource];
   */
  private void mockSchedulerNodes(String schedulerNodesConfigStr)
      throws IOException {
    String[] nodesConfigStrArray = schedulerNodesConfigStr.split(";");
    for (String p : nodesConfigStrArray) {
      String[] arr = p.split(" ");

      NodeId nodeId = NodeId.newInstance(arr[0].substring(0, arr[0].indexOf("=")), 1);
      String partition = arr[0].substring(arr[0].indexOf("=") + 1, arr[0].length());

      FiCaSchedulerNode sn = mock(FiCaSchedulerNode.class);
      when(sn.getNodeID()).thenReturn(nodeId);
      when(sn.getPartition()).thenReturn(partition);

      Resource totalRes = Resources.createResource(0);
      if (arr.length > 1) {
        String res = arr[1];
        if (res.contains("res=")) {
          String resSring = res.substring(
              res.indexOf("res=") + "res=".length());
          totalRes = parseResourceFromString(resSring);
        }
      }
      when(sn.getTotalResource()).thenReturn(totalRes);
      when(sn.getUnallocatedResource()).thenReturn(Resources.clone(totalRes));

      // TODO, add settings of killable resources when necessary
      when(sn.getTotalKillableResources()).thenReturn(Resources.none());

      List<RMContainer> liveContainers = new ArrayList<>();
      when(sn.getCopiedListOfRunningContainers()).thenReturn(liveContainers);

      nodeIdToSchedulerNodes.put(nodeId, sn);

      LOG.debug("add scheduler node, id=" + nodeId + ", partition=" + partition);
    }
  }

  /**
   * Format is:
   * <pre>
   * partition0=total_resource,exclusivity;
   * partition1=total_resource,exclusivity;
   * ...
   * </pre>
   */
  private void mockNodeLabelsManager(String nodeLabelsConfigStr) throws IOException {
    String[] partitionConfigArr = nodeLabelsConfigStr.split(";");
    clusterResource = Resources.createResource(0);
    for (String p : partitionConfigArr) {
      String partitionName = p.substring(0, p.indexOf("="));
      Resource res = parseResourceFromString(p.substring(p.indexOf("=") + 1,
          p.indexOf(",")));
      boolean exclusivity =
          Boolean.valueOf(p.substring(p.indexOf(",") + 1, p.length()));
      when(nlm.getResourceByLabel(eq(partitionName), any(Resource.class)))
          .thenReturn(res);
      when(nlm.isExclusiveNodeLabel(eq(partitionName))).thenReturn(exclusivity);

      // add to partition to resource
      partitionToResource.put(partitionName, res);
      LOG.debug("add partition=" + partitionName + " totalRes=" + res
          + " exclusivity=" + exclusivity);
      Resources.addTo(clusterResource, res);
    }

    when(nlm.getClusterNodeLabelNames()).thenReturn(
        partitionToResource.keySet());
  }

  private Resource parseResourceFromString(String p) {
    String[] resource = p.split(":");
    Resource res;
    if (resource.length == 1) {
      res = Resources.createResource(Integer.valueOf(resource[0]));
    } else {
      res = Resources.createResource(Integer.valueOf(resource[0]),
          Integer.valueOf(resource[1]));
    }
    return res;
  }

  /**
   * Format is:
   * <pre>
   * root (<partition-name-1>=[guaranteed max used pending (reserved)],<partition-name-2>=..);
   * -A(...);
   * --A1(...);
   * --A2(...);
   * -B...
   * </pre>
   * ";" splits queues, and there should no empty lines, no extra spaces
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private ParentQueue mockQueueHierarchy(String queueExprs) {
    String[] queueExprArray = queueExprs.split(";");
    ParentQueue rootQueue = null;
    for (int idx = 0; idx < queueExprArray.length; idx++) {
      String q = queueExprArray[idx];
      CSQueue queue;

      // Initialize queue
      if (isParent(queueExprArray, idx)) {
        ParentQueue parentQueue = mock(ParentQueue.class);
        queue = parentQueue;
        List<CSQueue> children = new ArrayList<CSQueue>();
        when(parentQueue.getChildQueues()).thenReturn(children);
      } else {
        LeafQueue leafQueue = mock(LeafQueue.class);
        final TreeSet<FiCaSchedulerApp> apps = new TreeSet<>(
            new Comparator<FiCaSchedulerApp>() {
              @Override
              public int compare(FiCaSchedulerApp a1, FiCaSchedulerApp a2) {
                if (a1.getPriority() != null
                    && !a1.getPriority().equals(a2.getPriority())) {
                  return a1.getPriority().compareTo(a2.getPriority());
                }

                int res = a1.getApplicationId()
                    .compareTo(a2.getApplicationId());
                return res;
              }
            });
        when(leafQueue.getApplications()).thenReturn(apps);
        when(leafQueue.getAllApplications()).thenReturn(apps);
        OrderingPolicy<FiCaSchedulerApp> so = mock(OrderingPolicy.class);
        when(so.getPreemptionIterator()).thenAnswer(new Answer() {
          public Object answer(InvocationOnMock invocation) {
            return apps.descendingIterator();
          }
        });
        when(leafQueue.getOrderingPolicy()).thenReturn(so);

        Map<String, TreeSet<RMContainer>> ignorePartitionContainers =
            new HashMap<>();
        when(leafQueue.getIgnoreExclusivityRMContainers()).thenReturn(
            ignorePartitionContainers);
        queue = leafQueue;
      }

      setupQueue(queue, q, queueExprArray, idx);
      if (queue.getQueueName().equals(ROOT)) {
        rootQueue = (ParentQueue) queue;
      }
    }
    return rootQueue;
  }

  private void setupQueue(CSQueue queue, String q, String[] queueExprArray,
      int idx) {
    LOG.debug("*** Setup queue, source=" + q);
    String queuePath = null;

    int myLevel = getLevel(q);
    if (0 == myLevel) {
      // It's root
      when(queue.getQueueName()).thenReturn(ROOT);
      queuePath = ROOT;
    }

    String queueName = getQueueName(q);
    when(queue.getQueueName()).thenReturn(queueName);

    // Setup parent queue, and add myself to parentQueue.children-list
    ParentQueue parentQueue = getParentQueue(queueExprArray, idx, myLevel);
    if (null != parentQueue) {
      when(queue.getParent()).thenReturn(parentQueue);
      parentQueue.getChildQueues().add(queue);

      // Setup my path
      queuePath = parentQueue.getQueuePath() + "." + queueName;
    }
    when(queue.getQueuePath()).thenReturn(queuePath);

    QueueCapacities qc = new QueueCapacities(0 == myLevel);
    ResourceUsage ru = new ResourceUsage();

    when(queue.getQueueCapacities()).thenReturn(qc);
    when(queue.getQueueResourceUsage()).thenReturn(ru);

    LOG.debug("Setup queue, name=" + queue.getQueueName() + " path="
        + queue.getQueuePath());
    LOG.debug("Parent=" + (parentQueue == null ? "null" : parentQueue
        .getQueueName()));

    // Setup other fields like used resource, guaranteed resource, etc.
    String capacitySettingStr = q.substring(q.indexOf("(") + 1, q.indexOf(")"));
    for (String s : capacitySettingStr.split(",")) {
      String partitionName = s.substring(0, s.indexOf("="));
      String[] values = s.substring(s.indexOf("[") + 1, s.indexOf("]")).split(" ");
      // Add a small epsilon to capacities to avoid truncate when doing
      // Resources.multiply
      float epsilon = 1e-6f;
      Resource totResoucePerPartition = partitionToResource.get(partitionName);
      float absGuaranteed = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[0].trim()), totResoucePerPartition)
          + epsilon;
      float absMax = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[1].trim()), totResoucePerPartition)
          + epsilon;
      float absUsed = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[2].trim()), totResoucePerPartition)
          + epsilon;
      float used = Resources.divide(rc, totResoucePerPartition,
          parseResourceFromString(values[2].trim()),
          parseResourceFromString(values[0].trim())) + epsilon;
      Resource pending = parseResourceFromString(values[3].trim());
      qc.setAbsoluteCapacity(partitionName, absGuaranteed);
      qc.setAbsoluteMaximumCapacity(partitionName, absMax);
      qc.setAbsoluteUsedCapacity(partitionName, absUsed);
      qc.setUsedCapacity(partitionName, used);
      when(queue.getUsedCapacity()).thenReturn(used);
      ru.setPending(partitionName, pending);
      if (!isParent(queueExprArray, idx)) {
        LeafQueue lq = (LeafQueue) queue;
        when(lq.getTotalPendingResourcesConsideringUserLimit(isA(Resource.class),
            isA(String.class))).thenReturn(pending);
      }
      ru.setUsed(partitionName, parseResourceFromString(values[2].trim()));

      // Setup reserved resource if it contained by input config
      Resource reserved = Resources.none();
      if(values.length == 5) {
        reserved = parseResourceFromString(values[4].trim());
        ru.setReserved(partitionName, reserved);
      }

      LOG.debug("Setup queue=" + queueName + " partition=" + partitionName
          + " [abs_guaranteed=" + absGuaranteed + ",abs_max=" + absMax
          + ",abs_used" + absUsed + ",pending_resource=" + pending
          + ", reserved_resource=" + reserved + "]");
    }

    // Setup preemption disabled
    when(queue.getPreemptionDisabled()).thenReturn(
        conf.getPreemptionDisabled(queuePath, false));

    nameToCSQueues.put(queueName, queue);
    when(cs.getQueue(eq(queueName))).thenReturn(queue);
  }

  /**
   * Level of a queue is how many "-" at beginning, root's level is 0
   */
  private int getLevel(String q) {
    int level = 0; // level = how many "-" at beginning
    while (level < q.length() && q.charAt(level) == '-') {
      level++;
    }
    return level;
  }

  private String getQueueName(String q) {
    int idx = 0;
    // find first != '-' char
    while (idx < q.length() && q.charAt(idx) == '-') {
      idx++;
    }
    if (idx == q.length()) {
      throw new IllegalArgumentException("illegal input:" + q);
    }
    // name = after '-' and before '('
    String name = q.substring(idx, q.indexOf('('));
    if (name.isEmpty()) {
      throw new IllegalArgumentException("queue name shouldn't be empty:" + q);
    }
    if (name.contains(".")) {
      throw new IllegalArgumentException("queue name shouldn't contain '.':"
          + name);
    }
    return name;
  }

  private ParentQueue getParentQueue(String[] queueExprArray, int idx, int myLevel) {
    idx--;
    while (idx >= 0) {
      int level = getLevel(queueExprArray[idx]);
      if (level < myLevel) {
        String parentQueuName = getQueueName(queueExprArray[idx]);
        return (ParentQueue) nameToCSQueues.get(parentQueuName);
      }
      idx--;
    }

    return null;
  }

  /**
   * Get if a queue is ParentQueue
   */
  private boolean isParent(String[] queues, int idx) {
    int myLevel = getLevel(queues[idx]);
    idx++;
    while (idx < queues.length && getLevel(queues[idx]) == myLevel) {
      idx++;
    }
    if (idx >= queues.length || getLevel(queues[idx]) < myLevel) {
      // It's a LeafQueue
      return false;
    } else {
      return true;
    }
  }

  public ApplicationAttemptId getAppAttemptId(int id) {
    ApplicationId appId = ApplicationId.newInstance(0L, id);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    return appAttemptId;
  }

  public void checkContainerNodesInApp(FiCaSchedulerApp app,
      int expectedContainersNumber, String host) {
    NodeId nodeId = NodeId.newInstance(host, 1);
    int num = 0;
    for (RMContainer c : app.getLiveContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    for (RMContainer c : app.getReservedContainers()) {
      if (c.getAllocatedNode().equals(nodeId)) {
        num++;
      }
    }
    Assert.assertEquals(expectedContainersNumber, num);
  }

  public FiCaSchedulerApp getApp(String queueName, int appId) {
    for (FiCaSchedulerApp app : ((LeafQueue) cs.getQueue(queueName))
        .getApplications()) {
      if (app.getApplicationId().getId() == appId) {
        return app;
      }
    }
    return null;
  }

  public void checkAbsCapacities(CSQueue queue, String partition,
      float guaranteed, float max, float used) {
    QueueCapacities qc = queue.getQueueCapacities();
    Assert.assertEquals(guaranteed, qc.getAbsoluteCapacity(partition), 1e-3);
    Assert.assertEquals(max, qc.getAbsoluteMaximumCapacity(partition), 1e-3);
    Assert.assertEquals(used, qc.getAbsoluteUsedCapacity(partition), 1e-3);
  }

  public void checkPendingResource(CSQueue queue, String partition, int pending) {
    ResourceUsage ru = queue.getQueueResourceUsage();
    Assert.assertEquals(pending, ru.getPending(partition).getMemorySize());
  }

  public void checkReservedResource(CSQueue queue, String partition, int reserved) {
    ResourceUsage ru = queue.getQueueResourceUsage();
    Assert.assertEquals(reserved, ru.getReserved(partition).getMemorySize());
  }

  static class IsPreemptionRequestForQueueAndNode
      extends ArgumentMatcher<ContainerPreemptEvent> {
    private final ApplicationAttemptId appAttId;
    private final String queueName;
    private final NodeId nodeId;

    IsPreemptionRequestForQueueAndNode(ApplicationAttemptId appAttId,
        String queueName, NodeId nodeId) {
      this.appAttId = appAttId;
      this.queueName = queueName;
      this.nodeId = nodeId;
    }
    @Override
    public boolean matches(Object o) {
      ContainerPreemptEvent cpe = (ContainerPreemptEvent)o;

      return appAttId.equals(cpe.getAppId())
          && queueName.equals(cpe.getContainer().getQueueName())
          && nodeId.equals(cpe.getContainer().getAllocatedNode());
    }
    @Override
    public String toString() {
      return appAttId.toString();
    }
  }
}
