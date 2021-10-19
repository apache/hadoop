/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework.parseResourceFromString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockQueueHierarchy {
  private static final Logger LOG = LoggerFactory.getLogger(MockQueueHierarchy.class);
  private final String ROOT = CapacitySchedulerConfiguration.ROOT;
  private final ParentQueue rootQueue;
  private String config;
  private final CapacityScheduler cs;
  private CapacitySchedulerConfiguration conf;
  private final ResourceCalculator resourceCalculator;
  private final Map<String, CSQueue> nameToCSQueues;
  private final Map<String, Resource> partitionToResource;

  MockQueueHierarchy(String config,
      CapacityScheduler cs,
      CapacitySchedulerConfiguration conf,
      ResourceCalculator resourceCalculator,
      Map<String, Resource> partitionToResource) {
    this.config = config;
    this.cs = cs;
    this.conf = conf;
    this.resourceCalculator = resourceCalculator;
    this.nameToCSQueues = new HashMap<>();
    this.partitionToResource = partitionToResource;
    this.rootQueue = init();
  }

  public ParentQueue getRootQueue() {
    return rootQueue;
  }

  Map<String, CSQueue> getNameToCSQueues() {
    return nameToCSQueues;
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
   *
   * For each queue, it has configurations to specify capacities (to each
   * partition), format is:
   * <pre>
   * -<queueName> (<labelName1>=[guaranteed max used pending], \
   *               <labelName2>=[guaranteed max used pending])
   *              {key1=value1,key2=value2};  // Additional configs
   * </pre>
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private ParentQueue init() {
    String[] queueExprArray = config.split(";");
    ParentQueue rootQueue = null;
    for (int idx = 0; idx < queueExprArray.length; idx++) {
      String q = queueExprArray[idx];
      CSQueue queue;

      // Initialize queue
      if (isParent(queueExprArray, idx)) {
        ParentQueue parentQueue = mock(ParentQueue.class);
        queue = parentQueue;
        List<CSQueue> children = new ArrayList<>();
        when(parentQueue.getChildQueues()).thenReturn(children);
        QueueOrderingPolicy policy = mock(QueueOrderingPolicy.class);
        when(policy.getConfigName()).thenReturn(
            CapacitySchedulerConfiguration.QUEUE_PRIORITY_UTILIZATION_ORDERING_POLICY);
        when(parentQueue.getQueueOrderingPolicy()).thenReturn(policy);
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

                return a1.getApplicationId()
                    .compareTo(a2.getApplicationId());
              }
            });
        when(leafQueue.getApplications()).thenReturn(apps);
        when(leafQueue.getAllApplications()).thenReturn(apps);
        OrderingPolicy<FiCaSchedulerApp> so = mock(OrderingPolicy.class);
        String opName = conf.get(CapacitySchedulerConfiguration.PREFIX
            + CapacitySchedulerConfiguration.ROOT + "." + getQueueName(q)
            + ".ordering-policy", "fifo");
        if (opName.equals("fair")) {
          so = Mockito.spy(new FairOrderingPolicy<>());
        }
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

      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      when(queue.getReadLock()).thenReturn(lock.readLock());
      setupQueue(queue, q, queueExprArray, idx);
      if (queue.getQueuePath().equals(ROOT)) {
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
      when(queue.getQueuePath()).thenReturn(ROOT);
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
    QueueResourceQuotas qr  = new QueueResourceQuotas();

    when(queue.getQueueCapacities()).thenReturn(qc);
    when(queue.getQueueResourceUsage()).thenReturn(ru);
    when(queue.getQueueResourceQuotas()).thenReturn(qr);

    LOG.debug("Setup queue, short name=" + queue.getQueueName() + " path="
        + queue.getQueuePath());
    LOG.debug("Parent=" + (parentQueue == null ? "null" : parentQueue
        .getQueuePath()));

    // Setup other fields like used resource, guaranteed resource, etc.
    String capacitySettingStr = q.substring(q.indexOf("(") + 1, q.indexOf(")"));
    for (String s : capacitySettingStr.split(",")) {
      String partitionName = s.substring(0, s.indexOf("="));
      String[] values = s.substring(s.indexOf("[") + 1, s.indexOf("]")).split(" ");
      // Add a small epsilon to capacities to avoid truncate when doing
      // Resources.multiply
      float epsilon = 1e-6f;
      Resource toResourcePerPartition = partitionToResource.get(partitionName);
      float absGuaranteed = Resources.divide(resourceCalculator, toResourcePerPartition,
          parseResourceFromString(values[0].trim()), toResourcePerPartition)
          + epsilon;
      float absMax = Resources.divide(resourceCalculator, toResourcePerPartition,
          parseResourceFromString(values[1].trim()), toResourcePerPartition)
          + epsilon;
      float absUsed = Resources.divide(resourceCalculator, toResourcePerPartition,
          parseResourceFromString(values[2].trim()), toResourcePerPartition)
          + epsilon;
      float used = Resources.divide(resourceCalculator, toResourcePerPartition,
          parseResourceFromString(values[2].trim()),
          parseResourceFromString(values[0].trim())) + epsilon;
      Resource pending = parseResourceFromString(values[3].trim());
      qc.setAbsoluteCapacity(partitionName, absGuaranteed);
      qc.setAbsoluteMaximumCapacity(partitionName, absMax);
      qc.setAbsoluteUsedCapacity(partitionName, absUsed);
      qc.setUsedCapacity(partitionName, used);
      qr.setEffectiveMaxResource(parseResourceFromString(values[1].trim()));
      qr.setEffectiveMinResource(parseResourceFromString(values[0].trim()));
      qr.setEffectiveMaxResource(partitionName,
          parseResourceFromString(values[1].trim()));
      qr.setEffectiveMinResource(partitionName,
          parseResourceFromString(values[0].trim()));
      when(queue.getUsedCapacity()).thenReturn(used);
      when(queue.getEffectiveCapacity(partitionName))
          .thenReturn(parseResourceFromString(values[0].trim()));
      when(queue.getEffectiveMaxCapacity(partitionName))
          .thenReturn(parseResourceFromString(values[1].trim()));
      ru.setPending(partitionName, pending);
      // Setup reserved resource if it contained by input config
      Resource reserved = Resources.none();
      if(values.length == 5) {
        reserved = parseResourceFromString(values[4].trim());
        ru.setReserved(partitionName, reserved);
      }
      if (!isParent(queueExprArray, idx)) {
        LeafQueue lq = (LeafQueue) queue;
        when(lq.getTotalPendingResourcesConsideringUserLimit(isA(Resource.class),
            isA(String.class), eq(false))).thenReturn(pending);
        when(lq.getTotalPendingResourcesConsideringUserLimit(isA(Resource.class),
            isA(String.class), eq(true))).thenReturn(
            Resources.subtract(pending, reserved));
      }
      ru.setUsed(partitionName, parseResourceFromString(values[2].trim()));

      LOG.debug("Setup queue=" + queueName + " partition=" + partitionName
          + " [abs_guaranteed=" + absGuaranteed + ",abs_max=" + absMax
          + ",abs_used" + absUsed + ",pending_resource=" + pending
          + ", reserved_resource=" + reserved + "]");
    }

    // Setup preemption disabled
    when(queue.getPreemptionDisabled()).thenReturn(
        conf.getPreemptionDisabled(queuePath, false));

    // Setup other queue configurations
    Map<String, String> otherConfigs = getOtherConfigurations(
        queueExprArray[idx]);
    if (otherConfigs.containsKey("priority")) {
      when(queue.getPriority()).thenReturn(
          Priority.newInstance(Integer.valueOf(otherConfigs.get("priority"))));
    } else {
      // set queue's priority to 0 by default
      when(queue.getPriority()).thenReturn(Priority.newInstance(0));
    }

    // Setup disable preemption of queues
    if (otherConfigs.containsKey("disable_preemption")) {
      when(queue.getPreemptionDisabled()).thenReturn(
          Boolean.valueOf(otherConfigs.get("disable_preemption")));
    }

    //TODO: Refactor this test class to use queue path internally like CS
    // does from now on
    nameToCSQueues.put(queuePath, queue);
    nameToCSQueues.put(queueName, queue);
    when(cs.getQueue(eq(queuePath))).thenReturn(queue);
    when(cs.getQueue(eq(queueName))).thenReturn(queue);
    when(cs.normalizeQueueName(eq(queuePath))).thenReturn(queuePath);
    when(cs.normalizeQueueName(eq(queueName))).thenReturn(queuePath);
  }

  /**
   * Get additional queue's configurations
   * @param queueExpr queue expr
   * @return maps of configs
   */
  private Map<String, String> getOtherConfigurations(String queueExpr) {
    if (queueExpr.contains("{")) {
      int left = queueExpr.indexOf('{');
      int right = queueExpr.indexOf('}');

      if (right > left) {
        Map<String, String> configs = new HashMap<>();

        String subStr = queueExpr.substring(left + 1, right);
        for (String kv : subStr.split(",")) {
          if (kv.contains("=")) {
            String key = kv.substring(0, kv.indexOf("="));
            String value = kv.substring(kv.indexOf("=") + 1);
            configs.put(key, value);
          }
        }

        return configs;
      }
    }

    return Collections.emptyMap();
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
        String parentQueueName = getQueueName(queueExprArray[idx]);
        return (ParentQueue) nameToCSQueues.get(parentQueueName);
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

}
