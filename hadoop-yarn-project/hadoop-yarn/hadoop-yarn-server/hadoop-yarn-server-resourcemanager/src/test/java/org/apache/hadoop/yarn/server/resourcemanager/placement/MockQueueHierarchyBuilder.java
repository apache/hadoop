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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockQueueHierarchyBuilder {
  private static final String ROOT = "root";
  private static final String QUEUE_SEP = ".";
  private List<String> queuePaths = Lists.newArrayList();
  private List<String> managedParentQueues = Lists.newArrayList();
  private List<String> dynamicParentQueues = Lists.newArrayList();
  private Set<String> ambiguous = Sets.newHashSet();
  private Map<String, String> shortNameMapping = Maps.newHashMap();
  private CapacitySchedulerQueueManager queueManager;
  private Map<String, List<CSQueue>> childrenMap = Maps.newHashMap();

  public static MockQueueHierarchyBuilder create() {
    return new MockQueueHierarchyBuilder();
  }

  public MockQueueHierarchyBuilder withQueueManager(
      CapacitySchedulerQueueManager queueManager) {
    this.queueManager = queueManager;
    return this;
  }

  public MockQueueHierarchyBuilder withQueue(String queue) {
    this.queuePaths.add(queue);
    return this;
  }

  public MockQueueHierarchyBuilder withManagedParentQueue(
      String managedQueue) {
    this.managedParentQueues.add(managedQueue);
    return this;
  }

  public MockQueueHierarchyBuilder withDynamicParentQueue(
      String dynamicQueue) {
    this.dynamicParentQueues.add(dynamicQueue);
    return this;
  }

  public void build() {
    if (this.queueManager == null) {
      throw new IllegalStateException(
          "QueueManager instance is not provided!");
    }

    for (String managedParentQueue : managedParentQueues) {
      if (!queuePaths.contains(managedParentQueue)) {
        queuePaths.add(managedParentQueue);
      } else {
        throw new IllegalStateException("Cannot add a managed parent " +
            "and a simple queue with the same path");
      }
    }

    for (String dynamicParentQueue : dynamicParentQueues) {
      if (!queuePaths.contains(dynamicParentQueue)) {
        queuePaths.add(dynamicParentQueue);
      } else {
        throw new IllegalStateException("Cannot add a dynamic parent " +
            "and a simple queue with the same path");
      }
    }

    Map<String, AbstractCSQueue> queues = Maps.newHashMap();
    for (String queuePath : queuePaths) {
      addQueues(queues, queuePath);
    }

    ambiguous.forEach(queue -> {
      if (queue.equals("root")) {
        return;
      }
      when(queueManager.isAmbiguous(queue)).thenReturn(true);
      when(queueManager.getQueue(queue)).thenReturn(null);
    });
  }

  private void addQueues(Map<String, AbstractCSQueue> queues,
      String queuePath) {
    final String[] pathComponents = queuePath.split("\\" + QUEUE_SEP);

    String currentQueuePath = "";
    for (int i = 0; i < pathComponents.length; ++i) {
      boolean isLeaf = i == pathComponents.length - 1;
      String queueName = pathComponents[i];
      String parentPath = currentQueuePath;
      currentQueuePath += currentQueuePath.equals("") ?
          queueName : QUEUE_SEP + queueName;

      if (shortNameMapping.containsKey(queueName) &&
          !shortNameMapping.get(queueName).equals(currentQueuePath)) {
        ambiguous.add(queueName);
      }
      shortNameMapping.put(queueName, currentQueuePath);

      if (managedParentQueues.contains(parentPath) && !isLeaf) {
        throw new IllegalStateException("Cannot add a queue under " +
            "managed parent");
      }
      if (!queues.containsKey(currentQueuePath)) {
        ParentQueue parentQueue = (ParentQueue) queues.get(parentPath);
        AbstractCSQueue queue = createQueue(parentQueue, queueName,
            currentQueuePath, isLeaf);
        queues.put(currentQueuePath, queue);
      }
    }
  }

  private AbstractCSQueue createQueue(ParentQueue parentQueue,
      String queueName, String currentQueuePath, boolean isLeaf) {
    if (queueName.equals(ROOT)) {
      return createRootQueue(ROOT);
    } else if (managedParentQueues.contains(currentQueuePath)) {
      return addManagedParentQueueAsChildOf(parentQueue, queueName);
    } else if (dynamicParentQueues.contains(currentQueuePath)) {
      return addParentQueueAsChildOf(parentQueue, queueName, true);
    } else if (isLeaf) {
      return addLeafQueueAsChildOf(parentQueue, queueName);
    } else {
      return addParentQueueAsChildOf(parentQueue, queueName, false);
    }
  }

  private AbstractCSQueue createRootQueue(String rootQueueName) {
    ParentQueue root = mock(ParentQueue.class);
    when(root.getQueuePath()).thenReturn(rootQueueName);
    when(queueManager.getQueue(rootQueueName)).thenReturn(root);
    when(queueManager.getQueueByFullName(rootQueueName)).thenReturn(root);
    return root;
  }

  private AbstractCSQueue addParentQueueAsChildOf(ParentQueue parent,
      String queueName, boolean isDynamic) {
    ParentQueue queue = mock(ParentQueue.class);
    when(queue.isEligibleForAutoQueueCreation()).thenReturn(isDynamic);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private AbstractCSQueue addManagedParentQueueAsChildOf(ParentQueue parent,
      String queueName) {
    ManagedParentQueue queue = mock(ManagedParentQueue.class);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private AbstractCSQueue addLeafQueueAsChildOf(ParentQueue parent,
      String queueName) {
    LeafQueue queue = mock(LeafQueue.class);
    setQueueFields(parent, queue, queueName);
    return queue;
  }

  private void setQueueFields(ParentQueue parent, AbstractCSQueue newQueue,
      String queueName) {
    String fullPathOfParent = parent.getQueuePath();
    String fullPathOfQueue = fullPathOfParent + QUEUE_SEP + queueName;
    addQueueToQueueManager(queueName, newQueue, fullPathOfQueue);

    if (childrenMap.get(fullPathOfParent) == null) {
      childrenMap.put(fullPathOfParent, new ArrayList<>());
    }
    childrenMap.get(fullPathOfParent).add(newQueue);
    when(parent.getChildQueues()).thenReturn(childrenMap.get(fullPathOfParent));

    when(newQueue.getParent()).thenReturn(parent);
    when(newQueue.getQueuePath()).thenReturn(fullPathOfQueue);
    when(newQueue.getQueueName()).thenReturn(queueName);

  }

  private void addQueueToQueueManager(String queueName, AbstractCSQueue queue,
      String fullPathOfQueue) {
    when(queueManager.getQueue(queueName)).thenReturn(queue);
    when(queueManager.getQueue(fullPathOfQueue)).thenReturn(queue);
    when(queueManager.getQueueByFullName(fullPathOfQueue)).thenReturn(queue);
  }
}
