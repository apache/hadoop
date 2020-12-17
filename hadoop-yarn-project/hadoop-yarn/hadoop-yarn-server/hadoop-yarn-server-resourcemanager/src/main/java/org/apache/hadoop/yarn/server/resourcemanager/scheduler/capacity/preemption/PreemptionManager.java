/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PreemptionManager {
  private ReentrantReadWriteLock.ReadLock readLock;
  private ReentrantReadWriteLock.WriteLock writeLock;
  private Map<String, PreemptableQueue> entities = new HashMap<>();

  public PreemptionManager() {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public void refreshQueues(CSQueue parent, CSQueue current) {
    writeLock.lock();
    try {
      PreemptableQueue parentEntity = null;
      if (parent != null) {
        parentEntity = entities.get(parent.getQueuePath());
      }

      if (!entities.containsKey(current.getQueuePath())) {
        entities.put(current.getQueuePath(),
            new PreemptableQueue(parentEntity));
      }

      if (current.getChildQueues() != null) {
        for (CSQueue child : current.getChildQueues()) {
          refreshQueues(current, child);
        }
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  public void addKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.addKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  public void removeKillableContainer(KillableContainer container) {
    writeLock.lock();
    try {
      PreemptableQueue entity = entities.get(container.getLeafQueueName());
      if (null != entity) {
        entity.removeKillableContainer(container);
      }
    }
    finally {
      writeLock.unlock();
    }
  }

  public void moveKillableContainer(KillableContainer oldContainer,
      KillableContainer newContainer) {
    // TODO, will be called when partition of the node changed OR
    // container moved to different queue
  }

  public void updateKillableContainerResource(KillableContainer container,
      Resource oldResource, Resource newResource) {
    // TODO, will be called when container's resource changed
  }

  @VisibleForTesting
  public Map<ContainerId, RMContainer> getKillableContainersMap(
      String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Map<ContainerId, RMContainer> containers =
            entity.getKillableContainers().get(partition);
        if (containers != null) {
          return containers;
        }
      }
      return Collections.emptyMap();
    }
    finally {
      readLock.unlock();
    }
  }

  public Iterator<RMContainer> getKillableContainers(String queueName,
      String partition) {
    return getKillableContainersMap(queueName, partition).values().iterator();
  }

  public Resource getKillableResource(String queueName, String partition) {
    readLock.lock();
    try {
      PreemptableQueue entity = entities.get(queueName);
      if (entity != null) {
        Resource res = entity.getTotalKillableResources().get(partition);
        if (res == null || res.equals(Resources.none())) {
          return Resources.none();
        }
        return Resources.clone(res);
      }
      return Resources.none();
    }
    finally {
      readLock.unlock();
    }
  }

  public Map<String, PreemptableQueue> getShallowCopyOfPreemptableQueues() {
    readLock.lock();
    try {
      Map<String, PreemptableQueue> map = new HashMap<>();
      for (Map.Entry<String, PreemptableQueue> entry : entities.entrySet()) {
        String key = entry.getKey();
        PreemptableQueue entity = entry.getValue();
        map.put(key, new PreemptableQueue(
            new HashMap<>(entity.getTotalKillableResources()),
            new HashMap<>(entity.getKillableContainers())));
      }
      return map;
    } finally {
      readLock.unlock();
    }
  }
}
