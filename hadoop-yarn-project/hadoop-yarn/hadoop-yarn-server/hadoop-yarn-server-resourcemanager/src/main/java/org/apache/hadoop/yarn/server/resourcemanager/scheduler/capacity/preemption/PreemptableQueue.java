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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class PreemptableQueue {
  // Partition -> killable resources and containers
  private Map<String, Resource> totalKillableResources = new HashMap<>();
  private Map<String, Map<ContainerId, RMContainer>> killableContainers =
      new HashMap<>();
  private PreemptableQueue parent;

  public PreemptableQueue(PreemptableQueue parent) {
    this.parent = parent;
  }

  public PreemptableQueue(Map<String, Resource> totalKillableResources,
      Map<String, Map<ContainerId, RMContainer>> killableContainers) {
    this.totalKillableResources = totalKillableResources;
    this.killableContainers = killableContainers;
  }

  void addKillableContainer(KillableContainer container) {
    String partition = container.getNodePartition();
    if (!totalKillableResources.containsKey(partition)) {
      totalKillableResources.put(partition, Resources.createResource(0));
      killableContainers.put(partition,
          new ConcurrentSkipListMap<ContainerId, RMContainer>());
    }

    RMContainer c = container.getRMContainer();
    Resources.addTo(totalKillableResources.get(partition),
        c.getAllocatedResource());
    killableContainers.get(partition).put(c.getContainerId(), c);

    if (null != parent) {
      parent.addKillableContainer(container);
    }
  }

  void removeKillableContainer(KillableContainer container) {
    String partition = container.getNodePartition();
    Map<ContainerId, RMContainer> partitionKillableContainers =
        killableContainers.get(partition);
    if (partitionKillableContainers != null) {
      RMContainer rmContainer = partitionKillableContainers.remove(
          container.getRMContainer().getContainerId());
      if (null != rmContainer) {
        Resources.subtractFrom(totalKillableResources.get(partition),
            rmContainer.getAllocatedResource());
      }
    }

    if (null != parent) {
      parent.removeKillableContainer(container);
    }
  }

  public Resource getKillableResource(String partition) {
    Resource res = totalKillableResources.get(partition);
    return res == null ? Resources.none() : res;
  }

  public Map<String, Map<ContainerId, RMContainer>> getKillableContainers() {
    return killableContainers;
  }

  Map<String, Resource> getTotalKillableResources() {
    return totalKillableResources;
  }
}
