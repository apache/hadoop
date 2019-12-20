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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * NumaNodeResource class holds the NUMA node topology with the total and used
 * resources.
 */
public class NumaNodeResource {
  private String nodeId;
  private long totalMemory;
  private int totalCpus;
  private long usedMemory;
  private int usedCpus;

  private static final Logger LOG = LoggerFactory.
      getLogger(NumaNodeResource.class);

  private Map<ContainerId, Long> containerVsMemUsage =
      new ConcurrentHashMap<>();
  private Map<ContainerId, Integer> containerVsCpusUsage =
      new ConcurrentHashMap<>();

  public NumaNodeResource(String nodeId, long totalMemory, int totalCpus) {
    this.nodeId = nodeId;
    this.totalMemory = totalMemory;
    this.totalCpus = totalCpus;
  }

  /**
   * Checks whether the specified resources available or not.
   *
   * @param resource resource
   * @return whether the specified resources available or not
   */
  public boolean isResourcesAvailable(Resource resource) {
    LOG.debug(
        "Memory available:" + (totalMemory - usedMemory) + ", CPUs available:"
            + (totalCpus - usedCpus) + ", requested:" + resource);
    if ((totalMemory - usedMemory) >= resource.getMemorySize()
        && (totalCpus - usedCpus) >= resource.getVirtualCores()) {
      return true;
    }
    return false;
  }

  /**
   * Assigns available memory and returns the remaining needed memory.
   *
   * @param memreq required memory
   * @param containerId which container memory to assign
   * @return remaining needed memory
   */
  public long assignAvailableMemory(long memreq, ContainerId containerId) {
    long memAvailable = totalMemory - usedMemory;
    if (memAvailable >= memreq) {
      containerVsMemUsage.put(containerId, memreq);
      usedMemory += memreq;
      return 0;
    } else {
      usedMemory += memAvailable;
      containerVsMemUsage.put(containerId, memAvailable);
      return memreq - memAvailable;
    }
  }

  /**
   * Assigns available cpu's and returns the remaining needed cpu's.
   *
   * @param cpusreq required cpu's
   * @param containerId which container cpu's to assign
   * @return remaining needed cpu's
   */
  public int assignAvailableCpus(int cpusreq, ContainerId containerId) {
    int cpusAvailable = totalCpus - usedCpus;
    if (cpusAvailable >= cpusreq) {
      containerVsCpusUsage.put(containerId, cpusreq);
      usedCpus += cpusreq;
      return 0;
    } else {
      usedCpus += cpusAvailable;
      containerVsCpusUsage.put(containerId, cpusAvailable);
      return cpusreq - cpusAvailable;
    }
  }

  /**
   * Assigns the requested resources for Container.
   *
   * @param resource resource to assign
   * @param containerId to which container the resources to assign
   */
  public void assignResources(Resource resource, ContainerId containerId) {
    containerVsMemUsage.put(containerId, resource.getMemorySize());
    containerVsCpusUsage.put(containerId, resource.getVirtualCores());
    usedMemory += resource.getMemorySize();
    usedCpus += resource.getVirtualCores();
  }

  /**
   * Releases the assigned resources for Container.
   *
   * @param containerId to which container the assigned resources to release
   */
  public void releaseResources(ContainerId containerId) {
    if (containerVsMemUsage.containsKey(containerId)) {
      usedMemory -= containerVsMemUsage.get(containerId);
      containerVsMemUsage.remove(containerId);
    }
    if (containerVsCpusUsage.containsKey(containerId)) {
      usedCpus -= containerVsCpusUsage.get(containerId);
      containerVsCpusUsage.remove(containerId);
    }
  }

  /**
   * Recovers the memory resources for Container.
   *
   * @param containerId recover the memory resources for the Container
   * @param memory memory to recover
   */
  public void recoverMemory(ContainerId containerId, long memory) {
    containerVsMemUsage.put(containerId, memory);
    usedMemory += memory;
  }

  /**
   * Recovers the cpu's resources for Container.
   *
   * @param containerId recover the cpu's resources for the Container
   * @param cpus cpu's to recover
   */
  public void recoverCpus(ContainerId containerId, int cpus) {
    containerVsCpusUsage.put(containerId, cpus);
    usedCpus += cpus;
  }

  @Override
  public String toString() {
    return "Node Id:" + nodeId + "\tMemory:" + totalMemory + "\tCPus:"
        + totalCpus;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
    result = prime * result + (int) (totalMemory ^ (totalMemory >>> 32));
    result = prime * result + totalCpus;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    NumaNodeResource other = (NumaNodeResource) obj;
    if (nodeId == null) {
      if (other.nodeId != null) {
        return false;
      }
    } else if (!nodeId.equals(other.nodeId)) {
      return false;
    }
    if (totalMemory != other.totalMemory) {
      return false;
    }
    if (totalCpus != other.totalCpus) {
      return false;
    }
    return true;
  }

  public String getNodeId() {
    return nodeId;
  }
}