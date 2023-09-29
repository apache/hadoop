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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * NumaResourceAllocation contains Memory nodes and CPU nodes assigned to a
 * container.
 */
public class NumaResourceAllocation implements Serializable {
  private static final long serialVersionUID = 6339719798446595123L;
  private final ImmutableMap<String, Long> nodeVsMemory;
  private final ImmutableMap<String, Integer> nodeVsCpus;

  public NumaResourceAllocation(Map<String, Long> memoryAllocations,
      Map<String, Integer> cpuAllocations) {
    nodeVsMemory = ImmutableMap.copyOf(memoryAllocations);
    nodeVsCpus = ImmutableMap.copyOf(cpuAllocations);
  }

  public NumaResourceAllocation(String memNodeId, long memory, String cpuNodeId,
      int cpus) {
    this(ImmutableMap.of(memNodeId, memory), ImmutableMap.of(cpuNodeId, cpus));
  }

  public Set<String> getMemNodes() {
    return nodeVsMemory.keySet();
  }

  public Set<String> getCpuNodes() {
    return nodeVsCpus.keySet();
  }

  public ImmutableMap<String, Long> getNodeVsMemory() {
    return nodeVsMemory;
  }

  public ImmutableMap<String, Integer> getNodeVsCpus() {
    return nodeVsCpus;
  }

  @Override
  public String toString() {
    return "NumaResourceAllocation{" +
        "nodeVsMemory=" + nodeVsMemory +
        ", nodeVsCpus=" + nodeVsCpus +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NumaResourceAllocation that = (NumaResourceAllocation) o;
    return Objects.equals(nodeVsMemory, that.nodeVsMemory) &&
        Objects.equals(nodeVsCpus, that.nodeVsCpus);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeVsMemory, nodeVsCpus);
  }
}