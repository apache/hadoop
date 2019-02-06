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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * NumaResourceAllocation contains Memory nodes and CPU nodes assigned to a
 * container.
 */
public class NumaResourceAllocation implements Serializable {
  private static final long serialVersionUID = 6339719798446595123L;
  private Map<String, Long> nodeVsMemory;
  private Map<String, Integer> nodeVsCpus;

  public NumaResourceAllocation() {
    nodeVsMemory = new HashMap<>();
    nodeVsCpus = new HashMap<>();
  }

  public NumaResourceAllocation(String memNodeId, long memory, String cpuNodeId,
      int cpus) {
    this();
    nodeVsMemory.put(memNodeId, memory);
    nodeVsCpus.put(cpuNodeId, cpus);
  }

  public void addMemoryNode(String memNodeId, long memory) {
    nodeVsMemory.put(memNodeId, memory);
  }

  public void addCpuNode(String cpuNodeId, int cpus) {
    nodeVsCpus.put(cpuNodeId, cpus);
  }

  public Set<String> getMemNodes() {
    return nodeVsMemory.keySet();
  }

  public Set<String> getCpuNodes() {
    return nodeVsCpus.keySet();
  }

  public Map<String, Long> getNodeVsMemory() {
    return nodeVsMemory;
  }

  public Map<String, Integer> getNodeVsCpus() {
    return nodeVsCpus;
  }
}
