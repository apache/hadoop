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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;

import com.google.common.annotations.VisibleForTesting;

/**
 * NUMA Resources Allocator reads the NUMA topology and assigns NUMA nodes to
 * the containers.
 */
public class NumaResourceAllocator {

  private static final Logger LOG = LoggerFactory.
      getLogger(NumaResourceAllocator.class);

  // Regex to find node ids, Ex: 'available: 2 nodes (0-1)'
  private static final String NUMA_NODEIDS_REGEX =
      "available:\\s*[0-9]+\\s*nodes\\s*\\(([0-9\\-,]*)\\)";

  // Regex to find node memory, Ex: 'node 0 size: 73717 MB'
  private static final String NUMA_NODE_MEMORY_REGEX =
      "node\\s*<NUMA-NODE>\\s*size:\\s*([0-9]+)\\s*([KMG]B)";

  // Regex to find node cpus, Ex: 'node 0 cpus: 0 2 4 6'
  private static final String NUMA_NODE_CPUS_REGEX =
      "node\\s*<NUMA-NODE>\\s*cpus:\\s*([0-9\\s]+)";

  private static final String GB = "GB";
  private static final String KB = "KB";
  private static final String NUMA_NODE = "<NUMA-NODE>";
  private static final String SPACE = "\\s";
  private static final long DEFAULT_NUMA_NODE_MEMORY = 1024;
  private static final int DEFAULT_NUMA_NODE_CPUS = 1;
  private static final String NUMA_RESOURCE_TYPE = "numa";

  private List<NumaNodeResource> numaNodesList = new ArrayList<>();
  private Map<String, NumaNodeResource> numaNodeIdVsResource = new HashMap<>();
  private int currentAssignNode;

  private Context context;

  public NumaResourceAllocator(Context context) {
    this.context = context;
  }

  public void init(Configuration conf) throws YarnException {
    if (conf.getBoolean(YarnConfiguration.NM_NUMA_AWARENESS_READ_TOPOLOGY,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_READ_TOPOLOGY)) {
      LOG.info("Reading NUMA topology using 'numactl --hardware' command.");
      String cmdOutput = executeNGetCmdOutput(conf);
      String[] outputLines = cmdOutput.split("\\n");
      Pattern pattern = Pattern.compile(NUMA_NODEIDS_REGEX);
      String nodeIdsStr = null;
      for (String line : outputLines) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
          nodeIdsStr = matcher.group(1);
          break;
        }
      }
      if (nodeIdsStr == null) {
        throw new YarnException("Failed to get numa nodes from"
            + " 'numactl --hardware' output and output is:\n" + cmdOutput);
      }
      String[] nodeIdCommaSplits = nodeIdsStr.split("[,\\s]");
      for (String nodeIdOrRange : nodeIdCommaSplits) {
        if (nodeIdOrRange.contains("-")) {
          String[] beginNEnd = nodeIdOrRange.split("-");
          int endNode = Integer.parseInt(beginNEnd[1]);
          for (int nodeId = Integer
              .parseInt(beginNEnd[0]); nodeId <= endNode; nodeId++) {
            long memory = parseMemory(outputLines, String.valueOf(nodeId));
            int cpus = parseCpus(outputLines, String.valueOf(nodeId));
            addToCollection(String.valueOf(nodeId), memory, cpus);
          }
        } else {
          long memory = parseMemory(outputLines, nodeIdOrRange);
          int cpus = parseCpus(outputLines, nodeIdOrRange);
          addToCollection(nodeIdOrRange, memory, cpus);
        }
      }
    } else {
      LOG.info("Reading NUMA topology using configurations.");
      Collection<String> nodeIds = conf
          .getStringCollection(YarnConfiguration.NM_NUMA_AWARENESS_NODE_IDS);
      for (String nodeId : nodeIds) {
        long mem = conf.getLong(
            "yarn.nodemanager.numa-awareness." + nodeId + ".memory",
            DEFAULT_NUMA_NODE_MEMORY);
        int cpus = conf.getInt(
            "yarn.nodemanager.numa-awareness." + nodeId + ".cpus",
            DEFAULT_NUMA_NODE_CPUS);
        addToCollection(nodeId, mem, cpus);
      }
    }
    if (numaNodesList.isEmpty()) {
      throw new YarnException("There are no available NUMA nodes"
          + " for making containers NUMA aware.");
    }
    LOG.info("Available numa nodes with capacities : " + numaNodesList.size());
  }

  @VisibleForTesting
  String executeNGetCmdOutput(Configuration conf) throws YarnException {
    String numaCtlCmd = conf.get(
        YarnConfiguration.NM_NUMA_AWARENESS_NUMACTL_CMD,
        YarnConfiguration.DEFAULT_NM_NUMA_AWARENESS_NUMACTL_CMD);
    String[] args = new String[] {numaCtlCmd, "--hardware"};
    ShellCommandExecutor shExec = new ShellCommandExecutor(args);
    try {
      shExec.execute();
    } catch (IOException e) {
      throw new YarnException("Failed to read the numa configurations.", e);
    }
    return shExec.getOutput();
  }

  private int parseCpus(String[] outputLines, String nodeId) {
    int cpus = 0;
    Pattern patternNodeCPUs = Pattern
        .compile(NUMA_NODE_CPUS_REGEX.replace(NUMA_NODE, nodeId));
    for (String line : outputLines) {
      Matcher matcherNodeCPUs = patternNodeCPUs.matcher(line);
      if (matcherNodeCPUs.find()) {
        String cpusStr = matcherNodeCPUs.group(1);
        cpus = cpusStr.split(SPACE).length;
        break;
      }
    }
    return cpus;
  }

  private long parseMemory(String[] outputLines, String nodeId)
      throws YarnException {
    long memory = 0;
    String units;
    Pattern patternNodeMem = Pattern
        .compile(NUMA_NODE_MEMORY_REGEX.replace(NUMA_NODE, nodeId));
    for (String line : outputLines) {
      Matcher matcherNodeMem = patternNodeMem.matcher(line);
      if (matcherNodeMem.find()) {
        try {
          memory = Long.parseLong(matcherNodeMem.group(1));
          units = matcherNodeMem.group(2);
          if (GB.equals(units)) {
            memory = memory * 1024;
          } else if (KB.equals(units)) {
            memory = memory / 1024;
          }
        } catch (Exception ex) {
          throw new YarnException("Failed to get memory for node:" + nodeId,
              ex);
        }
        break;
      }
    }
    return memory;
  }

  private void addToCollection(String nodeId, long memory, int cpus) {
    NumaNodeResource numaNode = new NumaNodeResource(nodeId, memory, cpus);
    numaNodesList.add(numaNode);
    numaNodeIdVsResource.put(nodeId, numaNode);
  }

  /**
   * Allocates the available NUMA nodes for the requested containerId with
   * resource in a round robin fashion.
   *
   * @param container the container to allocate NUMA resources
   * @return the assigned NUMA Node info or null if resources not available.
   * @throws ResourceHandlerException when failed to store NUMA resources
   */
  public synchronized NumaResourceAllocation allocateNumaNodes(
      Container container) throws ResourceHandlerException {
    NumaResourceAllocation allocation = allocate(container.getContainerId(),
        container.getResource());
    if (allocation != null) {
      try {
        // Update state store.
        context.getNMStateStore().storeAssignedResources(container,
            NUMA_RESOURCE_TYPE, Arrays.asList(allocation));
      } catch (IOException e) {
        releaseNumaResource(container.getContainerId());
        throw new ResourceHandlerException(e);
      }
    }
    return allocation;
  }

  private NumaResourceAllocation allocate(ContainerId containerId,
      Resource resource) {
    for (int index = 0; index < numaNodesList.size(); index++) {
      NumaNodeResource numaNode = numaNodesList
          .get((currentAssignNode + index) % numaNodesList.size());
      if (numaNode.isResourcesAvailable(resource)) {
        numaNode.assignResources(resource, containerId);
        LOG.info("Assigning NUMA node " + numaNode.getNodeId() + " for memory, "
            + numaNode.getNodeId() + " for cpus for the " + containerId);
        currentAssignNode = (currentAssignNode + index + 1)
            % numaNodesList.size();
        return new NumaResourceAllocation(numaNode.getNodeId(),
            resource.getMemorySize(), numaNode.getNodeId(),
            resource.getVirtualCores());
      }
    }

    // If there is no single node matched for the container resource
    // Check the NUMA nodes for Memory resources
    NumaResourceAllocation assignedNumaNodeInfo = new NumaResourceAllocation();
    long memreq = resource.getMemorySize();
    for (NumaNodeResource numaNode : numaNodesList) {
      long memrem = numaNode.assignAvailableMemory(memreq, containerId);
      assignedNumaNodeInfo.addMemoryNode(numaNode.getNodeId(), memreq - memrem);
      memreq = memrem;
      if (memreq == 0) {
        break;
      }
    }
    if (memreq != 0) {
      LOG.info("There is no available memory:" + resource.getMemorySize()
          + " in numa nodes for " + containerId);
      releaseNumaResource(containerId);
      return null;
    }

    // Check the NUMA nodes for CPU resources
    int cpusreq = resource.getVirtualCores();
    for (int index = 0; index < numaNodesList.size(); index++) {
      NumaNodeResource numaNode = numaNodesList
          .get((currentAssignNode + index) % numaNodesList.size());
      int cpusrem = numaNode.assignAvailableCpus(cpusreq, containerId);
      assignedNumaNodeInfo.addCpuNode(numaNode.getNodeId(), cpusreq - cpusrem);
      cpusreq = cpusrem;
      if (cpusreq == 0) {
        currentAssignNode = (currentAssignNode + index + 1)
            % numaNodesList.size();
        break;
      }
    }

    if (cpusreq != 0) {
      LOG.info("There are no available cpus:" + resource.getVirtualCores()
          + " in numa nodes for " + containerId);
      releaseNumaResource(containerId);
      return null;
    }
    LOG.info("Assigning multiple NUMA nodes ("
        + StringUtils.join(",", assignedNumaNodeInfo.getMemNodes())
        + ") for memory, ("
        + StringUtils.join(",", assignedNumaNodeInfo.getCpuNodes())
        + ") for cpus for " + containerId);
    return assignedNumaNodeInfo;
  }

  /**
   * Release assigned NUMA resources for the container.
   *
   * @param containerId the container ID
   */
  public synchronized void releaseNumaResource(ContainerId containerId) {
    LOG.info("Releasing the assigned NUMA resources for " + containerId);
    for (NumaNodeResource numaNode : numaNodesList) {
      numaNode.releaseResources(containerId);
    }
  }

  /**
   * Recovers assigned numa resources.
   *
   * @param containerId the container ID to recover resources
   */
  public synchronized void recoverNumaResource(ContainerId containerId) {
    Container container = context.getContainers().get(containerId);
    ResourceMappings resourceMappings = container.getResourceMappings();
    List<Serializable> assignedResources = resourceMappings
        .getAssignedResources(NUMA_RESOURCE_TYPE);
    if (assignedResources.size() == 1) {
      NumaResourceAllocation numaResourceAllocation =
          (NumaResourceAllocation) assignedResources.get(0);
      for (Entry<String, Long> nodeAndMemory : numaResourceAllocation
          .getNodeVsMemory().entrySet()) {
        numaNodeIdVsResource.get(nodeAndMemory.getKey())
            .recoverMemory(containerId, nodeAndMemory.getValue());
      }
      for (Entry<String, Integer> nodeAndCpus : numaResourceAllocation
          .getNodeVsCpus().entrySet()) {
        numaNodeIdVsResource.get(nodeAndCpus.getKey()).recoverCpus(containerId,
            nodeAndCpus.getValue());
      }
    } else {
      LOG.error("Unexpected number:" + assignedResources.size()
          + " of assigned numa resources for " + containerId
          + " while recovering.");
    }
  }

  @VisibleForTesting
  Collection<NumaNodeResource> getNumaNodesList() {
    return numaNodesList;
  }
}
