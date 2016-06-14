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

package org.apache.hadoop.yarn.server.resourcemanager.resource;

import java.io.InputStream;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

public class DynamicResourceConfiguration extends Configuration {

  private static final Log LOG =
    LogFactory.getLog(DynamicResourceConfiguration.class);

  @Private
  public static final String PREFIX = "yarn.resource.dynamic.";

  @Private
  public static final String DOT = ".";

  @Private
  public static final String NODES = "nodes";

  @Private
  public static final String VCORES = "vcores";

  @Private
  public static final String MEMORY = "memory";

  @Private
  public static final String OVERCOMMIT_TIMEOUT = "overcommittimeout";

  public DynamicResourceConfiguration() {
    this(new Configuration());
  }

  public DynamicResourceConfiguration(Configuration configuration) {
    super(configuration);
    addResource(YarnConfiguration.DR_CONFIGURATION_FILE);
  }

  public DynamicResourceConfiguration(Configuration configuration,
      InputStream drInputStream) {
    super(configuration);
    addResource(drInputStream);
  }

  private String getNodePrefix(String node) {
    String nodeName = PREFIX + node + DOT;
    return nodeName;
  }

  public int getVcoresPerNode(String node) {
    int vcoresPerNode =
      getInt(getNodePrefix(node) + VCORES,
        YarnConfiguration.DEFAULT_NM_VCORES);
    return vcoresPerNode;
  }

  public void setVcoresPerNode(String node, int vcores) {
    setInt(getNodePrefix(node) + VCORES, vcores);
    LOG.debug("DRConf - setVcoresPerNode: nodePrefix=" + getNodePrefix(node) +
      ", vcores=" + vcores);
  }

  public int getMemoryPerNode(String node) {
    int memoryPerNode =
      getInt(getNodePrefix(node) + MEMORY,
        YarnConfiguration.DEFAULT_NM_PMEM_MB);
    return memoryPerNode;
  }

  public void setMemoryPerNode(String node, int memory) {
    setInt(getNodePrefix(node) + MEMORY, memory);
    LOG.debug("DRConf - setMemoryPerNode: nodePrefix=" + getNodePrefix(node) +
      ", memory=" + memory);
  }

  public int getOverCommitTimeoutPerNode(String node) {
    int overCommitTimeoutPerNode =
      getInt(getNodePrefix(node) + OVERCOMMIT_TIMEOUT,
      ResourceOption.OVER_COMMIT_TIMEOUT_MILLIS_DEFAULT);
    return overCommitTimeoutPerNode;
  }

  public void setOverCommitTimeoutPerNode(String node, int overCommitTimeout) {
    setInt(getNodePrefix(node) + OVERCOMMIT_TIMEOUT, overCommitTimeout);
    LOG.debug("DRConf - setOverCommitTimeoutPerNode: nodePrefix=" +
      getNodePrefix(node) +
        ", overCommitTimeout=" + overCommitTimeout);
  }

  public String[] getNodes() {
    String[] nodes = getStrings(PREFIX + NODES);
    return nodes;
  }

  public void setNodes(String[] nodes) {
    set(PREFIX + NODES, StringUtils.arrayToString(nodes));
  }

  public Map<NodeId, ResourceOption> getNodeResourceMap() {
    String[] nodes = getNodes();
    Map<NodeId, ResourceOption> resourceOptions
      = new HashMap<NodeId, ResourceOption> ();

    for (String node : nodes) {
      NodeId nid = NodeId.fromString(node);
      int vcores = getVcoresPerNode(node);
      int memory = getMemoryPerNode(node);
      int overCommitTimeout = getOverCommitTimeoutPerNode(node);
      Resource resource = Resources.createResource(memory, vcores);
      ResourceOption resourceOption =
          ResourceOption.newInstance(resource, overCommitTimeout);
      resourceOptions.put(nid, resourceOption);
    }

    return resourceOptions;
  }
}
