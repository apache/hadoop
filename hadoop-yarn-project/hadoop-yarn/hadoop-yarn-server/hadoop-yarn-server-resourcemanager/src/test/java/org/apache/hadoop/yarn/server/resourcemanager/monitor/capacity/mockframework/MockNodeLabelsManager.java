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

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.mockframework.ProportionalCapacityPreemptionPolicyMockFramework.parseResourceFromString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

class MockNodeLabelsManager {
  private static final Logger LOG = LoggerFactory.getLogger(MockNodeLabelsManager.class);

  private String config;
  private final Resource clusterResource;
  private final Map<String, Resource> partitionToResource;
  private final RMNodeLabelsManager nodeLabelsManager;

  MockNodeLabelsManager(String config,
      RMNodeLabelsManager nodeLabelsManager,
      Map<String, Resource> partitionToResource) throws IOException {
    this.config = config;
    this.partitionToResource = partitionToResource;
    this.clusterResource = Resources.createResource(0);
    this.nodeLabelsManager = nodeLabelsManager;
    this.parse();
  }

  /**
   * Format is:
   * <pre>
   * partition0=total_resource,exclusivity;
   * partition1=total_resource,exclusivity;
   * ...
   * </pre>
   */
  private void parse() throws IOException {
    String[] partitionConfigArr = config.split(";");
    for (String p : partitionConfigArr) {
      String partitionName = p.substring(0, p.indexOf("="));
      Resource res = parseResourceFromString(p.substring(p.indexOf("=") + 1,
          p.indexOf(",")));
      boolean exclusivity =
          Boolean.valueOf(p.substring(p.indexOf(",") + 1));
      when(nodeLabelsManager.getResourceByLabel(eq(partitionName), any(Resource.class)))
          .thenReturn(res);
      when(nodeLabelsManager.isExclusiveNodeLabel(eq(partitionName))).thenReturn(exclusivity);

      // add to partition to resource
      partitionToResource.put(partitionName, res);
      LOG.debug("add partition=" + partitionName + " totalRes=" + res
          + " exclusivity=" + exclusivity);
      Resources.addTo(clusterResource, res);
    }

    when(nodeLabelsManager.getClusterNodeLabelNames()).thenReturn(
        partitionToResource.keySet());
  }

  public Resource getClusterResource() {
    return clusterResource;
  }
}
