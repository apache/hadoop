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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request sent by admin to change a list of nodes' resource to the 
 * <code>ResourceManager</code>.</p>
 * 
 * <p>The request contains details such as a map from {@link NodeId} to 
 * {@link ResourceOption} for updating the RMNodes' resources in 
 * <code>ResourceManager</code>.
 * 
 * @see ResourceManagerAdministrationProtocol#updateNodeResource(
 *      UpdateNodeResourceRequest)
 */
@Public
@Evolving
public abstract class UpdateNodeResourceRequest {


  @Public
  @Evolving
  public static UpdateNodeResourceRequest newInstance(
      Map<NodeId, ResourceOption> nodeResourceMap) {
    UpdateNodeResourceRequest request =
        Records.newRecord(UpdateNodeResourceRequest.class);
    request.setNodeResourceMap(nodeResourceMap);
    return request;
  }

  @Public
  @Evolving
  public static UpdateNodeResourceRequest newInstance(
      Map<NodeId, ResourceOption> nodeResourceMap, String subClusterId) {
    UpdateNodeResourceRequest request =
        Records.newRecord(UpdateNodeResourceRequest.class);
    request.setNodeResourceMap(nodeResourceMap);
    request.setSubClusterId(subClusterId);
    return request;
  }

  /**
   * Get the map from <code>NodeId</code> to <code>ResourceOption</code>.
   * @return the map of {@code <NodeId, ResourceOption>}
   */
  @Public
  @Evolving
  public abstract Map<NodeId, ResourceOption> getNodeResourceMap();
  
  /**
   * Set the map from <code>NodeId</code> to <code>ResourceOption</code>.
   * @param nodeResourceMap the map of {@code <NodeId, ResourceOption>}
   */
  @Public
  @Evolving
  public abstract void setNodeResourceMap(Map<NodeId, ResourceOption> nodeResourceMap);

  /**
   * Get the subClusterId.
   *
   * @return subClusterId.
   */
  @Public
  @Evolving
  public abstract String getSubClusterId();

  /**
   * Set the subClusterId.
   *
   * @param subClusterId subCluster Id.
   */
  @Public
  @Evolving
  public abstract void setSubClusterId(String subClusterId);
}
