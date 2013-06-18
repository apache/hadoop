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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The response sent by the <code>ResourceManager</code> to a client
 * requesting a {@link NodeReport} for all nodes.</p>
 * 
 * <p>The <code>NodeReport</code> contains per-node information such as 
 * available resources, number of containers, tracking url, rack name, health
 * status etc.
 * 
 * @see NodeReport
 * @see ApplicationClientProtocol#getClusterNodes(GetClusterNodesRequest)
 */
@Public
@Stable
public abstract class GetClusterNodesResponse {

  @Private
  @Unstable
  public static GetClusterNodesResponse
      newInstance(List<NodeReport> nodeReports) {
    GetClusterNodesResponse response =
        Records.newRecord(GetClusterNodesResponse.class);
    response.setNodeReports(nodeReports);
    return response;
  }

  /**
   * Get <code>NodeReport</code> for all nodes in the cluster.
   * @return <code>NodeReport</code> for all nodes in the cluster
   */
  @Public
  @Stable
  public abstract List<NodeReport> getNodeReports();
  
  @Private
  @Unstable
  public abstract void setNodeReports(List<NodeReport> nodeReports);
}
