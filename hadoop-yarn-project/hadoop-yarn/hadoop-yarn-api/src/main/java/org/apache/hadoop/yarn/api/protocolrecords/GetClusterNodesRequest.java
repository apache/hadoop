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

import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request from clients to get a report of all nodes
 * in the cluster from the <code>ResourceManager</code>.</p>
 *
 * The request will ask for all nodes in the given {@link NodeState}s.
 *
 * @see ApplicationClientProtocol#getClusterNodes(GetClusterNodesRequest) 
 */
@Public
@Stable
public abstract class GetClusterNodesRequest {
  @Public
  @Stable 
  public static GetClusterNodesRequest newInstance(EnumSet<NodeState> states) {
    GetClusterNodesRequest request =
        Records.newRecord(GetClusterNodesRequest.class);
    request.setNodeStates(states);
    return request;
  }
  
  @Public
  @Stable 
  public static GetClusterNodesRequest newInstance() {
    GetClusterNodesRequest request =
        Records.newRecord(GetClusterNodesRequest.class);
    return request;
  }
  
  /**
   * The state to filter the cluster nodes with.
   * @return the set of {@link NodeState}
   */
  public abstract EnumSet<NodeState> getNodeStates();
  
  /**
   * The state to filter the cluster nodes with.
   * @param states the set of {@link NodeState}
   */
  public abstract void setNodeStates(EnumSet<NodeState> states);
}
