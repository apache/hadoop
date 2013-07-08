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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * Utility methods to aid serving RM data through the REST and RPC APIs
 */
public class RMServerUtils {
  public static List<RMNode> queryRMNodes(RMContext context,
      EnumSet<NodeState> acceptedStates) {
    // nodes contains nodes that are NEW, RUNNING OR UNHEALTHY
    ArrayList<RMNode> results = new ArrayList<RMNode>();
    if (acceptedStates.contains(NodeState.NEW) ||
        acceptedStates.contains(NodeState.RUNNING) ||
        acceptedStates.contains(NodeState.UNHEALTHY)) {
      for (RMNode rmNode : context.getRMNodes().values()) {
        if (acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }
    
    // inactiveNodes contains nodes that are DECOMMISSIONED, LOST, OR REBOOTED
    if (acceptedStates.contains(NodeState.DECOMMISSIONED) ||
        acceptedStates.contains(NodeState.LOST) ||
        acceptedStates.contains(NodeState.REBOOTED)) {
      for (RMNode rmNode : context.getInactiveRMNodes().values()) {
        if (acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }
    return results;
  }
}
