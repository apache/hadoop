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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;

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
  
  /**
   * Utility method to validate a list resource requests, by insuring that the
   * requested memory/vcore is non-negative and not greater than max
   */
  public static void validateResourceRequests(List<ResourceRequest> ask,
      Resource maximumResource) throws InvalidResourceRequestException {
    for (ResourceRequest resReq : ask) {
      SchedulerUtils.validateResourceRequest(resReq, maximumResource);
    }
  }

  /*
   * @throw <code>InvalidResourceBlacklistRequestException </code> if the
   * resource is not able to be added to the blacklist.
   */
  public static void validateBlacklistRequest(ResourceBlacklistRequest blacklistRequest) 
  throws InvalidResourceBlacklistRequestException {
    if (blacklistRequest != null) {
      List<String> plus = blacklistRequest.getBlacklistAdditions();
      if (plus != null && plus.contains(ResourceRequest.ANY)) {
        throw new InvalidResourceBlacklistRequestException(
            "Cannot add " + ResourceRequest.ANY + " to the blacklist!");
      }
    }
  }

  /**
   * It will validate to make sure all the containers belong to correct
   * application attempt id. If not then it will throw
   * {@link InvalidContainerReleaseException}
   * @param containerReleaseList containers to be released as requested by
   * application master.
   * @param appAttemptId Application attempt Id
   * @throws InvalidContainerReleaseException 
   */
  public static void
      validateContainerReleaseRequest(List<ContainerId> containerReleaseList,
          ApplicationAttemptId appAttemptId)
          throws InvalidContainerReleaseException {
    for (ContainerId cId : containerReleaseList) {
      if (!appAttemptId.equals(cId.getApplicationAttemptId())) {
        throw new InvalidContainerReleaseException("Cannot release container : "
            + cId.toString() + " not belonging to this application attempt : "
            + appAttemptId);
      }
    }
  }
}
