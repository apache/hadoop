/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This policy selects the subcluster depending on the node where the Client
 * wants to run its application.
 *
 * It succeeds if:
 *
 * - There are three AMContainerResourceRequests in the order
 *   NODE, RACK, ANY
 *
 * It falls back to WeightedRandomRouterPolicy in case of:
 *
 * - Null or empty AMContainerResourceRequests;
 *
 * - One AMContainerResourceRequests and it has ANY as ResourceName;
 *
 * - The node is in blacklisted SubClusters.
 *
 * It fails if:
 *
 * - The node does not exist and RelaxLocality is False;
 *
 * - We have an invalid number (not 0, 1 or 3) resource requests
 */
public class LocalityRouterPolicy extends WeightedRandomRouterPolicy {

  public static final Logger LOG =
      LoggerFactory.getLogger(LocalityRouterPolicy.class);

  private SubClusterResolver resolver;
  private List<SubClusterId> enabledSCs;

  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    super.reinitialize(policyContext);
    resolver = policyContext.getFederationSubclusterResolver();
    Map<SubClusterIdInfo, Float> weights =
        getPolicyInfo().getRouterPolicyWeights();
    enabledSCs = new ArrayList<SubClusterId>();
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      if (entry != null && entry.getValue() > 0) {
        enabledSCs.add(entry.getKey().toId());
      }
    }
  }

  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // null checks and default-queue behavior
    validate(appSubmissionContext);

    List<ResourceRequest> rrList =
        appSubmissionContext.getAMContainerResourceRequests();

    // Fast path for FailForward to WeightedRandomRouterPolicy
    if (rrList == null || rrList.isEmpty() || (rrList.size() == 1
        && ResourceRequest.isAnyLocation(rrList.get(0).getResourceName()))) {
      return super
          .getHomeSubcluster(appSubmissionContext, blackListSubClusters);
    }

    if (rrList.size() != 3) {
      throw new FederationPolicyException(
          "Invalid number of resource requests: " + rrList.size());
    }

    Map<SubClusterId, SubClusterInfo> activeSubClusters =
        getActiveSubclusters();
    List<SubClusterId> validSubClusters =
        new ArrayList<>(activeSubClusters.keySet());
    FederationPolicyUtils
        .validateSubClusterAvailability(validSubClusters, blackListSubClusters);
    if (blackListSubClusters != null) {
      // Remove from the active SubClusters from StateStore the blacklisted ones
      validSubClusters.removeAll(blackListSubClusters);
    }

    try {
      // With three requests, this has been processed by the
      // ResourceRequestInterceptorREST, and should have
      // node, rack, and any
      SubClusterId targetId = null;
      ResourceRequest nodeRequest = null;
      ResourceRequest rackRequest = null;
      ResourceRequest anyRequest = null;
      for (ResourceRequest rr : rrList) {
        // Handle "node" requests
        try {
          targetId = resolver.getSubClusterForNode(rr.getResourceName());
          nodeRequest = rr;
        } catch (YarnException e) {
          LOG.error("Cannot resolve node : {}", e.getLocalizedMessage());
        }
        // Handle "rack" requests
        try {
          resolver.getSubClustersForRack(rr.getResourceName());
          rackRequest = rr;
        } catch (YarnException e) {
          LOG.error("Cannot resolve rack : {}", e.getLocalizedMessage());
        }
        // Handle "ANY" requests
        if (ResourceRequest.isAnyLocation(rr.getResourceName())) {
          anyRequest = rr;
          continue;
        }
      }
      if (nodeRequest == null) {
        throw new YarnException("Missing node request");
      }
      if (rackRequest == null) {
        throw new YarnException("Missing rack request");
      }
      if (anyRequest == null) {
        throw new YarnException("Missing any request");
      }
      LOG.info(
          "Node request: " + nodeRequest.getResourceName() + ", Rack request: "
              + rackRequest.getResourceName() + ", Any request: " + anyRequest
              .getResourceName());
      // Handle "node" requests
      if (validSubClusters.contains(targetId) && enabledSCs
          .contains(targetId)) {
        LOG.info("Node {} is in SubCluster: {}", nodeRequest.getResourceName(),
            targetId);
        return targetId;
      } else {
        throw new YarnException("The node " + nodeRequest.getResourceName()
            + " is in a blacklist SubCluster or not active. ");
      }
    } catch (YarnException e) {
      LOG.error("Validating resource requests failed, Falling back to "
          + "WeightedRandomRouterPolicy placement: " + e.getMessage());
      // FailForward to WeightedRandomRouterPolicy
      // Overwrite request to use a default ANY
      ResourceRequest amReq = Records.newRecord(ResourceRequest.class);
      amReq.setPriority(appSubmissionContext.getPriority());
      amReq.setResourceName(ResourceRequest.ANY);
      amReq.setCapability(appSubmissionContext.getResource());
      amReq.setNumContainers(1);
      amReq.setRelaxLocality(true);
      amReq.setNodeLabelExpression(
          appSubmissionContext.getNodeLabelExpression());
      amReq.setExecutionTypeRequest(
          ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
      appSubmissionContext
          .setAMContainerResourceRequests(Collections.singletonList(amReq));
      return super
          .getHomeSubcluster(appSubmissionContext, blackListSubClusters);
    }
  }
}
