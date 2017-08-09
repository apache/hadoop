/*
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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.UnknownSubclusterException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * An implementation of the {@link FederationAMRMProxyPolicy} that simply
 * broadcasts each {@link ResourceRequest} to all the available sub-clusters.
 */
public class BroadcastAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  private Set<SubClusterId> knownClusterIds = new HashSet<>();

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    // overrides initialize to avoid weight checks that do no apply for
    // this policy.
    FederationPolicyInitializationContextValidator
        .validate(policyContext, this.getClass().getCanonicalName());
    setPolicyContext(policyContext);
  }

  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests) throws YarnException {

    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getActiveSubclusters();

    Map<SubClusterId, List<ResourceRequest>> answer = new HashMap<>();

    // simply broadcast the resource request to all sub-clusters
    for (SubClusterId subClusterId : activeSubclusters.keySet()) {
      answer.put(subClusterId, resourceRequests);
      knownClusterIds.add(subClusterId);
    }

    return answer;
  }

  @Override
  public void notifyOfResponse(SubClusterId subClusterId,
      AllocateResponse response) throws YarnException {
    if (!knownClusterIds.contains(subClusterId)) {
      throw new UnknownSubclusterException(
          "The response is received from a subcluster that is unknown to this "
              + "policy.");
    }
    // stateless policy does not care about responses
  }

}
