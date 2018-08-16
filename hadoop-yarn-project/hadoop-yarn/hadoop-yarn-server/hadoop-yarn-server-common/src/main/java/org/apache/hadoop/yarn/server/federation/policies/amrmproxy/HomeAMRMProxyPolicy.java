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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * An implementation of the {@link FederationAMRMProxyPolicy} that simply
 * sends the {@link ResourceRequest} to the home subcluster.
 */
public class HomeAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  /** Identifier of the local subcluster. */
  private SubClusterId homeSubcluster;

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {

    FederationPolicyInitializationContextValidator
        .validate(policyContext, this.getClass().getCanonicalName());
    setPolicyContext(policyContext);

    this.homeSubcluster = policyContext.getHomeSubcluster();
  }

  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests) throws YarnException {

    if (homeSubcluster == null) {
      throw new FederationPolicyException("No home subcluster available");
    }

    Map<SubClusterId, SubClusterInfo> active = getActiveSubclusters();
    if (!active.containsKey(homeSubcluster)) {
      throw new FederationPolicyException(
          "The local subcluster " + homeSubcluster + " is not active");
    }

    List<ResourceRequest> resourceRequestsCopy =
        new ArrayList<>(resourceRequests);
    return Collections.singletonMap(homeSubcluster, resourceRequestsCopy);
  }
}
