/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * This {@link FederationRouterPolicy} simply rejects all incoming requests.
 * This is useful to prevent applications running in a queue to be run anywhere
 * in the federated cluster.
 */
public class RejectRouterPolicy extends AbstractRouterPolicy {

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext federationPolicyContext)
      throws FederationPolicyInitializationException {
    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());
    setPolicyContext(federationPolicyContext);
  }

  /**
   * The policy always reject requests.
   *
   * @param appSubmissionContext the {@link ApplicationSubmissionContext} that
   *          has to be routed to an appropriate subCluster for execution.
   *
   * @param blackListSubClusters the list of subClusters as identified by
   *          {@link SubClusterId} to blackList from the selection of the home
   *          subCluster.
   *
   * @return (never).
   *
   * @throws YarnException (always) to prevent applications in this queue to be
   *           run anywhere in the federated cluster.
   */
  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // run standard validation, as error might differ
    validate(appSubmissionContext);

    throw new FederationPolicyException("The policy configured for this queue"
        + " (" + appSubmissionContext.getQueue() + ") reject all routing "
        + "requests by construction. Application "
        + appSubmissionContext.getApplicationId()
        + " cannot be routed to any RM.");
  }

}
