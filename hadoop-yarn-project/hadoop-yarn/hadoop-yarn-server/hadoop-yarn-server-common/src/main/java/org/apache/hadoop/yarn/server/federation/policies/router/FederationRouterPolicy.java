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
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * Implements the logic for determining the routing of an application submission
 * based on a policy.
 */
public interface FederationRouterPolicy extends ConfigurableFederationPolicy {

  /**
   * Determines the sub-cluster that the user application submission should be
   * routed to.
   *
   * @param appSubmissionContext the {@link ApplicationSubmissionContext} that
   *          has to be routed to an appropriate subCluster for execution.
   *
   * @param blackListSubClusters the list of subClusters as identified by
   *          {@link SubClusterId} to blackList from the selection of the home
   *          subCluster.
   *
   * @return the {@link SubClusterId} that will be the "home" for this
   *         application.
   *
   * @throws YarnException if the policy cannot determine a viable subcluster.
   */
  SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException;
}
