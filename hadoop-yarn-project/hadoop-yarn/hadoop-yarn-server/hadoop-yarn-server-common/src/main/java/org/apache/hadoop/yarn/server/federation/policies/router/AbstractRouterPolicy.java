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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.AbstractConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * Base abstract class for {@link FederationRouterPolicy} implementations, that
 * provides common validation for reinitialization.
 */
public abstract class AbstractRouterPolicy extends
    AbstractConfigurableFederationPolicy implements FederationRouterPolicy {

  @Override
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    super.validate(newPolicyInfo);
    Map<SubClusterIdInfo, Float> newWeights =
        newPolicyInfo.getRouterPolicyWeights();
    if (newWeights == null || newWeights.size() < 1) {
      throw new FederationPolicyInitializationException(
          "Weight vector cannot be null/empty.");
    }
  }

  public void validate(ApplicationSubmissionContext appSubmissionContext)
      throws FederationPolicyException {

    if (appSubmissionContext == null) {
      throw new FederationPolicyException(
          "Cannot route an application with null context.");
    }

    // if the queue is not specified we set it to default value, to be
    // compatible with YARN behavior.
    String queue = appSubmissionContext.getQueue();
    if (queue == null) {
      appSubmissionContext.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }
  }

  protected abstract SubClusterId chooseSubCluster(String queue,
      Map<SubClusterId, SubClusterInfo> preSelectSubClusters) throws YarnException;

  protected Map<SubClusterId, SubClusterInfo> prefilterSubClusters(
      ReservationId reservationId, Map<SubClusterId, SubClusterInfo> activeSubClusters)
      throws YarnException {

    // if a reservation exists limit scope to the sub-cluster this
    // reservation is mapped to
    if (reservationId != null) {

      // note this might throw YarnException if the reservation is
      // unknown. This is to be expected, and should be handled by
      // policy invoker.
      SubClusterId resSubCluster =
          getPolicyContext().getFederationStateStoreFacade().
          getReservationHomeSubCluster(reservationId);

      return Collections.singletonMap(resSubCluster, activeSubClusters.get(resSubCluster));
    }

    return activeSubClusters;
  }

  @Override
  public SubClusterId getHomeSubcluster(ApplicationSubmissionContext appContext,
      List<SubClusterId> blackLists) throws YarnException {

    // null checks and default-queue behavior
    validate(appContext);

    // apply filtering based on reservation location and active sub-clusters
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = prefilterSubClusters(
        appContext.getReservationID(), getActiveSubclusters());

    FederationPolicyUtils.validateSubClusterAvailability(
        new ArrayList<>(filteredSubClusters.keySet()), blackLists);

    // remove black SubCluster
    if (blackLists != null) {
      blackLists.forEach(filteredSubClusters::remove);
    }

    // pick the chosen subCluster from the active ones
    return chooseSubCluster(appContext.getQueue(), filteredSubClusters);
  }


  @Override
  public SubClusterId getReservationHomeSubcluster(ReservationSubmissionRequest request)
      throws YarnException {
    if (request == null) {
      throw new FederationPolicyException(
          "The ReservationSubmissionRequest cannot be null.");
    }

    if (request.getQueue() == null) {
      request.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }

    // apply filtering based on reservation location and active sub-clusters
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = prefilterSubClusters(
        request.getReservationId(), getActiveSubclusters());

    // pick the chosen subCluster from the active ones
    return chooseSubCluster(request.getQueue(), filteredSubClusters);
  }
}
