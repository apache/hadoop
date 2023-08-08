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
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

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

  /**
   * This method is implemented by the specific policy, and it is used to route
   * both reservations, and applications among a given set of
   * sub-clusters.
   *
   * @param queue the queue for this application/reservation
   * @param preSelectSubClusters a pre-filter set of sub-clusters
   * @return the chosen sub-cluster
   *
   * @throws YarnException if the policy fails to choose a sub-cluster
   */
  protected abstract SubClusterId chooseSubCluster(String queue,
      Map<SubClusterId, SubClusterInfo> preSelectSubClusters) throws YarnException;

  /**
   * Filter chosen SubCluster based on reservationId.
   *
   * @param reservationId the globally unique identifier for a reservation.
   * @param activeSubClusters the map of ids to info for all active subclusters.
   * @return the chosen sub-cluster
   * @throws YarnException if the policy fails to choose a sub-cluster
   */
  protected Map<SubClusterId, SubClusterInfo> prefilterSubClusters(
      ReservationId reservationId, Map<SubClusterId, SubClusterInfo> activeSubClusters)
      throws YarnException {

    // if a reservation exists limit scope to the sub-cluster this
    // reservation is mapped to
    if (reservationId != null) {
      // note this might throw YarnException if the reservation is
      // unknown. This is to be expected, and should be handled by
      // policy invoker.
      FederationStateStoreFacade stateStoreFacade =
          getPolicyContext().getFederationStateStoreFacade();
      SubClusterId resSubCluster = stateStoreFacade.getReservationHomeSubCluster(reservationId);
      SubClusterInfo subClusterInfo = activeSubClusters.get(resSubCluster);
      return Collections.singletonMap(resSubCluster, subClusterInfo);
    }

    return activeSubClusters;
  }

  /**
   * Simply picks from alphabetically-sorted active subclusters based on the
   * hash of query name. Jobs of the same queue will all be routed to the same
   * sub-cluster, as far as the number of active sub-cluster and their names
   * remain the same.
   *
   * @param appContext the {@link ApplicationSubmissionContext} that
   *          has to be routed to an appropriate subCluster for execution.
   *
   * @param blackLists the list of subClusters as identified by
   *          {@link SubClusterId} to blackList from the selection of the home
   *          subCluster.
   *
   * @return a hash-based chosen {@link SubClusterId} that will be the "home"
   *         for this application.
   *
   * @throws YarnException if there are no active subclusters.
   */
  @Override
  public SubClusterId getHomeSubcluster(ApplicationSubmissionContext appContext,
      List<SubClusterId> blackLists) throws YarnException {

    // null checks and default-queue behavior
    validate(appContext);

    // apply filtering based on reservation location and active sub-clusters
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = prefilterSubClusters(
        appContext.getReservationID(), getActiveSubclusters());

    FederationPolicyUtils.validateSubClusterAvailability(filteredSubClusters.keySet(), blackLists);

    // remove black SubCluster
    if (blackLists != null) {
      blackLists.forEach(filteredSubClusters::remove);
    }

    // pick the chosen subCluster from the active ones
    return chooseSubCluster(appContext.getQueue(), filteredSubClusters);
  }

  /**
   * This method provides a wrapper of all policy functionalities for routing a
   * reservation. Internally it manages configuration changes, and policy
   * init/reinit.
   *
   * @param request the reservation to route.
   *
   * @return the id of the subcluster that will be the "home" for this
   *         reservation.
   *
   * @throws YarnException if there are issues initializing policies, or no
   *           valid sub-cluster id could be found for this reservation.
   */
  @Override
  public SubClusterId getReservationHomeSubcluster(ReservationSubmissionRequest request)
      throws YarnException {
    if (request == null) {
      throw new FederationPolicyException("The ReservationSubmissionRequest cannot be null.");
    }

    if (request.getQueue() == null) {
      request.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }

    // apply filtering based on reservation location and active sub-clusters
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = getActiveSubclusters();

    // pick the chosen subCluster from the active ones
    return chooseSubCluster(request.getQueue(), filteredSubClusters);
  }
}
