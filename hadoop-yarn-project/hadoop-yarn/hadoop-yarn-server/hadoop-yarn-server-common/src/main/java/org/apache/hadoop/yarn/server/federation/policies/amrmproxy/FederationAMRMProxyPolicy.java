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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * Implementors of this interface provide logic to split the list of
 * {@link ResourceRequest}s received by the AM among various RMs.
 */
public interface FederationAMRMProxyPolicy
    extends ConfigurableFederationPolicy {

  /**
   * Splits the {@link ResourceRequest}s from the client across one or more
   * sub-clusters based on the policy semantics (e.g., broadcast, load-based).
   *
   * @param resourceRequests the list of {@link ResourceRequest}s from the AM to
   *          be split
   * @param timedOutSubClusters the set of sub-clusters that haven't had a
   *          successful heart-beat response for a while.
   * @return map of sub-cluster as identified by {@link SubClusterId} to the
   *         list of {@link ResourceRequest}s that should be forwarded to it
   * @throws YarnException in case the request is malformed or no viable
   *           sub-clusters can be found.
   */
  Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException;

  /**
   * This method should be invoked to notify the policy about responses being
   * received. This is useful for stateful policies that make decisions based on
   * previous responses being received.
   *
   * @param subClusterId the id of the subcluster sending the notification
   * @param response the response received from one of the RMs
   *
   * @throws YarnException in case the response is not valid
   */
  void notifyOfResponse(SubClusterId subClusterId, AllocateResponse response)
      throws YarnException;

}
