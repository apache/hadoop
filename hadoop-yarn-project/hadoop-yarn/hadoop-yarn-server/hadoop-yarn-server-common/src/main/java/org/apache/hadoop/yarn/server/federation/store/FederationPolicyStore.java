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

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;

/**
 * The FederationPolicyStore provides a key-value interface to access the
 * policies configured for the system. The key is a "queue" name, i.e., the
 * system allows to configure a different policy for each queue in the system
 * (though each policy can make dynamic run-time decisions on a per-job/per-task
 * basis). The value is a {@code SubClusterPolicyConfiguration}, a serialized
 * representation of the policy type and its parameters.
 */
@Private
@Unstable
public interface FederationPolicyStore {

  /**
   * Get the policy configuration for a given queue.
   *
   * @param request the queue whose {@code SubClusterPolicyConfiguration} is
   *          required
   * @return the {@code SubClusterPolicyConfiguration} for the specified queue,
   *         or {@code null} if there is no mapping for the queue
   * @throws YarnException if the request is invalid/fails
   */
  GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException;

  /**
   * Set the policy configuration for a given queue.
   *
   * @param request the {@code SubClusterPolicyConfiguration} with the
   *          corresponding queue
   * @return response empty on successfully updating the
   *         {@code SubClusterPolicyConfiguration} for the specified queue
   * @throws YarnException if the request is invalid/fails
   */
  SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException;

  /**
   * Get a map of all queue-to-policy configurations.
   *
   * @param request empty to represent all configured queues in the system
   * @return the policies for all currently active queues in the system
   * @throws YarnException if the request is invalid/fails
   */
  GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException;

}
