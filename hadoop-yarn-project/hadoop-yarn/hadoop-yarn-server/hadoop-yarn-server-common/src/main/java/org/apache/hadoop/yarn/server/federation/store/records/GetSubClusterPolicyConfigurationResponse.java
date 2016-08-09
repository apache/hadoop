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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * GetSubClusterPolicyConfigurationResponse contains the answer from the {@code
 * FederationPolicyStore} to a request to get the information about how a policy
 * should be configured via a {@link SubClusterPolicyConfiguration}.
 */
@Private
@Unstable
public abstract class GetSubClusterPolicyConfigurationResponse {

  @Private
  @Unstable
  public static GetSubClusterPolicyConfigurationResponse newInstance(
      SubClusterPolicyConfiguration policy) {
    GetSubClusterPolicyConfigurationResponse response =
        Records.newRecord(GetSubClusterPolicyConfigurationResponse.class);
    response.setPolicyConfiguration(policy);
    return response;
  }

  /**
   * Get the policy configuration.
   *
   * @return the policy configuration for the specified queue
   */
  @Public
  @Unstable
  public abstract SubClusterPolicyConfiguration getPolicyConfiguration();

  /**
   * Sets the policyConfiguration configuration.
   *
   * @param policyConfiguration the policyConfiguration configuration for the
   *          specified queue
   */
  @Private
  @Unstable
  public abstract void setPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration);

}