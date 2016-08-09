/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * GetSubClusterPolicyConfigurationRequest is a request to the
 * {@code FederationPolicyStore} to get the configuration of a policy for a
 * given queue.
 */
@Private
@Unstable
public abstract class GetSubClusterPolicyConfigurationRequest {

  @Private
  @Unstable
  public static GetSubClusterPolicyConfigurationRequest newInstance(
      String queueName) {
    GetSubClusterPolicyConfigurationRequest request =
        Records.newRecord(GetSubClusterPolicyConfigurationRequest.class);
    request.setQueue(queueName);
    return request;
  }

  /**
   * Get the name of the queue for which we are requesting a policy
   * configuration.
   *
   * @return the name of the queue
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * Sets the name of the queue for which we are requesting a policy
   * configuration.
   *
   * @param queueName the name of the queue
   */
  @Private
  @Unstable
  public abstract void setQueue(String queueName);
}