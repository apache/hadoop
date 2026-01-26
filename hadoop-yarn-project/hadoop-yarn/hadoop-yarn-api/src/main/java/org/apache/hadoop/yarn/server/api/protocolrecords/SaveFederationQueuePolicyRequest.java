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
package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * In Yarn Federation mode, this class is used to save the queue policy interface.
 *
 * This class stores the queue, the weight of the queue,
 * and the PolicyManagerClassName information of the queue.
 */
@Private
@Unstable
public abstract class SaveFederationQueuePolicyRequest {

  @Private
  @Unstable
  public static SaveFederationQueuePolicyRequest newInstance(
      String queue, FederationQueueWeight federationQueueWeight, String policyManagerClassName) {
    SaveFederationQueuePolicyRequest request =
        Records.newRecord(SaveFederationQueuePolicyRequest.class);
    request.setQueue(queue);
    request.setFederationQueueWeight(federationQueueWeight);
    request.setPolicyManagerClassName(policyManagerClassName);
    return request;
  }

  @Public
  @Unstable
  public abstract FederationQueueWeight getFederationQueueWeight();

  @Private
  @Unstable
  public abstract void setFederationQueueWeight(FederationQueueWeight federationQueueWeight);

  @Public
  @Unstable
  public abstract String getQueue();

  @Public
  @Unstable
  public abstract void setQueue(String queue);

  @Public
  @Unstable
  public abstract String getPolicyManagerClassName();

  @Public
  @Unstable
  public abstract void setPolicyManagerClassName(String className);
}
