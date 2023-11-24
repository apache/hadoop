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

import java.util.List;

@Private
@Unstable
public abstract class DeleteFederationQueuePoliciesRequest {

  @Private
  @Unstable
  public static DeleteFederationQueuePoliciesRequest newInstance(
      List<String> queues) {
    DeleteFederationQueuePoliciesRequest request =
        Records.newRecord(DeleteFederationQueuePoliciesRequest.class);
    request.setQueues(queues);
    return request;
  }

 /**
  * Retrieves a list of queues.
  *
  * This part contains exact matches,
  * which will match the queues contained in the list.
  *
  * @return A list of queue names or identifiers.
  */
  @Public
  @Unstable
  public abstract List<String> getQueues();

  /**
   * Sets to delete of queues to the specified values.
   *
   * @param queues A list of queue names or identifiers to set.
   */
  @Private
  @Unstable
  public abstract void setQueues(List<String> queues);
}
