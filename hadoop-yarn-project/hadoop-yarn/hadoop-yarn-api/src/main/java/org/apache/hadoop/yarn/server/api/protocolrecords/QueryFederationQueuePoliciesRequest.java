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

/**
 * Request for querying Federation Queue Policies.
 * It includes several query conditions such as queue, queues, pageSize, and currentPage.
 *
 * queue: The specific queue name or identifier to query the Federation Queue Policy for.
 * queues: A list of queue names or identifiers for which to query the Federation Queue Policies.
 * pageSize: The number of policies to display per page.
 * currentPage: The current page number.
 */
@Private
@Unstable
public abstract class QueryFederationQueuePoliciesRequest {

  @Private
  @Unstable
  public static QueryFederationQueuePoliciesRequest newInstance(
      int pageSize, int currentPage, String queue, List<String> queues) {
    QueryFederationQueuePoliciesRequest request =
        Records.newRecord(QueryFederationQueuePoliciesRequest.class);
    request.setPageSize(pageSize);
    request.setCurrentPage(currentPage);
    request.setQueue(queue);
    request.setQueues(queues);
    return request;
  }

  /**
   * Sets the page size for FederationQueuePolicies pagination.
   *
   * @param pageSize The number of policies to display per page.
   */
  @Private
  @Unstable
  public abstract void setPageSize(int pageSize);

  /**
   * Retrieves the page size.
   *
   * @return The number of policies to display per page.
   */
  @Public
  @Unstable
  public abstract int getPageSize();

  /**
   * Sets the current page in the FederationQueuePolicies pagination.
   *
   * @param currentPage The current page number.
   */
  @Private
  @Unstable
  public abstract void setCurrentPage(int currentPage);

  /**
   * Returns the current page number in the FederationQueuePolicies pagination.
   *
   * @return The current page number.
   */
  @Public
  @Unstable
  public abstract int getCurrentPage();

  /**
   * Retrieves the queue.
   *
   * @return The name or identifier of the current queue.
   */
  @Public
  @Unstable
  public abstract String getQueue();

  /**
   * Sets the queue to the specified value.
   *
   * We will use the fully qualified name matching for queues.
   * For example, if the user inputs 'a', we will match
   * queues that contain 'a' in their fully qualified names,
   * such as 'root.a', 'root.b.a', and so on.
   *
   * @param queue queue name.
   */
  @Private
  @Unstable
  public abstract void setQueue(String queue);

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
   * Sets the list of queues to the specified values.
   *
   * @param queues A list of queue names or identifiers to set.
   */
  @Private
  @Unstable
  public abstract void setQueues(List<String> queues);
}
