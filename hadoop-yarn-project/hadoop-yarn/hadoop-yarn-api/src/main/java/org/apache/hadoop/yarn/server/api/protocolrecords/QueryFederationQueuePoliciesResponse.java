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
 * This is the QueryFederationQueuePoliciesResponse, which contains the following information:
 * 1. Number of policy information included,
 * 2. total Page number,
 * 3. pageSize Conditions passed by the user,
 * 4. Result of queue weight information returned.
 */
@Private
@Unstable
public abstract class QueryFederationQueuePoliciesResponse {

  @Private
  @Unstable
  public static QueryFederationQueuePoliciesResponse newInstance(
      int totalSize, int totalPage, int currentPage, int pageSize,
      List<FederationQueueWeight> federationQueueWeights) {
    QueryFederationQueuePoliciesResponse response =
        Records.newRecord(QueryFederationQueuePoliciesResponse.class);
    response.setTotalSize(totalSize);
    response.setTotalPage(totalPage);
    response.setCurrentPage(currentPage);
    response.setPageSize(pageSize);
    response.setFederationQueueWeights(federationQueueWeights);
    return response;
  }

  @Private
  @Unstable
  public static QueryFederationQueuePoliciesResponse newInstance() {
    QueryFederationQueuePoliciesResponse response =
        Records.newRecord(QueryFederationQueuePoliciesResponse.class);
    return response;
  }

  /**
   * Returns the total size of the query result.
   * It is mainly related to the filter conditions set by the user.
   *
   * @return The total size of the query result.
   */
  public abstract int getTotalSize();

  /**
   * Sets the total size of the federationQueueWeights.
   *
   * @param totalSize The total size of the query result to be set.
   */
  public abstract void setTotalSize(int totalSize);

  /**
   * Returns the page.
   *
   * @return page.
   */
  @Public
  @Unstable
  public abstract int getTotalPage();

  /**
   * Sets the page.
   *
   * @param page page.
   */
  @Private
  @Unstable
  public abstract void setTotalPage(int page);

  /**
   * Returns the current page number in the FederationQueuePolicies pagination.
   *
   * @return The current page number.
   */
  @Public
  @Unstable
  public abstract int getCurrentPage();

  /**
   * Sets the current page in the FederationQueuePolicies pagination.
   *
   * @param currentPage The current page number.
   */
  @Private
  @Unstable
  public abstract void setCurrentPage(int currentPage);


  /**
   * Retrieves the page size.
   *
   * @return The number of policies to display per page.
   */
  @Public
  @Unstable
  public abstract int getPageSize();

  /**
   * Sets the page size for FederationQueuePolicies pagination.
   *
   * @param pageSize The number of policies to display per page.
   */
  @Private
  @Unstable
  public abstract void setPageSize(int pageSize);

  /**
   * Get a list of FederationQueueWeight objects of different queues.
   *
   * @return list of FederationQueueWeight.
   */
  @Public
  @Unstable
  public abstract List<FederationQueueWeight> getFederationQueueWeights();

  /**
   * Sets the FederationQueueWeights, which represent the weights of different queues.
   *
   * @param federationQueueWeights list of FederationQueueWeight.
   */
  @Private
  @Unstable
  public abstract void setFederationQueueWeights(
      List<FederationQueueWeight> federationQueueWeights);
}
