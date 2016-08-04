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
 * Request class to obtain information about all sub-clusters that are
 * participating in federation.
 *
 * If filterInactiveSubClusters is set to true, only active sub-clusters will be
 * returned; otherwise, all sub-clusters will be returned regardless of state.
 * By default, filterInactiveSubClusters is true.
 */
@Private
@Unstable
public abstract class GetSubClustersInfoRequest {

  @Public
  @Unstable
  public static GetSubClustersInfoRequest newInstance(
      boolean filterInactiveSubClusters) {
    GetSubClustersInfoRequest request =
        Records.newRecord(GetSubClustersInfoRequest.class);
    request.setFilterInactiveSubClusters(filterInactiveSubClusters);
    return request;
  }

  /**
   * Get the flag that indicates whether only active sub-clusters should be
   * returned.
   *
   * @return whether to filter out inactive sub-clusters
   */
  @Public
  @Unstable
  public abstract boolean getFilterInactiveSubClusters();

  /**
   * Set the flag that indicates whether only active sub-clusters should be
   * returned.
   *
   * @param filterInactiveSubClusters whether to filter out inactive
   *          sub-clusters
   */
  @Public
  @Unstable
  public abstract void setFilterInactiveSubClusters(
      boolean filterInactiveSubClusters);

}
