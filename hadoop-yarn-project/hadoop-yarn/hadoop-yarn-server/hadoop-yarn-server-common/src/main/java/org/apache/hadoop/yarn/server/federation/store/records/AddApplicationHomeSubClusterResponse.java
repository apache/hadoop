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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * AddApplicationHomeSubClusterResponse contains the answer from the
 * {@code FederationApplicationHomeSubClusterStore} to a request to insert a
 * newly generated applicationId and its owner.
 *
 * The response contains application's home sub-cluster as it is stored in the
 * {@code FederationApplicationHomeSubClusterStore}. If a mapping for the
 * application already existed, the {@code SubClusterId} in this response will
 * return the existing mapping which might be different from that in the
 * {@code AddApplicationHomeSubClusterRequest}.
 */
@Private
@Unstable
public abstract class AddApplicationHomeSubClusterResponse {

  @Private
  @Unstable
  public static AddApplicationHomeSubClusterResponse newInstance(
      SubClusterId homeSubCluster) {
    AddApplicationHomeSubClusterResponse response =
        Records.newRecord(AddApplicationHomeSubClusterResponse.class);
    response.setHomeSubCluster(homeSubCluster);
    return response;
  }

  /**
   * Set the home sub-cluster that this application has been assigned to.
   *
   * @param homeSubCluster the {@link SubClusterId} of this application's home
   *          sub-cluster
   */
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * Get the home sub-cluster that this application has been assigned to. This
   * may not match the {@link SubClusterId} in the corresponding response, if
   * the mapping for the request's application already existed.
   *
   * @return the {@link SubClusterId} of this application's home sub-cluster
   */
  public abstract SubClusterId getHomeSubCluster();
}
