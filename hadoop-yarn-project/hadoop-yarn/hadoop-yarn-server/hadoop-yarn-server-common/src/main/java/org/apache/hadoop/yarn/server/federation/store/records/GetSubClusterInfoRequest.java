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
 * Request class to obtain information about a sub-cluster identified by its
 * {@link SubClusterId}.
 */
@Private
@Unstable
public abstract class GetSubClusterInfoRequest {

  @Private
  @Unstable
  public static GetSubClusterInfoRequest newInstance(
      SubClusterId subClusterId) {
    GetSubClusterInfoRequest subClusterRequest =
        Records.newRecord(GetSubClusterInfoRequest.class);
    subClusterRequest.setSubClusterId(subClusterId);
    return subClusterRequest;
  }

  /**
   * Get the {@link SubClusterId} representing the unique identifier of the
   * subcluster.
   *
   * @return the subcluster identifier
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * Set the {@link SubClusterId} representing the unique identifier of the
   * subcluster.
   *
   * @param subClusterId the subcluster identifier
   */
  @Public
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);
}
