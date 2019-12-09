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
 * <p>
 * The request sent by the <code>Router</code> to
 * <code>Federation state store</code> to update the home subcluster of a newly
 * submitted application.
 *
 * <p>
 * The request includes the mapping details, i.e.:
 * <ul>
 * <li>{@code ApplicationId}</li>
 * <li>{@code SubClusterId}</li>
 * </ul>
 */
@Private
@Unstable
public abstract class UpdateApplicationHomeSubClusterRequest {

  @Private
  @Unstable
  public static UpdateApplicationHomeSubClusterRequest newInstance(
      ApplicationHomeSubCluster applicationHomeSubCluster) {
    UpdateApplicationHomeSubClusterRequest updateApplicationRequest =
        Records.newRecord(UpdateApplicationHomeSubClusterRequest.class);
    updateApplicationRequest
        .setApplicationHomeSubCluster(applicationHomeSubCluster);
    return updateApplicationRequest;
  }

  /**
   * Get the {@link ApplicationHomeSubCluster} representing the mapping of the
   * application to it's home sub-cluster.
   *
   * @return the mapping of the application to it's home sub-cluster.
   */
  @Public
  @Unstable
  public abstract ApplicationHomeSubCluster getApplicationHomeSubCluster();

  /**
   * Set the {@link ApplicationHomeSubCluster} representing the mapping of the
   * application to it's home sub-cluster.
   *
   * @param applicationHomeSubCluster the mapping of the application to it's
   *          home sub-cluster.
   */
  @Private
  @Unstable
  public abstract void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster);
}
