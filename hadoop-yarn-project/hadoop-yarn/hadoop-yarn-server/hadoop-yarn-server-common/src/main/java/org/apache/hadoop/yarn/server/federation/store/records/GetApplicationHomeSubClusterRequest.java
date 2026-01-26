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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * Request class to obtain the home sub-cluster for the specified
 * {@link ApplicationId}.
 */
@Private
@Unstable
public abstract class GetApplicationHomeSubClusterRequest {

  @Private
  @Unstable
  public static GetApplicationHomeSubClusterRequest newInstance(
      ApplicationId appId) {
    GetApplicationHomeSubClusterRequest appMapping =
        Records.newRecord(GetApplicationHomeSubClusterRequest.class);
    appMapping.setApplicationId(appId);
    return appMapping;
  }

  @Private
  @Unstable
  public static GetApplicationHomeSubClusterRequest newInstance(
      ApplicationId appId, boolean containsAppSubmissionContext) {
    GetApplicationHomeSubClusterRequest appMapping =
         Records.newRecord(GetApplicationHomeSubClusterRequest.class);
    appMapping.setApplicationId(appId);
    appMapping.setContainsAppSubmissionContext(containsAppSubmissionContext);
    return appMapping;
  }

  /**
   * Get the {@link ApplicationId} representing the unique identifier of the
   * application.
   *
   * @return the application identifier
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * Set the {@link ApplicationId} representing the unique identifier of the
   * application.
   *
   * @param applicationId the application identifier
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);


  /**
   * Get the flag that indicates whether appSubmissionContext should be
   * returned.
   * The reason for adding this variable is due to the consideration that
   * appSubmissionContext is not commonly used and its data size can be large.
   *
   * @return whether to return appSubmissionContext.
   */
  @Public
  @Unstable
  public abstract boolean getContainsAppSubmissionContext();

  /**
   * Set the flag that indicates whether appSubmissionContext should be
   * returned.
   *
   * @param containsAppSubmissionContext whether to return appSubmissionContext.
   */
  @Public
  @Unstable
  public abstract void setContainsAppSubmissionContext(
      boolean containsAppSubmissionContext);
}
