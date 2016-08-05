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
 * The request to <code>Federation state store</code> to delete the mapping of
 * home subcluster of a submitted application.
 */
@Private
@Unstable
public abstract class DeleteApplicationHomeSubClusterRequest {

  @Private
  @Unstable
  public static DeleteApplicationHomeSubClusterRequest newInstance(
      ApplicationId applicationId) {
    DeleteApplicationHomeSubClusterRequest deleteApplicationRequest =
        Records.newRecord(DeleteApplicationHomeSubClusterRequest.class);
    deleteApplicationRequest.setApplicationId(applicationId);
    return deleteApplicationRequest;
  }

  /**
   * Get the identifier of the {@link ApplicationId} to be removed from
   * <code>Federation state store</code> .
   *
   * @return the identifier of the application to be removed from Federation
   *         State Store.
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * Set the identifier of the {@link ApplicationId} to be removed from
   * <code>Federation state store</code> .
   *
   * @param applicationId the identifier of the application to be removed from
   *          Federation State Store.
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);
}
