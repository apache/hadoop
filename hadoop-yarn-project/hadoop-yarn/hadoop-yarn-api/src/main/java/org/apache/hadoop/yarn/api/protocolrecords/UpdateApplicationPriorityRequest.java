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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The request sent by the client to the <code>ResourceManager</code> to set or
 * update the application priority.
 * </p>
 * <p>
 * The request includes the {@link ApplicationId} of the application and
 * {@link Priority} to be set for an application
 * </p>
 * 
 * @see ApplicationClientProtocol#updateApplicationPriority(UpdateApplicationPriorityRequest)
 */

@Public
@Unstable
public abstract class UpdateApplicationPriorityRequest {
  public static UpdateApplicationPriorityRequest newInstance(
      ApplicationId applicationId, Priority priority) {
    UpdateApplicationPriorityRequest request =
        Records.newRecord(UpdateApplicationPriorityRequest.class);
    request.setApplicationId(applicationId);
    request.setApplicationPriority(priority);
    return request;
  }

  /**
   * Get the <code>ApplicationId</code> of the application.
   * 
   * @return <code>ApplicationId</code> of the application
   */
  public abstract ApplicationId getApplicationId();

  /**
   * Set the <code>ApplicationId</code> of the application.
   * 
   * @param applicationId <code>ApplicationId</code> of the application
   */
  public abstract void setApplicationId(ApplicationId applicationId);

  /**
   * Get the <code>Priority</code> of the application to be set.
   * 
   * @return <code>Priority</code> of the application to be set.
   */
  public abstract Priority getApplicationPriority();

  /**
   * Set the <code>Priority</code> of the application.
   * 
   * @param priority <code>Priority</code> of the application
   */
  public abstract void setApplicationPriority(Priority priority);
}
