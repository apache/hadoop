/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * This class encapsulates the response received from the ResourceScheduler's
 * attemptAllocateOnNode method.
 */
public class SchedulingResponse {

  private final boolean isSuccess;
  private final ApplicationId applicationId;
  private final SchedulingRequest schedulingRequest;

  /**
   * Create a SchedulingResponse.
   * @param isSuccess did scheduler accept.
   * @param applicationId Application Id.
   * @param schedulingRequest Scheduling Request.
   */
  public SchedulingResponse(boolean isSuccess, ApplicationId applicationId,
      SchedulingRequest schedulingRequest) {
    this.isSuccess = isSuccess;
    this.applicationId = applicationId;
    this.schedulingRequest = schedulingRequest;
  }

  /**
   * Returns true if Scheduler was able to accept and commit this request.
   * @return isSuccessful.
   */
  public boolean isSuccess() {
    return this.isSuccess;
  }

  /**
   * Get Application Id.
   * @return Application Id.
   */
  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  /**
   * Get Scheduling Request.
   * @return Scheduling Request.
   */
  public SchedulingRequest getSchedulingRequest() {
    return this.schedulingRequest;
  }

}
