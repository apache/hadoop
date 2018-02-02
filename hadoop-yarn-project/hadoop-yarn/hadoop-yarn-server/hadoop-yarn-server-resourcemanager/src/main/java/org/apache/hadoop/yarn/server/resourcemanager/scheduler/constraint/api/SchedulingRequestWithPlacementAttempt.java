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

import org.apache.hadoop.yarn.api.records.SchedulingRequest;

/**
 * Simple holder class encapsulating a SchedulingRequest
 * with a placement attempt.
 */
public class SchedulingRequestWithPlacementAttempt {

  private final int placementAttempt;
  private final SchedulingRequest schedulingRequest;

  public SchedulingRequestWithPlacementAttempt(int placementAttempt,
      SchedulingRequest schedulingRequest) {
    this.placementAttempt = placementAttempt;
    this.schedulingRequest = schedulingRequest;
  }

  public int getPlacementAttempt() {
    return placementAttempt;
  }

  public SchedulingRequest getSchedulingRequest() {
    return schedulingRequest;
  }

  @Override
  public String toString() {
    return "SchedulingRequestWithPlacementAttempt{" +
        "placementAttempt=" + placementAttempt +
        ", schedulingRequest=" + schedulingRequest +
        '}';
  }
}
