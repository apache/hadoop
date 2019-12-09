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
package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

/**
 * This encapsulates a Rejected SchedulingRequest. It contains the offending
 * Scheduling Request along with the reason for rejection.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class RejectedSchedulingRequest {

  /**
   * Create new RejectedSchedulingRequest.
   * @param reason Rejection Reason.
   * @param request Rejected Scheduling Request.
   * @return RejectedSchedulingRequest.
   */
  public static RejectedSchedulingRequest newInstance(RejectionReason reason,
      SchedulingRequest request) {
    RejectedSchedulingRequest instance =
        Records.newRecord(RejectedSchedulingRequest.class);
    instance.setReason(reason);
    instance.setRequest(request);
    return instance;
  }

  /**
   * Get Rejection Reason.
   * @return Rejection reason.
   */
  public abstract RejectionReason getReason();

  /**
   * Set Rejection Reason.
   * @param reason Rejection Reason.
   */
  public abstract void setReason(RejectionReason reason);

  /**
   * Get the Rejected Scheduling Request.
   * @return SchedulingRequest.
   */
  public abstract SchedulingRequest getRequest();

  /**
   * Set the SchedulingRequest.
   * @param request SchedulingRequest.
   */
  public abstract void setRequest(SchedulingRequest request);
}
