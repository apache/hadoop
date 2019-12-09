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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Processor that reject all SchedulingRequests.
 */
public class DisabledPlacementProcessor extends AbstractPlacementProcessor {
  private static final Logger LOG =
      LoggerFactory.getLogger(DisabledPlacementProcessor.class);

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    if (request.getPlacementConstraints() != null && !request
        .getPlacementConstraints().isEmpty()) {
      String message = "Found non empty placement constraints map in "
          + "RegisterApplicationMasterRequest for application="
          + applicationAttemptId.toString() + ", but the configured "
          + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " cannot handle placement constraints. Rejecting this "
          + "registerApplicationMaster operation";
      LOG.warn(message);
      throw new YarnException(message);
    }
    nextAMSProcessor.registerApplicationMaster(applicationAttemptId, request,
        response);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    if (request.getSchedulingRequests() != null && !request
        .getSchedulingRequests().isEmpty()) {
      String message = "Found non empty SchedulingRequest in "
          + "AllocateRequest for application="
          + appAttemptId.toString() + ", but the configured "
          + YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_HANDLER
          + " cannot handle placement constraints. Rejecting this "
          + "allocate operation";
      LOG.warn(message);
      throw new YarnException(message);
    }
    nextAMSProcessor.allocate(appAttemptId, request, response);
  }
}
