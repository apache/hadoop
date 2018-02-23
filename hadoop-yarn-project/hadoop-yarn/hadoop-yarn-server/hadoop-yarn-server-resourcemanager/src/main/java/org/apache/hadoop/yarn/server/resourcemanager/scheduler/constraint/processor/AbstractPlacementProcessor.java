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

import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Base class for all PlacementProcessors.
 */
public abstract class AbstractPlacementProcessor implements
    ApplicationMasterServiceProcessor{
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractPlacementProcessor.class);

  protected ApplicationMasterServiceProcessor nextAMSProcessor;
  protected AbstractYarnScheduler scheduler;
  private PlacementConstraintManager constraintManager;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    this.nextAMSProcessor = nextProcessor;
    this.scheduler =
        (AbstractYarnScheduler) ((RMContextImpl) amsContext).getScheduler();
    this.constraintManager =
        ((RMContextImpl)amsContext).getPlacementConstraintManager();
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {
    Map<Set<String>, PlacementConstraint> appPlacementConstraints =
        request.getPlacementConstraints();
    processPlacementConstraints(applicationAttemptId.getApplicationId(),
        appPlacementConstraints);
    nextAMSProcessor.registerApplicationMaster(applicationAttemptId, request,
        response);
  }

  private void processPlacementConstraints(ApplicationId applicationId,
      Map<Set<String>, PlacementConstraint> appPlacementConstraints) {
    if (appPlacementConstraints != null && !appPlacementConstraints.isEmpty()) {
      LOG.info("Constraints added for application [{}] against tags [{}]",
          applicationId, appPlacementConstraints);
      constraintManager.registerApplication(
          applicationId, appPlacementConstraints);
    }
  }

  @Override
  public void finishApplicationMaster(ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    constraintManager.unregisterApplication(
        applicationAttemptId.getApplicationId());
    this.nextAMSProcessor.finishApplicationMaster(applicationAttemptId, request,
        response);
  }
}
