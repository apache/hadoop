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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.util.Records;

/**
 * Contains all the state data that needs to be stored persistently 
 * for an Application
 */
@Public
@Unstable
public abstract class ApplicationStateData {
  public static ApplicationStateData newInstance(long submitTime,
      long startTime, String user,
      ApplicationSubmissionContext submissionContext,
      RMAppState state, String diagnostics, long finishTime) {
    ApplicationStateData appState = Records.newRecord(ApplicationStateData.class);
    appState.setSubmitTime(submitTime);
    appState.setStartTime(startTime);
    appState.setUser(user);
    appState.setApplicationSubmissionContext(submissionContext);
    appState.setState(state);
    appState.setDiagnostics(diagnostics);
    appState.setFinishTime(finishTime);
    return appState;
  }

  public static ApplicationStateData newInstance(
      ApplicationState appState) {
    return newInstance(appState.getSubmitTime(), appState.getStartTime(),
        appState.getUser(), appState.getApplicationSubmissionContext(),
        appState.getState(), appState.getDiagnostics(),
        appState.getFinishTime());
  }

  public abstract ApplicationStateDataProto getProto();

  /**
   * The time at which the application was received by the Resource Manager
   * @return submitTime
   */
  @Public
  @Unstable
  public abstract long getSubmitTime();
  
  @Public
  @Unstable
  public abstract void setSubmitTime(long submitTime);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  @Public
  @Stable
  public abstract long getStartTime();

  @Private
  @Unstable
  public abstract void setStartTime(long startTime);

  /**
   * The application submitter
   */
  @Public
  @Unstable
  public abstract void setUser(String user);
  
  @Public
  @Unstable
  public abstract String getUser();
  
  /**
   * The {@link ApplicationSubmissionContext} for the application
   * {@link ApplicationId} can be obtained from the this
   * @return ApplicationSubmissionContext
   */
  @Public
  @Unstable
  public abstract ApplicationSubmissionContext getApplicationSubmissionContext();
  
  @Public
  @Unstable
  public abstract void setApplicationSubmissionContext(
      ApplicationSubmissionContext context);

  /**
   * Get the final state of the application.
   * @return the final state of the application.
   */
  public abstract RMAppState getState();

  public abstract void setState(RMAppState state);

  /**
   * Get the diagnostics information for the application master.
   * @return the diagnostics information for the application master.
   */
  public abstract String getDiagnostics();

  public abstract void setDiagnostics(String diagnostics);

  /**
   * The finish time of the application.
   * @return the finish time of the application.,
   */
  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);
}
