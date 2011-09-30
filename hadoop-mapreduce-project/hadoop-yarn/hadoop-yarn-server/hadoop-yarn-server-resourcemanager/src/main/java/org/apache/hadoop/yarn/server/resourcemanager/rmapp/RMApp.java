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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/**
 * The read interface to an Application in the ResourceManager. Take a
 * look at {@link RMAppImpl} for its implementation. This interface
 * exposes methods to access various updates in application status/report.
 */
public interface RMApp extends EventHandler<RMAppEvent> {

  /**
   * The application id for this {@link RMApp}.
   * @return the {@link ApplicationId} for this {@link RMApp}.
   */
  ApplicationId getApplicationId();

  /**
   * The current state of the {@link RMApp}.
   * @return the current state {@link RMAppState} for this application.
   */
  RMAppState getState();

  /**
   * The user who submitted this application.
   * @return the user who submitted the application.
   */
  String getUser();

  /**
   * Progress of application.
   * @return the progress of the {@link RMApp}.
   */
  float getProgress();

  /**
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the {@link RMAppAttempt} corresponding to
   *  {@link ApplicationAttemptId}.
   * @param appAttemptId the application attempt id
   * @return  the {@link RMAppAttempt} corresponding to the {@link ApplicationAttemptId}.
   */
  RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId);

  /**
   * Each Application is submitted to a queue decided by {@link
   * ApplicationSubmissionContext#setQueue(String)}.
   * This method returns the queue to which an application was submitted.
   * @return the queue to which the application was submitted to.
   */
  String getQueue();

  /**
   * The name of the application as set in {@link
   * ApplicationSubmissionContext#setApplicationName(String)}.
   * @return the name of the application.
   */
  String getName();

  /**
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the current {@link RMAppAttempt}.
   * @return the current {@link RMAppAttempt}
   */
  RMAppAttempt getCurrentAppAttempt();

  /**
   * To get the status of an application in the RM, this method can be used.
   * @return the {@link ApplicationReport} detailing the status of the application.
   */
  ApplicationReport createAndGetApplicationReport();

  /**
   * Application level metadata is stored in {@link ApplicationStore} whicn
   * can persist the information.
   * @return the {@link ApplicationStore}  for this {@link RMApp}.
   */
  ApplicationStore getApplicationStore();

  /**
   * The finish time of the {@link RMApp}
   * @return the finish time of the application.,
   */
  long getFinishTime();

  /**
   * the start time of the application.
   * @return the start time of the application.
   */
  long getStartTime();

  /**
   * The tracking url for the application master.
   * @return the tracking url for the application master.
   */
  String getTrackingUrl();

  /**
   * the diagnostics information for the application master.
   * @return the diagnostics information for the application master.
   */
  StringBuilder getDiagnostics();

  /**
   * The final finish state of the AM when unregistering as in
   * {@link FinishApplicationMasterRequest#setFinishApplicationStatus(FinalApplicationStatus)}.
   * @return the final finish state of the AM as set in
   * {@link FinishApplicationMasterRequest#setFinishApplicationStatus(FinalApplicationStatus)}.
   */
  FinalApplicationStatus getFinalApplicationStatus();
}
