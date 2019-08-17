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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.resourcemanager.placement
    .ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * The interface to an Application in the ResourceManager. Take a
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
   * The application submission context for this {@link RMApp}
   * @return the {@link ApplicationSubmissionContext} for this {@link RMApp}
   */
  ApplicationSubmissionContext getApplicationSubmissionContext();

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
   * Reflects a change in the application's queue from the one specified in the
   * {@link ApplicationSubmissionContext}.
   * @param name the new queue name
   */
  void setQueue(String name);

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
   * {@link RMApp} can have multiple application attempts {@link RMAppAttempt}.
   * This method returns the all {@link RMAppAttempt}s for the RMApp.
   * @return all {@link RMAppAttempt}s for the RMApp.
   */
  Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts();

  /**
   * To get the status of an application in the RM, this method can be used.
   * If full access is not allowed then the following fields in the report
   * will be stubbed:
   * <ul>
   *   <li>host - set to "N/A"</li>
   *   <li>RPC port - set to -1</li>
   *   <li>client token - set to "N/A"</li>
   *   <li>diagnostics - set to "N/A"</li>
   *   <li>tracking URL - set to "N/A"</li>
   *   <li>original tracking URL - set to "N/A"</li>
   *   <li>resource usage report - all values are -1</li>
   * </ul>
   *
   * @param clientUserName the user name of the client requesting the report
   * @param allowAccess whether to allow full access to the report
   * @return the {@link ApplicationReport} detailing the status of the application.
   */
  ApplicationReport createAndGetApplicationReport(String clientUserName,
      boolean allowAccess);
  
  /**
   * To receive the collection of all {@link RMNode}s whose updates have been
   * received by the RMApp. Updates can be node becoming lost or becoming
   * healthy etc. The method clears the information from the {@link RMApp}. So
   * each call to this method gives the delta from the previous call.
   * @param updatedNodes Map into which the updates are transferred, with each
   * node updates as the key, and the {@link NodeUpdateType} for that update
   * as the corresponding value.
   * @return the number of nodes added to the {@link Map}
   */
  int pullRMNodeUpdates(Map<RMNode, NodeUpdateType> updatedNodes);

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
   * the submit time of the application.
   * @return the submit time of the application.
   */
  long getSubmitTime();

  /**
   * The launch time of the application.
   * Since getStartTime() returns what is essentially submit time,
   * this new field is to prevent potential backwards compatibility issues.
   * @return the launch time of the application.
   */
  long getLaunchTime();

  /**
   * The tracking url for the application master.
   * @return the tracking url for the application master.
   */
  String getTrackingUrl();

  /**
   * The timeline collector information for the application. It should be used
   * only if the timeline service v.2 is enabled.
   *
   * @return the data for the application's collector, including collector
   * address, RM ID, version and collector token. Return null if the timeline
   * service v.2 is not enabled.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  AppCollectorData getCollectorData();

  /**
   * The timeline collector information to be sent to AM. It should be used
   * only if the timeline service v.2 is enabled.
   *
   * @return collector info, including collector address and collector token.
   * Return null if the timeline service v.2 is not enabled.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  CollectorInfo getCollectorInfo();
  /**
   * The original tracking url for the application master.
   * @return the original tracking url for the application master.
   */
  String getOriginalTrackingUrl();

  /**
   * the diagnostics information for the application master.
   * @return the diagnostics information for the application master.
   */
  StringBuilder getDiagnostics();

  /**
   * The final finish state of the AM when unregistering as in
   * {@link FinishApplicationMasterRequest#setFinalApplicationStatus(FinalApplicationStatus)}.
   * @return the final finish state of the AM as set in
   * {@link FinishApplicationMasterRequest#setFinalApplicationStatus(FinalApplicationStatus)}.
   */
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * The number of max attempts of the application.
   * @return the number of max attempts of the application.
   */
  int getMaxAppAttempts();

  boolean isLogAggregationEnabled();

  boolean isLogAggregationFinished();

  /**
   * Returns the application type
   * @return the application type.
   */
  String getApplicationType();

  /**
   * Get tags for the application
   * @return tags corresponding to the application
   */
  Set<String> getApplicationTags();

  /**
   * Check whether this application's state has been saved to the state store.
   * @return the flag indicating whether the applications's state is stored.
   */
  boolean isAppFinalStateStored();
  
  
  /**
   * Nodes on which the containers for this {@link RMApp} ran.
   * @return the set of nodes that ran any containers from this {@link RMApp}
   * Add more node on which containers for this {@link RMApp} ran
   */
  Set<NodeId> getRanNodes();

  /**
   * Create the external user-facing state of ApplicationMaster from the
   * current state of the {@link RMApp}.
   * @return the external user-facing state of ApplicationMaster.
   */
  YarnApplicationState createApplicationState();
  
  /**
   * Get RMAppMetrics of the {@link RMApp}.
   * 
   * @return metrics
   */
  RMAppMetrics getRMAppMetrics();

  ReservationId getReservationId();
  
  List<ResourceRequest> getAMResourceRequests();

  Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp();

  LogAggregationStatus getLogAggregationStatusForAppReport();
  /**
   * Return the node label expression of the AM container.
   */
  String getAmNodeLabelExpression();

  String getAppNodeLabelExpression();

  CallerContext getCallerContext();

  Map<ApplicationTimeoutType, Long> getApplicationTimeouts();

  /**
   * Get priority of the application.
   * @return priority
   */
  Priority getApplicationPriority();

  /**
   * To verify whether app has reached in its completing/completed states.
   *
   * @return True/False to confirm whether app is in final states
   */
  boolean isAppInCompletedStates();

  /**
   * Get the application -&gt; queue placement context
   * @return ApplicationPlacementContext
   */
  ApplicationPlacementContext getApplicationPlacementContext();

  /**
   * Get the application scheduling environment variables.
   * @return Map of envs related to application scheduling preferences.
   */
  Map<String, String> getApplicationSchedulingEnvs();
}
