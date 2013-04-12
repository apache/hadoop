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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ClientToken;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * Interface to an Application Attempt in the Resource Manager.
 * A {@link RMApp} can have multiple app attempts based on
 * {@link YarnConfiguration#RM_AM_MAX_RETRIES}. For specific
 * implementation take a look at {@link RMAppAttemptImpl}.
 */
public interface RMAppAttempt extends EventHandler<RMAppAttemptEvent> {

  /**
   * Get the application attempt id for this {@link RMAppAttempt}.
   * @return the {@link ApplicationAttemptId} for this RM attempt.
   */
  ApplicationAttemptId getAppAttemptId();

  /**
   * The state of the {@link RMAppAttempt}.
   * @return the state {@link RMAppAttemptState} of this {@link RMAppAttempt}
   */
  RMAppAttemptState getAppAttemptState();

  /**
   * The host on which the {@link RMAppAttempt} is running/ran on.
   * @return the host on which the {@link RMAppAttempt} ran/is running on.
   */
  String getHost();

  /**
   * The rpc port of the {@link RMAppAttempt}.
   * @return the rpc port of the {@link RMAppAttempt} to which the clients can connect
   * to.
   */
  int getRpcPort();

  /**
   * The url at which the status of the application attempt can be accessed.
   * @return the url at which the status of the attempt can be accessed.
   */
  String getTrackingUrl();

  /**
   * The original url at which the status of the application attempt can be 
   * accessed. This url is not fronted by a proxy. This is only intended to be
   * used by the proxy.
   * @return the url at which the status of the attempt can be accessed and is
   * not fronted by a proxy.
   */
  String getOriginalTrackingUrl();

  /**
   * The base to be prepended to web URLs that are not relative, and the user
   * has been checked.
   * @return the base URL to be prepended to web URLs that are not relative.
   */
  String getWebProxyBase();

  /**
   * The token required by the clients to talk to the application attempt
   * @return the token required by the clients to talk to the application attempt
   */
  ClientToken getClientToken();

  /**
   * Diagnostics information for the application attempt.
   * @return diagnostics information for the application attempt.
   */
  String getDiagnostics();

  /**
   * Progress for the application attempt.
   * @return the progress for this {@link RMAppAttempt}
   */
  float getProgress();

  /**
   * The final status set by the AM.
   * @return the final status that is set by the AM when unregistering itself. Can return a null 
   * if the AM has not unregistered itself. 
   */
  FinalApplicationStatus getFinalApplicationStatus();

  /**
   * Nodes on which the containers for this {@link RMAppAttempt} ran.
   * @return the set of nodes that ran any containers from this {@link RMAppAttempt}
   */
  Set<NodeId> getRanNodes();

  /**
   * Return a list of the last set of finished containers, resetting the
   * finished containers to empty.
   * @return the list of just finished containers, re setting the finished containers.
   */
  List<ContainerStatus> pullJustFinishedContainers();

  /**
   * Return the list of last set of finished containers. This does not reset the
   * finished containers.
   * @return the list of just finished contianers, this does not reset the
   * finished containers.
   */
  List<ContainerStatus> getJustFinishedContainers();

  /**
   * The container on which the Application Master is running.
   * @return the {@link Container} on which the application master is running.
   */
  Container getMasterContainer();

  /**
   * The application submission context for this {@link RMAppAttempt}.
   * @return the application submission context for this Application.
   */
  ApplicationSubmissionContext getSubmissionContext();

  /**
   * Get application container and resource usage information.
   * @return an ApplicationResourceUsageReport object.
   */
  ApplicationResourceUsageReport getApplicationResourceUsageReport();

  /**
   * the start time of the application.
   * @return the start time of the application.
   */
  long getStartTime();
}
