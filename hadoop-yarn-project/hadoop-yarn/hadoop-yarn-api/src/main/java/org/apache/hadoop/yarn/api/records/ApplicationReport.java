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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p><code>ApplicationReport</code> is a report of an application.</p>
 *
 * <p>It includes details such as:
 *   <ul>
 *     <li>{@link ApplicationId} of the application.</li>
 *     <li>Applications user.</li>
 *     <li>Application queue.</li>
 *     <li>Application name.</li>
 *     <li>Host on which the <code>ApplicationMaster</code> is running.</li>
 *     <li>RPC port of the <code>ApplicationMaster</code>.</li>
 *     <li>Tracking URL.</li>
 *     <li>{@link YarnApplicationState} of the application.</li>
 *     <li>Diagnostic information in case of errors.</li>
 *     <li>Start time of the application.</li>
 *     <li>Client token of the application (if security is enabled).</li>
 *   </ul>
 * </p>
 *
 * @see ClientRMProtocol#getApplicationReport(org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest)
 */
@Public
@Stable
public interface ApplicationReport {

  /**
   * Get the <code>ApplicationId</code> of the application.
   * @return <code>ApplicationId</code> of the application
   */
  @Public
  @Stable
  ApplicationId getApplicationId();

  @Private
  @Unstable
  void setApplicationId(ApplicationId applicationId);
  
  /**
   * Get the <code>ApplicationAttemptId</code> of the current
   * attempt of the application
   * @return <code>ApplicationAttemptId</code> of the attempt
   */
  @Private
  @Unstable
  ApplicationAttemptId getCurrentApplicationAttemptId();
  
  @Private
  @Unstable
  void setCurrentApplicationAttemptId(ApplicationAttemptId applicationAttemptId);

  /**
   * Get the <em>user</em> who submitted the application.
   * @return <em>user</em> who submitted the application
   */
  @Public
  @Stable
  String getUser();

  @Private
  @Unstable
  void setUser(String user);

  /**
   * Get the <em>queue</em> to which the application was submitted.
   * @return <em>queue</em> to which the application was submitted
   */
  @Public
  @Stable
  String getQueue();

  @Private
  @Unstable
  void setQueue(String queue);

  /**
   * Get the user-defined <em>name</em> of the application.
   * @return <em>name</em> of the application
   */
  @Public
  @Stable
  String getName();

  @Private
  @Unstable
  void setName(String name);

  /**
   * Get the <em>host</em> on which the <code>ApplicationMaster</code>
   * is running.
   * @return <em>host</em> on which the <code>ApplicationMaster</code>
   *         is running
   */
  @Public
  @Stable
  String getHost();

  @Private
  @Unstable
  void setHost(String host);

  /**
   * Get the <em>RPC port</em> of the <code>ApplicationMaster</code>.
   * @return <em>RPC port</em> of the <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  int getRpcPort();

  @Private
  @Unstable
  void setRpcPort(int rpcPort);

  /**
   * Get the <em>client token</em> for communicating with the
   * <code>ApplicationMaster</code>.
   * @return <em>client token</em> for communicating with the
   * <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  ClientToken getClientToken();

  @Private
  @Unstable
  void setClientToken(ClientToken clientToken);

  /**
   * Get the <code>YarnApplicationState</code> of the application.
   * @return <code>YarnApplicationState</code> of the application
   */
  @Public
  @Stable
  YarnApplicationState getYarnApplicationState();

  @Private
  @Unstable
  void setYarnApplicationState(YarnApplicationState state);

  /**
   * Get  the <em>diagnositic information</em> of the application in case of
   * errors.
   * @return <em>diagnositic information</em> of the application in case
   *         of errors
   */
  @Public
  @Stable
  String getDiagnostics();

  @Private
  @Unstable
  void setDiagnostics(String diagnostics);

  /**
   * Get the <em>tracking url</em> for the application.
   * @return <em>tracking url</em> for the application
   */
  @Public
  @Stable
  String getTrackingUrl();

  @Private
  @Unstable
  void setTrackingUrl(String url);
  
  /**
   * Get the original not-proxied <em>tracking url</em> for the application.
   * This is intended to only be used by the proxy itself.
   * @return the original not-proxied <em>tracking url</em> for the application
   */
  @Private
  @Unstable
  String getOriginalTrackingUrl();

  @Private
  @Unstable
  void setOriginalTrackingUrl(String url);

  /**
   * Get the <em>start time</em> of the application.
   * @return <em>start time</em> of the application
   */
  @Public
  @Stable
  long getStartTime();

  @Private
  @Unstable
  void setStartTime(long startTime);

  /**
   * Get the <em>finish time</em> of the application.
   * @return <em>finish time</em> of the application
   */
  @Public
  @Stable
  long getFinishTime();

  @Private
  @Unstable
  void setFinishTime(long finishTime);


  /**
   * Get the <em>final finish status</em> of the application.
   * @return <em>final finish status</em> of the application
   */
  @Public
  @Stable
  FinalApplicationStatus getFinalApplicationStatus();

  @Private
  @Unstable
  void setFinalApplicationStatus(FinalApplicationStatus finishState);

  /**
   * Retrieve the structure containing the job resources for this application
   * @return the job resources structure for this application
   */
  @Public
  @Stable
  ApplicationResourceUsageReport getApplicationResourceUsageReport();

  /**
   * Store the structure containing the job resources for this application
   * @param appResources structure for this application
   */
  @Private
  @Unstable
  void setApplicationResourceUsageReport(ApplicationResourceUsageReport appResources);
}
