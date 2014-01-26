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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;

/**
 * The class contains all the fields that are stored persistently for
 * <code>RMAppAttempt</code>.
 */
@Public
@Unstable
public class ApplicationAttemptHistoryData {

  private ApplicationAttemptId applicationAttemptId;

  private String host;

  private int rpcPort;

  private String trackingURL;

  private String diagnosticsInfo;

  private FinalApplicationStatus finalApplicationStatus;

  private ContainerId masterContainerId;

  private YarnApplicationAttemptState yarnApplicationAttemptState;

  @Public
  @Unstable
  public static ApplicationAttemptHistoryData newInstance(
      ApplicationAttemptId appAttemptId, String host, int rpcPort,
      ContainerId masterContainerId, String diagnosticsInfo,
      String trackingURL, FinalApplicationStatus finalApplicationStatus,
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    ApplicationAttemptHistoryData appAttemptHD =
        new ApplicationAttemptHistoryData();
    appAttemptHD.setApplicationAttemptId(appAttemptId);
    appAttemptHD.setHost(host);
    appAttemptHD.setRPCPort(rpcPort);
    appAttemptHD.setMasterContainerId(masterContainerId);
    appAttemptHD.setDiagnosticsInfo(diagnosticsInfo);
    appAttemptHD.setTrackingURL(trackingURL);
    appAttemptHD.setFinalApplicationStatus(finalApplicationStatus);
    appAttemptHD.setYarnApplicationAttemptState(yarnApplicationAttemptState);
    return appAttemptHD;
  }

  @Public
  @Unstable
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  @Public
  @Unstable
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    this.applicationAttemptId = applicationAttemptId;
  }

  @Public
  @Unstable
  public String getHost() {
    return host;
  }

  @Public
  @Unstable
  public void setHost(String host) {
    this.host = host;
  }

  @Public
  @Unstable
  public int getRPCPort() {
    return rpcPort;
  }

  @Public
  @Unstable
  public void setRPCPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  @Public
  @Unstable
  public String getTrackingURL() {
    return trackingURL;
  }

  @Public
  @Unstable
  public void setTrackingURL(String trackingURL) {
    this.trackingURL = trackingURL;
  }

  @Public
  @Unstable
  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  @Public
  @Unstable
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    this.diagnosticsInfo = diagnosticsInfo;
  }

  @Public
  @Unstable
  public FinalApplicationStatus getFinalApplicationStatus() {
    return finalApplicationStatus;
  }

  @Public
  @Unstable
  public void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus) {
    this.finalApplicationStatus = finalApplicationStatus;
  }

  @Public
  @Unstable
  public ContainerId getMasterContainerId() {
    return masterContainerId;
  }

  @Public
  @Unstable
  public void setMasterContainerId(ContainerId masterContainerId) {
    this.masterContainerId = masterContainerId;
  }

  @Public
  @Unstable
  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    return yarnApplicationAttemptState;
  }

  @Public
  @Unstable
  public void setYarnApplicationAttemptState(
      YarnApplicationAttemptState yarnApplicationAttemptState) {
    this.yarnApplicationAttemptState = yarnApplicationAttemptState;
  }

}
