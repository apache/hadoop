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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * The class contains all the fields that are stored persistently for
 * <code>RMApp</code>.
 */
@Public
@Unstable
public class ApplicationHistoryData {

  private ApplicationId applicationId;

  private String applicationName;

  private String applicationType;

  private String user;

  private String queue;

  private long submitTime;

  private long startTime;

  private long finishTime;

  private String diagnosticsInfo;

  private FinalApplicationStatus finalApplicationStatus;

  private YarnApplicationState yarnApplicationState;

  @Public
  @Unstable
  public static ApplicationHistoryData newInstance(ApplicationId applicationId,
      String applicationName, String applicationType, String queue,
      String user, long submitTime, long startTime, long finishTime,
      String diagnosticsInfo, FinalApplicationStatus finalApplicationStatus,
      YarnApplicationState yarnApplicationState) {
    ApplicationHistoryData appHD = new ApplicationHistoryData();
    appHD.setApplicationId(applicationId);
    appHD.setApplicationName(applicationName);
    appHD.setApplicationType(applicationType);
    appHD.setQueue(queue);
    appHD.setUser(user);
    appHD.setSubmitTime(submitTime);
    appHD.setStartTime(startTime);
    appHD.setFinishTime(finishTime);
    appHD.setDiagnosticsInfo(diagnosticsInfo);
    appHD.setFinalApplicationStatus(finalApplicationStatus);
    appHD.setYarnApplicationState(yarnApplicationState);
    return appHD;
  }

  @Public
  @Unstable
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Public
  @Unstable
  public void setApplicationId(ApplicationId applicationId) {
    this.applicationId = applicationId;
  }

  @Public
  @Unstable
  public String getApplicationName() {
    return applicationName;
  }

  @Public
  @Unstable
  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  @Public
  @Unstable
  public String getApplicationType() {
    return applicationType;
  }

  @Public
  @Unstable
  public void setApplicationType(String applicationType) {
    this.applicationType = applicationType;
  }

  @Public
  @Unstable
  public String getUser() {
    return user;
  }

  @Public
  @Unstable
  public void setUser(String user) {
    this.user = user;
  }

  @Public
  @Unstable
  public String getQueue() {
    return queue;
  }

  @Public
  @Unstable
  public void setQueue(String queue) {
    this.queue = queue;
  }

  @Public
  @Unstable
  public long getSubmitTime() {
    return submitTime;
  }

  @Public
  @Unstable
  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  @Public
  @Unstable
  public long getStartTime() {
    return startTime;
  }

  @Public
  @Unstable
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Public
  @Unstable
  public long getFinishTime() {
    return finishTime;
  }

  @Public
  @Unstable
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
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
  public YarnApplicationState getYarnApplicationState() {
    return this.yarnApplicationState;
  }

  @Public
  @Unstable
  public void
      setYarnApplicationState(YarnApplicationState yarnApplicationState) {
    this.yarnApplicationState = yarnApplicationState;
  }

}
