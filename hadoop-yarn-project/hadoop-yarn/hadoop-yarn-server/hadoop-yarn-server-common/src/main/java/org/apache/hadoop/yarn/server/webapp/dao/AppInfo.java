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

package org.apache.hadoop.yarn.server.webapp.dao;

import static org.apache.hadoop.yarn.util.StringHelper.CSV_JOINER;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Times;

@Public
@Evolving
@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  protected String appId;
  protected String currentAppAttemptId;
  protected String user;
  protected String name;
  protected String queue;
  protected String type;
  protected String host;
  protected int rpcPort;
  protected YarnApplicationState appState;
  protected int runningContainers;
  protected float progress;
  protected String diagnosticsInfo;
  protected String originalTrackingUrl;
  protected String trackingUrl;
  protected FinalApplicationStatus finalAppStatus;
  protected long submittedTime;
  protected long startedTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String applicationTags;
  protected int priority;
  private long allocatedCpuVcores;
  private long allocatedMemoryMB;
  protected boolean unmanagedApplication;
  private String appNodeLabelExpression;
  private String amNodeLabelExpression;

  public AppInfo() {
    // JAXB needs this
  }

  public AppInfo(ApplicationReport app) {
    appId = app.getApplicationId().toString();
    if (app.getCurrentApplicationAttemptId() != null) {
      currentAppAttemptId = app.getCurrentApplicationAttemptId().toString();
    }
    user = app.getUser();
    queue = app.getQueue();
    name = app.getName();
    type = app.getApplicationType();
    host = app.getHost();
    rpcPort = app.getRpcPort();
    appState = app.getYarnApplicationState();
    diagnosticsInfo = app.getDiagnostics();
    trackingUrl = app.getTrackingUrl();
    originalTrackingUrl = app.getOriginalTrackingUrl();
    submittedTime = app.getStartTime();
    startedTime = app.getStartTime();
    finishedTime = app.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    priority = 0;
    if (app.getPriority() != null) {
      priority = app.getPriority().getPriority();
    }
    if (app.getApplicationResourceUsageReport() != null) {
      runningContainers = app.getApplicationResourceUsageReport()
          .getNumUsedContainers();
      if (app.getApplicationResourceUsageReport().getUsedResources() != null) {
        allocatedCpuVcores = app.getApplicationResourceUsageReport()
            .getUsedResources().getVirtualCores();
        allocatedMemoryMB = app.getApplicationResourceUsageReport()
            .getUsedResources().getMemorySize();
      }
    }
    progress = app.getProgress() * 100; // in percent
    if (app.getApplicationTags() != null && !app.getApplicationTags().isEmpty()) {
      this.applicationTags = CSV_JOINER.join(app.getApplicationTags());
    }
    unmanagedApplication = app.isUnmanagedApp();
    appNodeLabelExpression = app.getAppNodeLabelExpression();
    amNodeLabelExpression = app.getAmNodeLabelExpression();
  }

  public String getAppId() {
    return appId;
  }

  public String getCurrentAppAttemptId() {
    return currentAppAttemptId;
  }

  public String getUser() {
    return user;
  }

  public String getName() {
    return name;
  }

  public String getQueue() {
    return queue;
  }

  public String getType() {
    return type;
  }

  public String getHost() {
    return host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public YarnApplicationState getAppState() {
    return appState;
  }

  public int getRunningContainers() {
    return runningContainers;
  }

  public long getAllocatedCpuVcores() {
    return allocatedCpuVcores;
  }

  public long getAllocatedMemoryMB() {
    return allocatedMemoryMB;
  }

  public float getProgress() {
    return progress;
  }

  public String getDiagnosticsInfo() {
    return diagnosticsInfo;
  }

  public String getOriginalTrackingUrl() {
    return originalTrackingUrl;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public FinalApplicationStatus getFinalAppStatus() {
    return finalAppStatus;
  }

  public long getSubmittedTime() {
    return submittedTime;
  }

  public long getStartedTime() {
    return startedTime;
  }

  public long getFinishedTime() {
    return finishedTime;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public String getApplicationTags() {
    return applicationTags;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public int getPriority() {
    return priority;
  }

  public String getAppNodeLabelExpression() {
    return appNodeLabelExpression;
  }

  public String getAmNodeLabelExpression() {
    return amNodeLabelExpression;
  }
}
