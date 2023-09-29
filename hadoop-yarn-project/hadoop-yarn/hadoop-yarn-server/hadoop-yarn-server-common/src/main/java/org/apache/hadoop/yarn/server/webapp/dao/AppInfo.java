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
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.StringHelper;

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
  private long submittedTime;
  protected long startedTime;
  private long launchTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String applicationTags;
  protected int priority;
  private long allocatedCpuVcores;
  private long allocatedMemoryMB;
  private long allocatedGpus;
  private long reservedCpuVcores;
  private long reservedMemoryMB;
  private long reservedGpus;
  protected boolean unmanagedApplication;
  private String appNodeLabelExpression;
  private String amNodeLabelExpression;
  private String aggregateResourceAllocation;
  private String aggregatePreemptedResourceAllocation;

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
    submittedTime = app.getSubmitTime();
    startedTime = app.getStartTime();
    launchTime = app.getLaunchTime();
    finishedTime = app.getFinishTime();
    elapsedTime = Times.elapsed(startedTime, finishedTime);
    finalAppStatus = app.getFinalApplicationStatus();
    priority = 0;
    if (app.getPriority() != null) {
      priority = app.getPriority().getPriority();
    }
    ApplicationResourceUsageReport usageReport =
        app.getApplicationResourceUsageReport();
    if (usageReport != null) {
      runningContainers = usageReport
          .getNumUsedContainers();
      if (usageReport.getUsedResources() != null) {
        allocatedCpuVcores = usageReport
            .getUsedResources().getVirtualCores();
        allocatedMemoryMB = usageReport
            .getUsedResources().getMemorySize();
        reservedCpuVcores = usageReport
            .getReservedResources().getVirtualCores();
        reservedMemoryMB = usageReport
            .getReservedResources().getMemorySize();
        Integer gpuIndex = ResourceUtils.getResourceTypeIndex()
            .get(ResourceInformation.GPU_URI);
        allocatedGpus = -1;
        reservedGpus = -1;
        if (gpuIndex != null) {
          allocatedGpus = usageReport.getUsedResources()
              .getResourceValue(ResourceInformation.GPU_URI);
          reservedGpus = usageReport.getReservedResources()
              .getResourceValue(ResourceInformation.GPU_URI);
        }
      }
      aggregateResourceAllocation = StringHelper.getResourceSecondsString(
          usageReport.getResourceSecondsMap());
      aggregatePreemptedResourceAllocation = StringHelper
        .getResourceSecondsString(usageReport.getPreemptedResourceSecondsMap());
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

  public long getAllocatedGpus() {
    return allocatedGpus;
  }

  public long getReservedCpuVcores() {
    return reservedCpuVcores;
  }

  public long getReservedMemoryMB() {
    return reservedMemoryMB;
  }

  public long getReservedGpus() {
    return reservedGpus;
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

  public long getLaunchTime() {
    return launchTime;
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

  public String getAggregateResourceAllocation() {
    return aggregateResourceAllocation;
  }

  public String getAggregatePreemptedResourceAllocation() {
    return aggregatePreemptedResourceAllocation;
  }
}
