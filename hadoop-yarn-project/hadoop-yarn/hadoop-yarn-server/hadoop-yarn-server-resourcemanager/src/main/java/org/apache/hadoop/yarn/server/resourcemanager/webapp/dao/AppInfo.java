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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields.DeSelectType;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

@XmlRootElement(name = "app")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  @XmlTransient
  protected String appIdNum;
  @XmlTransient
  protected boolean trackingUrlIsNotReady;
  @XmlTransient
  protected String trackingUrlPretty;
  @XmlTransient
  protected boolean amContainerLogsExist = false;
  @XmlTransient
  protected ApplicationId applicationId;
  @XmlTransient
  private String schemePrefix;

  // these are ok for any user to see
  protected String id;
  protected String user;
  private String name;
  protected String queue;
  private YarnApplicationState state;
  protected FinalApplicationStatus finalStatus;
  protected float progress;
  protected String trackingUI;
  protected String trackingUrl;
  protected String diagnostics;
  protected long clusterId;
  protected String applicationType;
  protected String applicationTags = "";
  protected int priority;

  // these are only allowed if acls allow
  protected long startedTime;
  protected long finishedTime;
  protected long elapsedTime;
  protected String amContainerLogs;
  protected String amHostHttpAddress;
  private String amRPCAddress;
  private long allocatedMB;
  private long allocatedVCores;
  private long reservedMB;
  private long reservedVCores;
  private int runningContainers;
  private long memorySeconds;
  private long vcoreSeconds;
  protected float queueUsagePercentage;
  protected float clusterUsagePercentage;
  protected Map<String, Long> resourceSecondsMap;

  // preemption info fields
  private long preemptedResourceMB;
  private long preemptedResourceVCores;
  private int numNonAMContainerPreempted;
  private int numAMContainerPreempted;
  private long preemptedMemorySeconds;
  private long preemptedVcoreSeconds;
  protected Map<String, Long> preemptedResourceSecondsMap;

  // list of resource requests
  @XmlElement(name = "resourceRequests")
  private List<ResourceRequestInfo> resourceRequests =
      new ArrayList<ResourceRequestInfo>();

  protected LogAggregationStatus logAggregationStatus;
  protected boolean unmanagedApplication;
  protected String appNodeLabelExpression;
  protected String amNodeLabelExpression;

  protected ResourcesInfo resourceInfo = null;
  private AppTimeoutsInfo timeouts;

  public AppInfo() {
  } // JAXB needs this

  public AppInfo(ResourceManager rm, RMApp app, Boolean hasAccess,
      String schemePrefix) {
    this(rm, app, hasAccess, schemePrefix, new DeSelectFields());
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public AppInfo(ResourceManager rm, RMApp app, Boolean hasAccess,
      String schemePrefix, DeSelectFields deSelects) {
    this.schemePrefix = schemePrefix;
    if (app != null) {
      String trackingUrl = app.getTrackingUrl();
      this.state = app.createApplicationState();
      this.trackingUrlIsNotReady = trackingUrl == null || trackingUrl.isEmpty()
          || YarnApplicationState.NEW == this.state
          || YarnApplicationState.NEW_SAVING == this.state
          || YarnApplicationState.SUBMITTED == this.state
          || YarnApplicationState.ACCEPTED == this.state;
      this.trackingUI = this.trackingUrlIsNotReady ? "UNASSIGNED"
          : (app.getFinishTime() == 0 ? "ApplicationMaster" : "History");
      if (!trackingUrlIsNotReady) {
        this.trackingUrl =
            WebAppUtils.getURLWithScheme(schemePrefix, trackingUrl);
        this.trackingUrlPretty = this.trackingUrl;
      } else {
        this.trackingUrlPretty = "UNASSIGNED";
      }
      this.applicationId = app.getApplicationId();
      this.applicationType = app.getApplicationType();
      this.appIdNum = String.valueOf(app.getApplicationId().getId());
      this.id = app.getApplicationId().toString();
      this.user = app.getUser().toString();
      this.name = app.getName().toString();
      this.queue = app.getQueue().toString();
      this.priority = 0;

      if (app.getApplicationPriority() != null) {
        this.priority = app.getApplicationPriority().getPriority();
      }
      this.progress = app.getProgress() * 100;
      this.diagnostics = app.getDiagnostics().toString();
      if (diagnostics == null || diagnostics.isEmpty()) {
        this.diagnostics = "";
      }
      if (app.getApplicationTags() != null
          && !app.getApplicationTags().isEmpty()) {
        this.applicationTags = Joiner.on(',').join(app.getApplicationTags());
      }
      this.finalStatus = app.getFinalApplicationStatus();
      this.clusterId = ResourceManager.getClusterTimeStamp();
      if (hasAccess) {
        this.startedTime = app.getStartTime();
        this.finishedTime = app.getFinishTime();
        this.elapsedTime =
            Times.elapsed(app.getStartTime(), app.getFinishTime());
        this.logAggregationStatus = app.getLogAggregationStatusForAppReport();
        RMAppAttempt attempt = app.getCurrentAppAttempt();
        if (attempt != null) {
          Container masterContainer = attempt.getMasterContainer();
          if (masterContainer != null) {
            this.amContainerLogsExist = true;
            this.amContainerLogs = WebAppUtils.getRunningLogURL(
                schemePrefix + masterContainer.getNodeHttpAddress(),
                masterContainer.getId().toString(), app.getUser());
            this.amHostHttpAddress = masterContainer.getNodeHttpAddress();
          }

          this.amRPCAddress = getAmRPCAddressFromRMAppAttempt(attempt);

          ApplicationResourceUsageReport resourceReport =
              attempt.getApplicationResourceUsageReport();
          if (resourceReport != null) {
            Resource usedResources = resourceReport.getUsedResources();
            Resource reservedResources = resourceReport.getReservedResources();
            allocatedMB = usedResources.getMemorySize();
            allocatedVCores = usedResources.getVirtualCores();
            reservedMB = reservedResources.getMemorySize();
            reservedVCores = reservedResources.getVirtualCores();
            runningContainers = resourceReport.getNumUsedContainers();
            queueUsagePercentage = resourceReport.getQueueUsagePercentage();
            clusterUsagePercentage = resourceReport.getClusterUsagePercentage();
          }

          /*
           * When the deSelects parameter contains "resourceRequests", it skips
           * returning massive ResourceRequest objects and vice versa. Default
           * behavior is no skipping. (YARN-6280)
           */
          if (!deSelects.contains(DeSelectType.RESOURCE_REQUESTS)) {
            List<ResourceRequest> resourceRequestsRaw = rm.getRMContext()
                .getScheduler().getPendingResourceRequestsForAttempt(
                    attempt.getAppAttemptId());

            if (resourceRequestsRaw != null) {
              for (ResourceRequest req : resourceRequestsRaw) {
                resourceRequests.add(new ResourceRequestInfo(req));
              }
            }
          }
        }
      }

      // copy preemption info fields
      RMAppMetrics appMetrics = app.getRMAppMetrics();
      numAMContainerPreempted = appMetrics.getNumAMContainersPreempted();
      preemptedResourceMB = appMetrics.getResourcePreempted().getMemorySize();
      numNonAMContainerPreempted = appMetrics.getNumNonAMContainersPreempted();
      preemptedResourceVCores =
          appMetrics.getResourcePreempted().getVirtualCores();
      memorySeconds = appMetrics.getMemorySeconds();
      vcoreSeconds = appMetrics.getVcoreSeconds();
      resourceSecondsMap = appMetrics.getResourceSecondsMap();
      preemptedMemorySeconds = appMetrics.getPreemptedMemorySeconds();
      preemptedVcoreSeconds = appMetrics.getPreemptedVcoreSeconds();
      preemptedResourceSecondsMap = appMetrics.getPreemptedResourceSecondsMap();
      ApplicationSubmissionContext appSubmissionContext =
          app.getApplicationSubmissionContext();
      unmanagedApplication = appSubmissionContext.getUnmanagedAM();
      appNodeLabelExpression =
          app.getApplicationSubmissionContext().getNodeLabelExpression();
      /*
       * When the deSelects parameter contains "amNodeLabelExpression", objects
       * pertaining to the amNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      if(!deSelects.contains(DeSelectType.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression = (unmanagedApplication) ?
            null :
            app.getAMResourceRequests().get(0).getNodeLabelExpression();
      }
      /*
       * When the deSelects parameter contains "appNodeLabelExpression", objects
       * pertaining to the appNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      if (!deSelects.contains(DeSelectType.APP_NODE_LABEL_EXPRESSION)) {
        appNodeLabelExpression =
            app.getApplicationSubmissionContext().getNodeLabelExpression();
      }
      /*
       * When the deSelects parameter contains "amNodeLabelExpression", objects
       * pertaining to the amNodeLabelExpression are not returned. By default,
       * this is not skipped. (YARN-6871)
       */
      if (!deSelects.contains(DeSelectType.AM_NODE_LABEL_EXPRESSION)) {
        amNodeLabelExpression = (unmanagedApplication) ?
            null :
            app.getAMResourceRequests().get(0).getNodeLabelExpression();
      }

      /*
       * When the deSelects parameter contains "resourceInfo", ResourceInfo
       * objects are not returned. Default behavior is no skipping. (YARN-6871)
       */
      // Setting partition based resource usage of application
      if (!deSelects.contains(DeSelectType.RESOURCE_INFO)) {
        ResourceScheduler scheduler = rm.getRMContext().getScheduler();
        if (scheduler instanceof CapacityScheduler) {
          RMAppAttempt attempt = app.getCurrentAppAttempt();
          if (null != attempt) {
            FiCaSchedulerApp ficaAppAttempt = ((CapacityScheduler) scheduler)
                .getApplicationAttempt(attempt.getAppAttemptId());
            resourceInfo = null != ficaAppAttempt ?
                new ResourcesInfo(ficaAppAttempt.getSchedulingResourceUsage()) :
                null;
          }
        }
      }

      /*
       * When the deSelects parameter contains "appTimeouts", objects pertaining
       * to app timeouts are not returned. By default, this is not skipped.
       * (YARN-6871)
       */
      if (!deSelects.contains(DeSelectType.TIMEOUTS)) {
        Map<ApplicationTimeoutType, Long> applicationTimeouts =
            app.getApplicationTimeouts();
        if (applicationTimeouts.isEmpty()) {
          // If application is not set timeout, lifetime should be sent
          // as default with expiryTime=UNLIMITED and remainingTime=-1
          AppTimeoutInfo timeoutInfo = new AppTimeoutInfo();
          timeoutInfo.setTimeoutType(ApplicationTimeoutType.LIFETIME);
          timeouts = new AppTimeoutsInfo();
          timeouts.add(timeoutInfo);
        } else {
          for (Map.Entry<ApplicationTimeoutType, Long> entry : app
              .getApplicationTimeouts().entrySet()) {
            AppTimeoutInfo timeout = new AppTimeoutInfo();
            timeout.setTimeoutType(entry.getKey());
            long timeoutInMillis = entry.getValue().longValue();
            timeout.setExpiryTime(Times.formatISO8601(timeoutInMillis));
            if (app.isAppInCompletedStates()) {
              timeout.setRemainingTime(0);
            } else {
              timeout.setRemainingTime(Math.max(
                  (timeoutInMillis - System.currentTimeMillis()) / 1000, 0));
            }
            timeouts.add(timeout);
          }
        }
      }
    }
  }

  public boolean isTrackingUrlReady() {
    return !this.trackingUrlIsNotReady;
  }

  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  public String getAppId() {
    return this.id;
  }

  public String getAppIdNum() {
    return this.appIdNum;
  }

  public String getUser() {
    return this.user;
  }

  public String getQueue() {
    return this.queue;
  }

  public String getName() {
    return this.name;
  }

  public YarnApplicationState getState() {
    return this.state;
  }

  public float getProgress() {
    return this.progress;
  }

  public String getTrackingUI() {
    return this.trackingUI;
  }

  public String getNote() {
    return this.diagnostics;
  }

  public FinalApplicationStatus getFinalStatus() {
    return this.finalStatus;
  }

  public String getTrackingUrl() {
    return this.trackingUrl;
  }

  public String getTrackingUrlPretty() {
    return this.trackingUrlPretty;
  }

  public long getStartTime() {
    return this.startedTime;
  }

  public long getFinishTime() {
    return this.finishedTime;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

  public String getAMContainerLogs() {
    return this.amContainerLogs;
  }

  public String getAMHostHttpAddress() {
    return this.amHostHttpAddress;
  }

  public String getAmRPCAddress() {
    return amRPCAddress;
  }

  static public String getAmRPCAddressFromRMAppAttempt(RMAppAttempt attempt) {
    String amRPCAddress = null;
    if (attempt != null) {
      String amHost = attempt.getHost();
      int amRpcPort = attempt.getRpcPort();
      if (!"N/A".equals(amHost) && amRpcPort != -1) {
        amRPCAddress = amHost + ":" + amRpcPort;
      }
    }
    return amRPCAddress;
  }

  public boolean amContainerLogsExist() {
    return this.amContainerLogsExist;
  }

  public long getClusterId() {
    return this.clusterId;
  }

  public String getApplicationType() {
    return this.applicationType;
  }

  public String getApplicationTags() {
    return this.applicationTags;
  }

  public int getRunningContainers() {
    return this.runningContainers;
  }

  public long getAllocatedMB() {
    return this.allocatedMB;
  }

  public long getAllocatedVCores() {
    return this.allocatedVCores;
  }

  public long getReservedMB() {
    return this.reservedMB;
  }

  public long getReservedVCores() {
    return this.reservedVCores;
  }

  public long getPreemptedMB() {
    return preemptedResourceMB;
  }

  public long getPreemptedVCores() {
    return preemptedResourceVCores;
  }

  public int getNumNonAMContainersPreempted() {
    return numNonAMContainerPreempted;
  }
  
  public int getNumAMContainersPreempted() {
    return numAMContainerPreempted;
  }

  public long getMemorySeconds() {
    return memorySeconds;
  }

  public long getVcoreSeconds() {
    return vcoreSeconds;
  }

  public Map<String, Long> getResourceSecondsMap() {
    return resourceSecondsMap;
  }

  public long getPreemptedMemorySeconds() {
    return preemptedMemorySeconds;
  }

  public long getPreemptedVcoreSeconds() {
    return preemptedVcoreSeconds;
  }

  public Map<String, Long> getPreemptedResourceSecondsMap() {
    return preemptedResourceSecondsMap;
  }
  
  public List<ResourceRequestInfo> getResourceRequests() {
    return this.resourceRequests;
  }

  public void setResourceRequests(List<ResourceRequestInfo> resourceRequests) {
    this.resourceRequests = resourceRequests;
  }

  public LogAggregationStatus getLogAggregationStatus() {
    return this.logAggregationStatus;
  }

  public boolean isUnmanagedApp() {
    return unmanagedApplication;
  }

  public int getPriority() {
    return this.priority;
  }

  public String getAppNodeLabelExpression() {
    return this.appNodeLabelExpression;
  }

  public String getAmNodeLabelExpression() {
    return this.amNodeLabelExpression;
  }

  public ResourcesInfo getResourceInfo() {
    return resourceInfo;
  }

  public long getPreemptedResourceMB() {
    return preemptedResourceMB;
  }

  public void setPreemptedResourceMB(long preemptedResourceMB) {
    this.preemptedResourceMB = preemptedResourceMB;
  }

  public long getPreemptedResourceVCores() {
    return preemptedResourceVCores;
  }

  public void setPreemptedResourceVCores(long preemptedResourceVCores) {
    this.preemptedResourceVCores = preemptedResourceVCores;
  }

  public int getNumNonAMContainerPreempted() {
    return numNonAMContainerPreempted;
  }

  public void setNumNonAMContainerPreempted(int numNonAMContainerPreempted) {
    this.numNonAMContainerPreempted = numNonAMContainerPreempted;
  }

  public int getNumAMContainerPreempted() {
    return numAMContainerPreempted;
  }

  public void setNumAMContainerPreempted(int numAMContainerPreempted) {
    this.numAMContainerPreempted = numAMContainerPreempted;
  }

  public void setPreemptedMemorySeconds(long preemptedMemorySeconds) {
    this.preemptedMemorySeconds = preemptedMemorySeconds;
  }

  public void setPreemptedVcoreSeconds(long preemptedVcoreSeconds) {
    this.preemptedVcoreSeconds = preemptedVcoreSeconds;
  }

  public void setAllocatedMB(long allocatedMB) {
    this.allocatedMB = allocatedMB;
  }

  public void setAllocatedVCores(long allocatedVCores) {
    this.allocatedVCores = allocatedVCores;
  }

  public void setReservedMB(long reservedMB) {
    this.reservedMB = reservedMB;
  }

  public void setReservedVCores(long reservedVCores) {
    this.reservedVCores = reservedVCores;
  }

  public void setRunningContainers(int runningContainers) {
    this.runningContainers = runningContainers;
  }

  public void setMemorySeconds(long memorySeconds) {
    this.memorySeconds = memorySeconds;
  }

  public void setVcoreSeconds(long vcoreSeconds) {
    this.vcoreSeconds = vcoreSeconds;
  }

  public void setAppId(String appId) {
    this.id = appId;
  }

  @VisibleForTesting
  public void setAMHostHttpAddress(String amHost) {
    this.amHostHttpAddress = amHost;
  }

  public void setState(YarnApplicationState state) {
    this.state = state;
  }

  public void setName(String name) {
    this.name = name;
  }
}
