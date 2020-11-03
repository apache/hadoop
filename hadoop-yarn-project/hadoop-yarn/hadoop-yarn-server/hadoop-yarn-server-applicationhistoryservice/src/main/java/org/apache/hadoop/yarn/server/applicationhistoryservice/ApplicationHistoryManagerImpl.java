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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationHistoryManagerImpl extends AbstractService implements
    ApplicationHistoryManager {
  private static final Logger LOG =
          LoggerFactory.getLogger(ApplicationHistoryManagerImpl.class);
  private static final String UNAVAILABLE = "N/A";

  private ApplicationHistoryStore historyStore;
  private String serverHttpAddress;

  public ApplicationHistoryManagerImpl() {
    super(ApplicationHistoryManagerImpl.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    LOG.info("ApplicationHistory Init");
    historyStore = createApplicationHistoryStore(conf);
    historyStore.init(conf);
    serverHttpAddress = WebAppUtils.getHttpSchemePrefix(conf) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.info("Starting ApplicationHistory");
    historyStore.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    LOG.info("Stopping ApplicationHistory");
    historyStore.stop();
    super.serviceStop();
  }

  protected ApplicationHistoryStore createApplicationHistoryStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
      YarnConfiguration.APPLICATION_HISTORY_STORE,
      FileSystemApplicationHistoryStore.class,
      ApplicationHistoryStore.class), conf);
  }

  @Override
  public ContainerReport getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException {
    ApplicationReport app =
        getApplication(appAttemptId.getApplicationId());
    return convertToContainerReport(historyStore.getAMContainer(appAttemptId),
        app == null ? null : app.getUser());
  }

  @Override
  public Map<ApplicationId, ApplicationReport> getApplications(long appsNum,
      long appStartedTimeBegin, long appStartedTimeEnd) throws IOException {
    Map<ApplicationId, ApplicationHistoryData> histData =
        historyStore.getAllApplications();
    HashMap<ApplicationId, ApplicationReport> applicationsReport =
        new HashMap<ApplicationId, ApplicationReport>();
    int count = 0;
    for (Entry<ApplicationId, ApplicationHistoryData> entry : histData
      .entrySet()) {
      if (count == appsNum) {
        break;
      }
      long appStartTime = entry.getValue().getStartTime();
      if (appStartTime < appStartedTimeBegin
          || appStartTime > appStartedTimeEnd) {
        continue;
      }
      applicationsReport.put(entry.getKey(),
        convertToApplicationReport(entry.getValue()));
      count++;
    }
    return applicationsReport;
  }

  @Override
  public ApplicationReport getApplication(ApplicationId appId)
      throws IOException {
    return convertToApplicationReport(historyStore.getApplication(appId));
  }

  private ApplicationReport convertToApplicationReport(
      ApplicationHistoryData appHistory) throws IOException {
    ApplicationAttemptId currentApplicationAttemptId = null;
    String trackingUrl = UNAVAILABLE;
    String host = UNAVAILABLE;
    int rpcPort = -1;

    ApplicationAttemptHistoryData lastAttempt =
        getLastAttempt(appHistory.getApplicationId());
    if (lastAttempt != null) {
      currentApplicationAttemptId = lastAttempt.getApplicationAttemptId();
      trackingUrl = lastAttempt.getTrackingURL();
      host = lastAttempt.getHost();
      rpcPort = lastAttempt.getRPCPort();
    }
    return ApplicationReport.newInstance(appHistory.getApplicationId(),
      currentApplicationAttemptId, appHistory.getUser(), appHistory.getQueue(),
      appHistory.getApplicationName(), host, rpcPort, null,
      appHistory.getYarnApplicationState(), appHistory.getDiagnosticsInfo(),
      trackingUrl, appHistory.getStartTime(), appHistory.getSubmitTime(), 0,
      appHistory.getFinishTime(), appHistory.getFinalApplicationStatus(),
      null, "", 100, appHistory.getApplicationType(), null);
  }

  private ApplicationAttemptHistoryData getLastAttempt(ApplicationId appId)
      throws IOException {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> attempts =
        historyStore.getApplicationAttempts(appId);
    ApplicationAttemptId prevMaxAttemptId = null;
    for (ApplicationAttemptId attemptId : attempts.keySet()) {
      if (prevMaxAttemptId == null) {
        prevMaxAttemptId = attemptId;
      } else {
        if (prevMaxAttemptId.getAttemptId() < attemptId.getAttemptId()) {
          prevMaxAttemptId = attemptId;
        }
      }
    }
    return attempts.get(prevMaxAttemptId);
  }

  private ApplicationAttemptReport convertToApplicationAttemptReport(
      ApplicationAttemptHistoryData appAttemptHistory) {
    return ApplicationAttemptReport.newInstance(
      appAttemptHistory.getApplicationAttemptId(), appAttemptHistory.getHost(),
      appAttemptHistory.getRPCPort(), appAttemptHistory.getTrackingURL(), null,
      appAttemptHistory.getDiagnosticsInfo(),
      appAttemptHistory.getYarnApplicationAttemptState(),
      appAttemptHistory.getMasterContainerId());
  }

  @Override
  public ApplicationAttemptReport getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException {
    return convertToApplicationAttemptReport(historyStore
      .getApplicationAttempt(appAttemptId));
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptReport>
      getApplicationAttempts(ApplicationId appId) throws IOException {
    Map<ApplicationAttemptId, ApplicationAttemptHistoryData> histData =
        historyStore.getApplicationAttempts(appId);
    HashMap<ApplicationAttemptId, ApplicationAttemptReport> applicationAttemptsReport =
        new HashMap<ApplicationAttemptId, ApplicationAttemptReport>();
    for (Entry<ApplicationAttemptId, ApplicationAttemptHistoryData> entry : histData
      .entrySet()) {
      applicationAttemptsReport.put(entry.getKey(),
        convertToApplicationAttemptReport(entry.getValue()));
    }
    return applicationAttemptsReport;
  }

  @Override
  public ContainerReport getContainer(ContainerId containerId)
      throws IOException {
    ApplicationReport app =
        getApplication(containerId.getApplicationAttemptId().getApplicationId());
    return convertToContainerReport(historyStore.getContainer(containerId),
        app == null ? null: app.getUser());
  }

  private ContainerReport convertToContainerReport(
      ContainerHistoryData containerHistory, String user) {
    // If the container has the aggregated log, add the server root url
    String logUrl = WebAppUtils.getAggregatedLogURL(
        serverHttpAddress,
        containerHistory.getAssignedNode().toString(),
        containerHistory.getContainerId().toString(),
        containerHistory.getContainerId().toString(),
        user);
    ContainerReport container = ContainerReport.newInstance(
        containerHistory.getContainerId(),
        containerHistory.getAllocatedResource(),
        containerHistory.getAssignedNode(), containerHistory.getPriority(),
        containerHistory.getStartTime(), containerHistory.getFinishTime(),
        containerHistory.getDiagnosticsInfo(), logUrl,
        containerHistory.getContainerExitStatus(),
        containerHistory.getContainerState(), null);
    container.setExposedPorts(containerHistory.getExposedPorts());
    return container;
  }

  @Override
  public Map<ContainerId, ContainerReport> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    ApplicationReport app =
        getApplication(appAttemptId.getApplicationId());
    Map<ContainerId, ContainerHistoryData> histData =
        historyStore.getContainers(appAttemptId);
    HashMap<ContainerId, ContainerReport> containersReport =
        new HashMap<ContainerId, ContainerReport>();
    for (Entry<ContainerId, ContainerHistoryData> entry : histData.entrySet()) {
      containersReport.put(entry.getKey(),
        convertToContainerReport(entry.getValue(),
            app == null ? null : app.getUser()));
    }
    return containersReport;
  }

  @Private
  @VisibleForTesting
  public ApplicationHistoryStore getHistoryStore() {
    return this.historyStore;
  }
}
