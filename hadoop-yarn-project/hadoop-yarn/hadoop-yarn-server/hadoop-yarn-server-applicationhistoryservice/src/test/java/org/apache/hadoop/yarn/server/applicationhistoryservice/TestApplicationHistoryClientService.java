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
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestApplicationHistoryClientService extends
    ApplicationHistoryStoreTestUtils {

  ApplicationHistoryServer historyServer = null;
  String expectedLogUrl = null;

  @Before
  public void setup() {
    historyServer = new ApplicationHistoryServer();
    Configuration config = new YarnConfiguration();
    expectedLogUrl = WebAppUtils.getHttpSchemePrefix(config) +
        WebAppUtils.getAHSWebAppURLWithoutScheme(config) +
        "/applicationhistory/logs/localhost:0/container_0_0001_01_000001/" +
        "container_0_0001_01_000001/test user";
    config.setClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
      MemoryApplicationHistoryStore.class, ApplicationHistoryStore.class);
    historyServer.init(config);
    historyServer.start();
    store =
        ((ApplicationHistoryManagerImpl) historyServer.getApplicationHistory())
          .getHistoryStore();
  }

  @After
  public void tearDown() throws Exception {
    historyServer.stop();
  }

  @Test
  public void testApplicationReport() throws IOException, YarnException {
    ApplicationId appId = null;
    appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    writeApplicationFinishData(appId);
    GetApplicationReportRequest request =
        GetApplicationReportRequest.newInstance(appId);
    GetApplicationReportResponse response =
        historyServer.getClientService().getClientHandler()
          .getApplicationReport(request);
    ApplicationReport appReport = response.getApplicationReport();
    Assert.assertNotNull(appReport);
    Assert.assertEquals("application_0_0001", appReport.getApplicationId()
      .toString());
    Assert.assertEquals("test type", appReport.getApplicationType().toString());
    Assert.assertEquals("test queue", appReport.getQueue().toString());
  }

  @Test
  public void testApplications() throws IOException, YarnException {
    ApplicationId appId = null;
    appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    writeApplicationFinishData(appId);
    ApplicationId appId1 = ApplicationId.newInstance(0, 2);
    writeApplicationStartData(appId1);
    writeApplicationFinishData(appId1);
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    GetApplicationsResponse response =
        historyServer.getClientService().getClientHandler()
          .getApplications(request);
    List<ApplicationReport> appReport = response.getApplicationList();
    Assert.assertNotNull(appReport);
    Assert.assertEquals(appId, appReport.get(0).getApplicationId());
    Assert.assertEquals(appId1, appReport.get(1).getApplicationId());
  }

  @Test
  public void testApplicationAttemptReport() throws IOException, YarnException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    writeApplicationAttemptStartData(appAttemptId);
    writeApplicationAttemptFinishData(appAttemptId);
    GetApplicationAttemptReportRequest request =
        GetApplicationAttemptReportRequest.newInstance(appAttemptId);
    GetApplicationAttemptReportResponse response =
        historyServer.getClientService().getClientHandler()
          .getApplicationAttemptReport(request);
    ApplicationAttemptReport attemptReport =
        response.getApplicationAttemptReport();
    Assert.assertNotNull(attemptReport);
    Assert.assertEquals("appattempt_0_0001_000001", attemptReport
      .getApplicationAttemptId().toString());
  }

  @Test
  public void testApplicationAttempts() throws IOException, YarnException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ApplicationAttemptId appAttemptId1 =
        ApplicationAttemptId.newInstance(appId, 2);
    writeApplicationAttemptStartData(appAttemptId);
    writeApplicationAttemptFinishData(appAttemptId);
    writeApplicationAttemptStartData(appAttemptId1);
    writeApplicationAttemptFinishData(appAttemptId1);
    GetApplicationAttemptsRequest request =
        GetApplicationAttemptsRequest.newInstance(appId);
    GetApplicationAttemptsResponse response =
        historyServer.getClientService().getClientHandler()
          .getApplicationAttempts(request);
    List<ApplicationAttemptReport> attemptReports =
        response.getApplicationAttemptList();
    Assert.assertNotNull(attemptReports);
    Assert.assertEquals(appAttemptId, attemptReports.get(0)
      .getApplicationAttemptId());
    Assert.assertEquals(appAttemptId1, attemptReports.get(1)
      .getApplicationAttemptId());
  }

  @Test
  public void testContainerReport() throws IOException, YarnException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    writeContainerStartData(containerId);
    writeContainerFinishData(containerId);
    writeApplicationFinishData(appId);
    GetContainerReportRequest request =
        GetContainerReportRequest.newInstance(containerId);
    GetContainerReportResponse response =
        historyServer.getClientService().getClientHandler()
          .getContainerReport(request);
    ContainerReport container = response.getContainerReport();
    Assert.assertNotNull(container);
    Assert.assertEquals(containerId, container.getContainerId());
    Assert.assertEquals(expectedLogUrl, container.getLogUrl());
  }

  @Test
  public void testContainers() throws IOException, YarnException {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    writeApplicationStartData(appId);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    ContainerId containerId1 = ContainerId.newInstance(appAttemptId, 2);
    writeContainerStartData(containerId);
    writeContainerFinishData(containerId);
    writeContainerStartData(containerId1);
    writeContainerFinishData(containerId1);
    writeApplicationFinishData(appId);
    GetContainersRequest request =
        GetContainersRequest.newInstance(appAttemptId);
    GetContainersResponse response =
        historyServer.getClientService().getClientHandler()
          .getContainers(request);
    List<ContainerReport> containers = response.getContainerList();
    Assert.assertNotNull(containers);
    Assert.assertEquals(containerId, containers.get(1).getContainerId());
    Assert.assertEquals(containerId1, containers.get(0).getContainerId());
  }
}
