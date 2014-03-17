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

package org.apache.hadoop.yarn.client.api.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationHistoryProtocol;
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
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;

public class TestAHSClient {

  @Test
  public void testClientStop() {
    Configuration conf = new Configuration();
    AHSClient client = AHSClient.createAHSClient();
    client.init(conf);
    client.start();
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplications() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports =
        ((MockAHSClient) client).getReports();

    List<ApplicationReport> reports = client.getApplications();
    Assert.assertEquals(reports, expectedReports);

    reports = client.getApplications();
    Assert.assertEquals(reports.size(), 4);
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplicationReport() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports =
        ((MockAHSClient) client).getReports();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationReport report = client.getApplicationReport(applicationId);
    Assert.assertEquals(report, expectedReports.get(0));
    Assert.assertEquals(report.getApplicationId().toString(), expectedReports
      .get(0).getApplicationId().toString());
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplicationAttempts() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    List<ApplicationAttemptReport> reports =
        client.getApplicationAttempts(applicationId);
    Assert.assertNotNull(reports);
    Assert.assertEquals(reports.get(0).getApplicationAttemptId(),
      ApplicationAttemptId.newInstance(applicationId, 1));
    Assert.assertEquals(reports.get(1).getApplicationAttemptId(),
      ApplicationAttemptId.newInstance(applicationId, 2));
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplicationAttempt() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports =
        ((MockAHSClient) client).getReports();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ApplicationAttemptReport report =
        client.getApplicationAttemptReport(appAttemptId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getApplicationAttemptId().toString(),
      expectedReports.get(0).getCurrentApplicationAttemptId().toString());
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetContainers() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    List<ContainerReport> reports = client.getContainers(appAttemptId);
    Assert.assertNotNull(reports);
    Assert.assertEquals(reports.get(0).getContainerId(),
      (ContainerId.newInstance(appAttemptId, 1)));
    Assert.assertEquals(reports.get(1).getContainerId(),
      (ContainerId.newInstance(appAttemptId, 2)));
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetContainerReport() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final AHSClient client = new MockAHSClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports =
        ((MockAHSClient) client).getReports();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    ContainerReport report = client.getContainerReport(containerId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getContainerId().toString(), (ContainerId
      .newInstance(expectedReports.get(0).getCurrentApplicationAttemptId(), 1))
      .toString());
    client.stop();
  }

  private static class MockAHSClient extends AHSClientImpl {
    // private ApplicationReport mockReport;
    private List<ApplicationReport> reports =
        new ArrayList<ApplicationReport>();
    private HashMap<ApplicationId, List<ApplicationAttemptReport>> attempts =
        new HashMap<ApplicationId, List<ApplicationAttemptReport>>();
    private HashMap<ApplicationAttemptId, List<ContainerReport>> containers =
        new HashMap<ApplicationAttemptId, List<ContainerReport>>();
    GetApplicationsResponse mockAppResponse =
        mock(GetApplicationsResponse.class);
    GetApplicationReportResponse mockResponse =
        mock(GetApplicationReportResponse.class);
    GetApplicationAttemptsResponse mockAppAttemptsResponse =
        mock(GetApplicationAttemptsResponse.class);
    GetApplicationAttemptReportResponse mockAttemptResponse =
        mock(GetApplicationAttemptReportResponse.class);
    GetContainersResponse mockContainersResponse =
        mock(GetContainersResponse.class);
    GetContainerReportResponse mockContainerResponse =
        mock(GetContainerReportResponse.class);

    public MockAHSClient() {
      super();
      createAppReports();
    }

    @Override
    public void start() {
      ahsClient = mock(ApplicationHistoryProtocol.class);

      try {
        when(
          ahsClient
            .getApplicationReport(any(GetApplicationReportRequest.class)))
          .thenReturn(mockResponse);
        when(ahsClient.getApplications(any(GetApplicationsRequest.class)))
          .thenReturn(mockAppResponse);
        when(
          ahsClient
            .getApplicationAttemptReport(any(GetApplicationAttemptReportRequest.class)))
          .thenReturn(mockAttemptResponse);
        when(
          ahsClient
            .getApplicationAttempts(any(GetApplicationAttemptsRequest.class)))
          .thenReturn(mockAppAttemptsResponse);
        when(ahsClient.getContainers(any(GetContainersRequest.class)))
          .thenReturn(mockContainersResponse);

        when(ahsClient.getContainerReport(any(GetContainerReportRequest.class)))
          .thenReturn(mockContainerResponse);

      } catch (YarnException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
    }

    @Override
    public List<ApplicationReport> getApplications() throws YarnException,
        IOException {
      when(mockAppResponse.getApplicationList()).thenReturn(reports);
      return super.getApplications();
    }

    @Override
    public ApplicationReport getApplicationReport(ApplicationId appId)
        throws YarnException, IOException {
      when(mockResponse.getApplicationReport()).thenReturn(getReport(appId));
      return super.getApplicationReport(appId);
    }

    @Override
    public List<ApplicationAttemptReport> getApplicationAttempts(
        ApplicationId appId) throws YarnException, IOException {
      when(mockAppAttemptsResponse.getApplicationAttemptList()).thenReturn(
        getAttempts(appId));
      return super.getApplicationAttempts(appId);
    }

    @Override
    public ApplicationAttemptReport getApplicationAttemptReport(
        ApplicationAttemptId appAttemptId) throws YarnException, IOException {
      when(mockAttemptResponse.getApplicationAttemptReport()).thenReturn(
        getAttempt(appAttemptId));
      return super.getApplicationAttemptReport(appAttemptId);
    }

    @Override
    public List<ContainerReport>
        getContainers(ApplicationAttemptId appAttemptId) throws YarnException,
            IOException {
      when(mockContainersResponse.getContainerList()).thenReturn(
        getContainersReport(appAttemptId));
      return super.getContainers(appAttemptId);
    }

    @Override
    public ContainerReport getContainerReport(ContainerId containerId)
        throws YarnException, IOException {
      when(mockContainerResponse.getContainerReport()).thenReturn(
        getContainer(containerId));
      return super.getContainerReport(containerId);
    }

    @Override
    public void stop() {
    }

    public ApplicationReport getReport(ApplicationId appId) {
      for (int i = 0; i < reports.size(); ++i) {
        if (appId.toString().equalsIgnoreCase(
          reports.get(i).getApplicationId().toString())) {
          return reports.get(i);
        }
      }
      return null;
    }

    public List<ApplicationAttemptReport> getAttempts(ApplicationId appId) {
      return attempts.get(appId);
    }

    public ApplicationAttemptReport
        getAttempt(ApplicationAttemptId appAttemptId) {
      return attempts.get(appAttemptId.getApplicationId()).get(0);
    }

    public List<ContainerReport> getContainersReport(
        ApplicationAttemptId appAttemptId) {
      return containers.get(appAttemptId);
    }

    public ContainerReport getContainer(ContainerId containerId) {
      return containers.get(containerId.getApplicationAttemptId()).get(0);
    }

    public List<ApplicationReport> getReports() {
      return this.reports;
    }

    private void createAppReports() {
      ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
      ApplicationReport newApplicationReport =
          ApplicationReport.newInstance(applicationId,
            ApplicationAttemptId.newInstance(applicationId, 1), "user",
            "queue", "appname", "host", 124, null,
            YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN",
            null);
      List<ApplicationReport> applicationReports =
          new ArrayList<ApplicationReport>();
      applicationReports.add(newApplicationReport);
      List<ApplicationAttemptReport> appAttempts =
          new ArrayList<ApplicationAttemptReport>();
      ApplicationAttemptReport attempt =
          ApplicationAttemptReport.newInstance(
            ApplicationAttemptId.newInstance(applicationId, 1),
            "host",
            124,
            "url",
            "diagnostics",
            YarnApplicationAttemptState.FINISHED,
            ContainerId.newInstance(
              newApplicationReport.getCurrentApplicationAttemptId(), 1));
      appAttempts.add(attempt);
      ApplicationAttemptReport attempt1 =
          ApplicationAttemptReport.newInstance(
            ApplicationAttemptId.newInstance(applicationId, 2),
            "host",
            124,
            "url",
            "diagnostics",
            YarnApplicationAttemptState.FINISHED,
            ContainerId.newInstance(
              newApplicationReport.getCurrentApplicationAttemptId(), 2));
      appAttempts.add(attempt1);
      attempts.put(applicationId, appAttempts);

      List<ContainerReport> containerReports = new ArrayList<ContainerReport>();
      ContainerReport container =
          ContainerReport.newInstance(
            ContainerId.newInstance(attempt.getApplicationAttemptId(), 1),
            null, NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234,
            5678, "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE);
      containerReports.add(container);

      ContainerReport container1 =
          ContainerReport.newInstance(
            ContainerId.newInstance(attempt.getApplicationAttemptId(), 2),
            null, NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234,
            5678, "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE);
      containerReports.add(container1);
      containers.put(attempt.getApplicationAttemptId(), containerReports);

      ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
      ApplicationReport newApplicationReport2 =
          ApplicationReport.newInstance(applicationId2,
            ApplicationAttemptId.newInstance(applicationId2, 2), "user2",
            "queue2", "appname2", "host2", 125, null,
            YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f,
            "NON-YARN", null);
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
      ApplicationReport newApplicationReport3 =
          ApplicationReport.newInstance(applicationId3,
            ApplicationAttemptId.newInstance(applicationId3, 3), "user3",
            "queue3", "appname3", "host3", 126, null,
            YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f,
            "MAPREDUCE", null);
      applicationReports.add(newApplicationReport3);

      ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
      ApplicationReport newApplicationReport4 =
          ApplicationReport.newInstance(applicationId4,
            ApplicationAttemptId.newInstance(applicationId4, 4), "user4",
            "queue4", "appname4", "host4", 127, null,
            YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4,
            FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
            "NON-MAPREDUCE", null);
      applicationReports.add(newApplicationReport4);
      reports = applicationReports;
    }
  }
}
