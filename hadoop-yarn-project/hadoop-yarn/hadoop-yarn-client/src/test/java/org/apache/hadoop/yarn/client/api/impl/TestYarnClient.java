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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
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
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestYarnClient {

  @Test
  public void test() {
    // More to come later.
  }

  @Test
  public void testClientStop() {
    Configuration conf = new Configuration();
    ResourceManager rm = new ResourceManager();
    rm.init(conf);
    rm.start();

    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    client.stop();
    rm.stop();
  }

  @SuppressWarnings("deprecation")
  @Test (timeout = 30000)
  public void testSubmitApplication() {
    Configuration conf = new Configuration();
    conf.setLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        100); // speed up tests
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    YarnApplicationState[] exitStates = new YarnApplicationState[]
        {
          YarnApplicationState.SUBMITTED,
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.RUNNING,
          YarnApplicationState.FINISHED,
          YarnApplicationState.FAILED,
          YarnApplicationState.KILLED
        };

    // Submit an application without ApplicationId provided
    // Should get ApplicationIdNotProvidedException
    ApplicationSubmissionContext contextWithoutApplicationId =
        mock(ApplicationSubmissionContext.class);
    try {
      client.submitApplication(contextWithoutApplicationId);
      Assert.fail("Should throw the ApplicationIdNotProvidedException");
    } catch (YarnException e) {
      Assert.assertTrue(e instanceof ApplicationIdNotProvidedException);
      Assert.assertTrue(e.getMessage().contains(
          "ApplicationId is not provided in ApplicationSubmissionContext"));
    } catch (IOException e) {
      Assert.fail("IOException is not expected.");
    }

    // Submit the application with applicationId provided
    // Should be successful
    for (int i = 0; i < exitStates.length; ++i) {
      ApplicationSubmissionContext context =
          mock(ApplicationSubmissionContext.class);
      ApplicationId applicationId = ApplicationId.newInstance(
          System.currentTimeMillis(), i);
      when(context.getApplicationId()).thenReturn(applicationId);
      ((MockYarnClient) client).setYarnApplicationState(exitStates[i]);
      try {
        client.submitApplication(context);
      } catch (YarnException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      verify(((MockYarnClient) client).mockReport,times(4 * i + 4))
          .getYarnApplicationState();
    }

    client.stop();
  }

  @Test
  public void testKillApplication() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    RMApp app = rm.submitApp(2000);

    Configuration conf = new Configuration();
    @SuppressWarnings("resource")
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    client.killApplication(app.getApplicationId());
    verify(((MockYarnClient) client).getRMClient(), times(2))
      .forceKillApplication(any(KillApplicationRequest.class));
  }

  @Test(timeout = 30000)
  public void testApplicationType() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app = rm.submitApp(2000);
    RMApp app1 =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE");
    Assert.assertEquals("YARN", app.getApplicationType());
    Assert.assertEquals("MAPREDUCE", app1.getApplicationType());
    rm.stop();
  }

  @Test(timeout = 30000)
  public void testApplicationTypeLimit() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app1 =
        rm.submitApp(200, "name", "user",
          new HashMap<ApplicationAccessType, String>(), false, "default", -1,
          null, "MAPREDUCE-LENGTH-IS-20");
    Assert.assertEquals("MAPREDUCE-LENGTH-IS-", app1.getApplicationType());
    rm.stop();
  }

  @Test (timeout = 10000)
  public void testGetApplications() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient)client).getReports();

    List<ApplicationReport>  reports = client.getApplications();
    Assert.assertEquals(reports, expectedReports);

    Set<String> appTypes = new HashSet<String>();
    appTypes.add("YARN");
    appTypes.add("NON-YARN");

    reports =
        client.getApplications(appTypes, null);
    Assert.assertEquals(reports.size(), 2);
    Assert
        .assertTrue((reports.get(0).getApplicationType().equals("YARN") && reports
            .get(1).getApplicationType().equals("NON-YARN"))
            || (reports.get(1).getApplicationType().equals("YARN") && reports
                .get(0).getApplicationType().equals("NON-YARN")));
    for(ApplicationReport report : reports) {
      Assert.assertTrue(expectedReports.contains(report));
    }

    EnumSet<YarnApplicationState> appStates =
        EnumSet.noneOf(YarnApplicationState.class);
    appStates.add(YarnApplicationState.FINISHED);
    appStates.add(YarnApplicationState.FAILED);
    reports = client.getApplications(null, appStates);
    Assert.assertEquals(reports.size(), 2);
    Assert
    .assertTrue((reports.get(0).getApplicationType().equals("NON-YARN") && reports
        .get(1).getApplicationType().equals("NON-MAPREDUCE"))
        || (reports.get(1).getApplicationType().equals("NON-YARN") && reports
            .get(0).getApplicationType().equals("NON-MAPREDUCE")));
    for (ApplicationReport report : reports) {
      Assert.assertTrue(expectedReports.contains(report));
    }

    reports = client.getApplications(appTypes, appStates);
    Assert.assertEquals(reports.size(), 1);
    Assert
    .assertTrue((reports.get(0).getApplicationType().equals("NON-YARN")));
    for (ApplicationReport report : reports) {
      Assert.assertTrue(expectedReports.contains(report));
    }

    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplicationAttempts() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    List<ApplicationAttemptReport> reports = client
        .getApplicationAttempts(applicationId);
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
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient) client)
        .getReports();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ApplicationAttemptReport report = client
        .getApplicationAttemptReport(appAttemptId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getApplicationAttemptId().toString(),
        expectedReports.get(0).getCurrentApplicationAttemptId().toString());
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetContainers() throws YarnException, IOException {
    Configuration conf = new Configuration();
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
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
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient) client)
        .getReports();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newInstance(appAttemptId, 1);
    ContainerReport report = client.getContainerReport(containerId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getContainerId().toString(),
        (ContainerId.newInstance(expectedReports.get(0)
            .getCurrentApplicationAttemptId(), 1)).toString());
    client.stop();
  }

  private static class MockYarnClient extends YarnClientImpl {
    private ApplicationReport mockReport;
    private List<ApplicationReport> reports;
    private HashMap<ApplicationId, List<ApplicationAttemptReport>> attempts = 
      new HashMap<ApplicationId, List<ApplicationAttemptReport>>();
    private HashMap<ApplicationAttemptId, List<ContainerReport>> containers = 
      new HashMap<ApplicationAttemptId, List<ContainerReport>>();
    GetApplicationsResponse mockAppResponse =
      mock(GetApplicationsResponse.class);
    GetApplicationAttemptsResponse mockAppAttemptsResponse = 
      mock(GetApplicationAttemptsResponse.class);
    GetApplicationAttemptReportResponse mockAttemptResponse = 
      mock(GetApplicationAttemptReportResponse.class);
    GetContainersResponse mockContainersResponse = 
      mock(GetContainersResponse.class);
    GetContainerReportResponse mockContainerResponse = 
      mock(GetContainerReportResponse.class);

    public MockYarnClient() {
      super();
      reports = createAppReports();
    }

    @Override
    public void start() {
      rmClient = mock(ApplicationClientProtocol.class);
      GetApplicationReportResponse mockResponse =
          mock(GetApplicationReportResponse.class);
      mockReport = mock(ApplicationReport.class);
      try{
        when(rmClient.getApplicationReport(any(
            GetApplicationReportRequest.class))).thenReturn(mockResponse);
        when(rmClient.getApplications(any(GetApplicationsRequest.class)))
            .thenReturn(mockAppResponse);
        // return false for 1st kill request, and true for the 2nd.
        when(rmClient.forceKillApplication(any(
          KillApplicationRequest.class)))
          .thenReturn(KillApplicationResponse.newInstance(false)).thenReturn(
            KillApplicationResponse.newInstance(true));
        when(
            rmClient
                .getApplicationAttemptReport(any(GetApplicationAttemptReportRequest.class)))
            .thenReturn(mockAttemptResponse);
        when(
            rmClient
                .getApplicationAttempts(any(GetApplicationAttemptsRequest.class)))
            .thenReturn(mockAppAttemptsResponse);
        when(rmClient.getContainers(any(GetContainersRequest.class)))
            .thenReturn(mockContainersResponse);

        when(rmClient.getContainerReport(any(GetContainerReportRequest.class)))
            .thenReturn(mockContainerResponse);
      } catch (YarnException e) {
        Assert.fail("Exception is not expected.");
      } catch (IOException e) {
        Assert.fail("Exception is not expected.");
      }
      when(mockResponse.getApplicationReport()).thenReturn(mockReport);
    }

    public ApplicationClientProtocol getRMClient() {
      return rmClient;
    }

    @Override
    public List<ApplicationReport> getApplications(
        Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates)
        throws YarnException, IOException {
      when(mockAppResponse.getApplicationList()).thenReturn(
          getApplicationReports(reports, applicationTypes, applicationStates));
      return super.getApplications(applicationTypes, applicationStates);
    }

    @Override
    public void stop() {
    }

    public void setYarnApplicationState(YarnApplicationState state) {
      when(mockReport.getYarnApplicationState()).thenReturn(
          YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
          YarnApplicationState.NEW_SAVING, state);
    }

    public List<ApplicationReport> getReports() {
      return this.reports;
    }

    private List<ApplicationReport> createAppReports() {
      ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
      ApplicationReport newApplicationReport = ApplicationReport.newInstance(
          applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
      List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
      applicationReports.add(newApplicationReport);
      List<ApplicationAttemptReport> appAttempts = new ArrayList<ApplicationAttemptReport>();
      ApplicationAttemptReport attempt = ApplicationAttemptReport.newInstance(
          ApplicationAttemptId.newInstance(applicationId, 1),
          "host",
          124,
          "url",
          "diagnostics",
          YarnApplicationAttemptState.FINISHED,
          ContainerId.newInstance(
              newApplicationReport.getCurrentApplicationAttemptId(), 1));
      appAttempts.add(attempt);
      ApplicationAttemptReport attempt1 = ApplicationAttemptReport.newInstance(
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
      ContainerReport container = ContainerReport.newInstance(
          ContainerId.newInstance(attempt.getApplicationAttemptId(), 1), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE);
      containerReports.add(container);

      ContainerReport container1 = ContainerReport.newInstance(
          ContainerId.newInstance(attempt.getApplicationAttemptId(), 2), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE);
      containerReports.add(container1);
      containers.put(attempt.getApplicationAttemptId(), containerReports);

      ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
      ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
          applicationId2, ApplicationAttemptId.newInstance(applicationId2, 2),
          "user2", "queue2", "appname2", "host2", 125, null,
          YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN", 
        null);
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
      ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
          applicationId3, ApplicationAttemptId.newInstance(applicationId3, 3),
          "user3", "queue3", "appname3", "host3", 126, null,
          YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE",
        null);
      applicationReports.add(newApplicationReport3);

      ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
      ApplicationReport newApplicationReport4 =
          ApplicationReport.newInstance(
              applicationId4,
              ApplicationAttemptId.newInstance(applicationId4, 4),
              "user4", "queue4", "appname4", "host4", 127, null,
              YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4,
              FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
              "NON-MAPREDUCE", null);
      applicationReports.add(newApplicationReport4);
      return applicationReports;
    }

    private List<ApplicationReport> getApplicationReports(
        List<ApplicationReport> applicationReports,
        Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) {

      List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();
      for (ApplicationReport appReport : applicationReports) {
        if (applicationTypes != null && !applicationTypes.isEmpty()) {
          if (!applicationTypes.contains(appReport.getApplicationType())) {
            continue;
          }
        }

        if (applicationStates != null && !applicationStates.isEmpty()) {
          if (!applicationStates.contains(appReport.getYarnApplicationState())) {
            continue;
          }
        }
        appReports.add(appReport);
      }
      return appReports;
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
  }

  @Test(timeout = 30000)
  public void testAMMRTokens() throws Exception {
    MiniYARNCluster cluster = new MiniYARNCluster("testMRAMTokens", 1, 1, 1);
    YarnClient rmClient = null;
    try {
      cluster.init(new YarnConfiguration());
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      ApplicationId appId = createApp(rmClient, false);
      waitTillAccepted(rmClient, appId);
      //managed AMs don't return AMRM token
      Assert.assertNull(rmClient.getAMRMToken(appId));

      appId = createApp(rmClient, true);
      waitTillAccepted(rmClient, appId);
      long start = System.currentTimeMillis();
      while (rmClient.getAMRMToken(appId) == null) {
        if (System.currentTimeMillis() - start > 20 * 1000) {
          Assert.fail("AMRM token is null");
        }
        Thread.sleep(100);
      }
      //unmanaged AMs do return AMRM token
      Assert.assertNotNull(rmClient.getAMRMToken(appId));
      
      UserGroupInformation other =
        UserGroupInformation.createUserForTesting("foo", new String[]{});
      appId = other.doAs(
        new PrivilegedExceptionAction<ApplicationId>() {
          @Override
          public ApplicationId run() throws Exception {
            YarnClient rmClient = YarnClient.createYarnClient();
            rmClient.init(yarnConf);
            rmClient.start();
            ApplicationId appId = createApp(rmClient, true);
            waitTillAccepted(rmClient, appId);
            long start = System.currentTimeMillis();
            while (rmClient.getAMRMToken(appId) == null) {
              if (System.currentTimeMillis() - start > 20 * 1000) {
                Assert.fail("AMRM token is null");
              }
              Thread.sleep(100);
            }
            //unmanaged AMs do return AMRM token
            Assert.assertNotNull(rmClient.getAMRMToken(appId));
            return appId;
          }
        });
      //other users don't get AMRM token
      Assert.assertNull(rmClient.getAMRMToken(appId));
    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }

  private ApplicationId createApp(YarnClient rmClient, boolean unmanaged) 
    throws Exception {
    YarnClientApplication newApp = rmClient.createApplication();

    ApplicationId appId = newApp.getNewApplicationResponse().getApplicationId();

    // Create launch context for app master
    ApplicationSubmissionContext appContext
      = Records.newRecord(ApplicationSubmissionContext.class);

    // set the application id
    appContext.setApplicationId(appId);

    // set the application name
    appContext.setApplicationName("test");

    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(1);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer
      = Records.newRecord(ContainerLaunchContext.class);
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    appContext.setUnmanagedAM(unmanaged);

    // Submit the application to the applications manager
    rmClient.submitApplication(appContext);

    return appId;
  }
  
  private void waitTillAccepted(YarnClient rmClient, ApplicationId appId)
    throws Exception {
    try {
      long start = System.currentTimeMillis();
      ApplicationReport report = rmClient.getApplicationReport(appId);
      while (YarnApplicationState.ACCEPTED != report.getYarnApplicationState()) {
        if (System.currentTimeMillis() - start > 20 * 1000) {
          throw new Exception("App '" + appId + 
            "' time out, failed to reach ACCEPTED state");
        }
        Thread.sleep(200);
        report = rmClient.getApplicationReport(appId);
      }
    } catch (Exception ex) {
      throw new Exception(ex);
    }
  }

  @Test
  public void testAsyncAPIPollTimeout() {
    testAsyncAPIPollTimeoutHelper(null, false);
    testAsyncAPIPollTimeoutHelper(0L, true);
    testAsyncAPIPollTimeoutHelper(1L, true);
  }

  private void testAsyncAPIPollTimeoutHelper(Long valueForTimeout,
      boolean expectedTimeoutEnforcement) {
    YarnClientImpl client = new YarnClientImpl();
    try {
      Configuration conf = new Configuration();
      if (valueForTimeout != null) {
        conf.setLong(
            YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS,
            valueForTimeout);
      }

      client.init(conf);

      Assert.assertEquals(
          expectedTimeoutEnforcement, client.enforceAsyncAPITimeout());
    } finally {
      IOUtils.closeQuietly(client);
    }
  }

  @Test
  public void testAutomaticTimelineDelegationTokenLoading()
      throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    final Token<TimelineDelegationTokenIdentifier> dToken =
        new Token<TimelineDelegationTokenIdentifier>();
    // crate a mock client
    YarnClientImpl client = new YarnClientImpl() {
      @Override
      protected void serviceInit(Configuration conf) throws Exception {
        if (getConfig().getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
          timelineServiceEnabled = true;
          timelineClient = mock(TimelineClient.class);
          when(timelineClient.getDelegationToken(any(String.class)))
              .thenReturn(dToken);
          timelineClient.init(getConfig());
          timelineService = TimelineUtils.buildTimelineTokenService(getConfig());
        }
        this.setConfig(conf);
      }

      @Override
      protected void serviceStart() throws Exception {
        rmClient = mock(ApplicationClientProtocol.class);
      }

      @Override
      protected void serviceStop() throws Exception {
      }

      @Override
      public ApplicationReport getApplicationReport(ApplicationId appId) {
        ApplicationReport report = mock(ApplicationReport.class);
        when(report.getYarnApplicationState())
            .thenReturn(YarnApplicationState.SUBMITTED);
        return report;
      }

      @Override
      public boolean isSecurityEnabled() {
        return true;
      }
    };
    client.init(conf);
    client.start();
    ApplicationSubmissionContext context =
        mock(ApplicationSubmissionContext.class);
    ApplicationId applicationId = ApplicationId.newInstance(0, 1);
    when(context.getApplicationId()).thenReturn(applicationId);
    DataOutputBuffer dob = new DataOutputBuffer();
    Credentials credentials = new Credentials();
    credentials.writeTokenStorageToStream(dob);
    ByteBuffer tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
        null, null, null, null, tokens, null);
    when(context.getAMContainerSpec()).thenReturn(clc);
    client.submitApplication(context);
    // Check whether token is added or not
    credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    tokens = clc.getTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    Collection<Token<? extends TokenIdentifier>> dTokens =
        credentials.getAllTokens();
    Assert.assertEquals(1, dTokens.size());
    Assert.assertEquals(dToken, dTokens.iterator().next());
    client.stop();
  }
}
