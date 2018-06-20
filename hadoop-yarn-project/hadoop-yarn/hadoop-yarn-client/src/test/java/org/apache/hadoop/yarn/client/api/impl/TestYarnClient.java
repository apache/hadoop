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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
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
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.ParameterizedSchedulerTestBase;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.event.Level;

import java.io.IOException;
import java.lang.Thread.State;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class is to test class {@link YarnClient).
 */
public class TestYarnClient extends ParameterizedSchedulerTestBase {

  public TestYarnClient(SchedulerType type) throws IOException {
    super(type);
  }

  protected void configureFairScheduler(YarnConfiguration conf) {}

  @Before
  public void setup() {
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Test
  public void testClientStop() {
    Configuration conf = getConf();
    ResourceManager rm = new ResourceManager();
    rm.init(conf);
    rm.start();

    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    client.stop();
    rm.stop();
  }

  @Test
  public void testStartTimelineClientWithErrors()
      throws Exception {
    // If timeline client failed to init with a NoClassDefFoundError
    // it should be wrapped with an informative error message
    testCreateTimelineClientWithError(
        1.5f,
        true,
        false,
        new NoClassDefFoundError("Mock a NoClassDefFoundError"),
        new CreateTimelineClientErrorVerifier(1) {
          @Override
          public void verifyError(Throwable e) {
            Assert.assertTrue(e instanceof NoClassDefFoundError);
            Assert.assertTrue(e.getMessage() != null &&
                e.getMessage().contains(
                    YarnConfiguration.TIMELINE_SERVICE_ENABLED));
          }
        });

    // Disable timeline service for this client,
    // yarn client will not fail when such error happens
    testCreateTimelineClientWithError(
        1.5f,
        false,
        false,
        new NoClassDefFoundError("Mock a NoClassDefFoundError"),
        new CreateTimelineClientErrorVerifier(0) {
          @Override public void verifyError(Throwable e) {
            Assert.fail("NoClassDefFoundError while creating timeline client"
                + "should be tolerated when timeline service is disabled.");
          }
        }
    );

    // Set best-effort to true, verify an error is still fatal
    testCreateTimelineClientWithError(
        1.5f,
        true,
        true,
        new NoClassDefFoundError("Mock a NoClassDefFoundError"),
        new CreateTimelineClientErrorVerifier(1) {
          @Override public void verifyError(Throwable e) {
            Assert.assertTrue(e instanceof NoClassDefFoundError);
            Assert.assertTrue(e.getMessage() != null &&
                e.getMessage().contains(
                    YarnConfiguration.TIMELINE_SERVICE_ENABLED));
          }
        }
    );

    // Set best-effort to false, verify that an exception
    // causes the client to fail
    testCreateTimelineClientWithError(
        1.5f,
        true,
        false,
        new IOException("ATS v1.5 client initialization failed."),
        new CreateTimelineClientErrorVerifier(1) {
          @Override
          public void verifyError(Throwable e) {
            Assert.assertTrue(e instanceof IOException);
          }
        }
    );

    // Set best-effort to true, verify that an normal exception
    // won't fail the entire client
    testCreateTimelineClientWithError(
        1.5f,
        true,
        true,
        new IOException("ATS v1.5 client initialization failed."),
        new CreateTimelineClientErrorVerifier(0) {
          @Override
          public void verifyError(Throwable e) {
            Assert.fail("IOException while creating timeline client"
                + "should be tolerated when best effort is true");
          }
        }
    );
  }

  @SuppressWarnings("deprecation")
  @Test (timeout = 30000)
  public void testSubmitApplication() throws Exception {
    Configuration conf = getConf();
    conf.setLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        100); // speed up tests
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    YarnApplicationState[] exitStates = new YarnApplicationState[]
        {
          YarnApplicationState.ACCEPTED,
          YarnApplicationState.RUNNING,
          YarnApplicationState.FINISHED
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
      client.submitApplication(context);
      verify(((MockYarnClient) client).mockReport,times(4 * i + 4))
          .getYarnApplicationState();
    }

    client.stop();
  }

  @SuppressWarnings("deprecation")
  @Test (timeout = 20000)
  public void testSubmitApplicationInterrupted() throws IOException {
    Configuration conf = getConf();
    int pollIntervalMs = 1000;
    conf.setLong(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
        pollIntervalMs);
    try (YarnClient client = new MockYarnClient()) {
      client.init(conf);
      client.start();
      // Submit the application and then interrupt it while its waiting
      // for submission to be successful.
      final class SubmitThread extends Thread {
        private boolean isInterrupted  = false;
        @Override
        public void run() {
          ApplicationSubmissionContext context =
              mock(ApplicationSubmissionContext.class);
          ApplicationId applicationId = ApplicationId.newInstance(
              System.currentTimeMillis(), 1);
          when(context.getApplicationId()).thenReturn(applicationId);
          ((MockYarnClient) client).setYarnApplicationState(
              YarnApplicationState.NEW);
          try {
            client.submitApplication(context);
          } catch (YarnException | IOException e) {
            if (e instanceof YarnException && e.getCause() != null &&
                e.getCause() instanceof InterruptedException) {
              isInterrupted = true;
            }
          }
        }
      }
      SubmitThread appSubmitThread = new SubmitThread();
      appSubmitThread.start();
      try {
        // Wait for thread to start and begin to sleep
        // (enter TIMED_WAITING state).
        while (appSubmitThread.getState() != State.TIMED_WAITING) {
          Thread.sleep(pollIntervalMs / 2);
        }
        // Interrupt the thread.
        appSubmitThread.interrupt();
        appSubmitThread.join();
      } catch (InterruptedException e) {
      }
      Assert.assertTrue("Expected an InterruptedException wrapped inside a " +
          "YarnException", appSubmitThread.isInterrupted);
    }
  }

  @Test (timeout = 30000)
  public void testSubmitIncorrectQueueToCapacityScheduler() throws IOException {
    MiniYARNCluster cluster = new MiniYARNCluster("testMRAMTokens", 1, 1, 1);
    YarnClient rmClient = null;
    try {
      YarnConfiguration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class.getName());
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();
      YarnClientApplication newApp = rmClient.createApplication();

      ApplicationId appId = newApp.getNewApplicationResponse().getApplicationId();

      // Create launch context for app master
      ApplicationSubmissionContext appContext
        = Records.newRecord(ApplicationSubmissionContext.class);

      // set the application id
      appContext.setApplicationId(appId);

      // set the application name
      appContext.setApplicationName("test");

      // Set the queue to which this application is to be submitted in the RM
      appContext.setQueue("nonexist");

      // Set up the container launch context for the application master
      ContainerLaunchContext amContainer
        = Records.newRecord(ContainerLaunchContext.class);
      appContext.setAMContainerSpec(amContainer);
      appContext.setResource(Resource.newInstance(1024, 1));
      // appContext.setUnmanagedAM(unmanaged);

      // Submit the application to the applications manager
      rmClient.submitApplication(appContext);
      Assert.fail("Job submission should have thrown an exception");
    } catch (YarnException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to submit"));
    } finally {
      if (rmClient != null) {
        rmClient.stop();
      }
      cluster.stop();
    }
  }
  
  @Test
  public void testKillApplication() throws Exception {
    MockRM rm = new MockRM();
    rm.start();
    RMApp app = rm.submitApp(2000);

    @SuppressWarnings("resource")
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
    client.start();

    client.killApplication(app.getApplicationId());
    verify(((MockYarnClient) client).getRMClient(), times(2))
      .forceKillApplication(any(KillApplicationRequest.class));
  }

  @Test(timeout = 30000)
  public void testApplicationType() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app = rm.submitApp(2000);
    RMApp app1 =
        rm.submitApp(200, "name", "user",
                new HashMap<>(), false, "default", -1,
          null, "MAPREDUCE");
    Assert.assertEquals("YARN", app.getApplicationType());
    Assert.assertEquals("MAPREDUCE", app1.getApplicationType());
    rm.stop();
  }

  @Test(timeout = 30000)
  public void testApplicationTypeLimit() throws Exception {
    GenericTestUtils.setRootLogLevel(Level.DEBUG);
    MockRM rm = new MockRM();
    rm.start();
    RMApp app1 =
        rm.submitApp(200, "name", "user",
                new HashMap<>(), false, "default", -1,
          null, "MAPREDUCE-LENGTH-IS-20");
    Assert.assertEquals("MAPREDUCE-LENGTH-IS-", app1.getApplicationType());
    rm.stop();
  }

  @Test (timeout = 10000)
  public void testGetApplications() throws YarnException, IOException {
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient)client).getReports();

    List<ApplicationReport>  reports = client.getApplications();
    Assert.assertEquals(reports, expectedReports);

    Set<String> appTypes = new HashSet<>();
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
    Assert.assertEquals(1, reports.size());
    Assert.assertEquals("NON-YARN", reports.get(0).getApplicationType());
    for (ApplicationReport report : reports) {
      Assert.assertTrue(expectedReports.contains(report));
    }

    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetApplicationAttempts() throws YarnException, IOException {
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
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
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        true);
    
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    List<ContainerReport> reports = client.getContainers(appAttemptId);
    Assert.assertNotNull(reports);
    Assert.assertEquals(reports.get(0).getContainerId(),
        (ContainerId.newContainerId(appAttemptId, 1)));
    Assert.assertEquals(reports.get(1).getContainerId(),
        (ContainerId.newContainerId(appAttemptId, 2)));
    Assert.assertEquals(reports.get(2).getContainerId(),
        (ContainerId.newContainerId(appAttemptId, 3)));
    
    //First2 containers should come from RM with updated state information and 
    // 3rd container is not there in RM and should
    Assert.assertEquals(ContainerState.RUNNING,
        (reports.get(0).getContainerState()));
    Assert.assertEquals(ContainerState.RUNNING,
        (reports.get(1).getContainerState()));
    Assert.assertEquals(ContainerState.COMPLETE,
        (reports.get(2).getContainerState()));
    client.stop();
  }

  @Test(timeout = 10000)
  public void testGetContainerReport() throws YarnException, IOException {
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
        true);
    final YarnClient client = new MockYarnClient();
    client.init(conf);
    client.start();

    List<ApplicationReport> expectedReports = ((MockYarnClient) client)
        .getReports();

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    ContainerReport report = client.getContainerReport(containerId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getContainerId().toString(),
        (ContainerId.newContainerId(expectedReports.get(0)
            .getCurrentApplicationAttemptId(), 1)).toString());
    containerId = ContainerId.newContainerId(appAttemptId, 3);
    report = client.getContainerReport(containerId);
    Assert.assertNotNull(report);
    Assert.assertEquals(report.getContainerId().toString(),
        (ContainerId.newContainerId(expectedReports.get(0)
            .getCurrentApplicationAttemptId(), 3)).toString());
    Assert.assertNotNull(report.getExecutionType());
    client.stop();
  }

  @Test (timeout = 10000)
  public void testGetLabelsToNodes() throws YarnException, IOException {
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
    client.start();

    // Get labels to nodes mapping
    Map<String, Set<NodeId>> expectedLabelsToNodes =
        ((MockYarnClient)client).getLabelsToNodesMap();
    Map<String, Set<NodeId>> labelsToNodes = client.getLabelsToNodes();
    Assert.assertEquals(labelsToNodes, expectedLabelsToNodes);
    Assert.assertEquals(labelsToNodes.size(), 3);

    // Get labels to nodes for selected labels
    Set<String> setLabels = new HashSet<>(Arrays.asList("x", "z"));
    expectedLabelsToNodes =
        ((MockYarnClient)client).getLabelsToNodesMap(setLabels);
    labelsToNodes = client.getLabelsToNodes(setLabels);
    Assert.assertEquals(labelsToNodes, expectedLabelsToNodes);
    Assert.assertEquals(labelsToNodes.size(), 2);

    client.stop();
    client.close();
  }

  @Test (timeout = 10000)
  public void testGetNodesToLabels() throws YarnException, IOException {
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
    client.start();

    // Get labels to nodes mapping
    Map<NodeId, Set<String>> expectedNodesToLabels = ((MockYarnClient) client)
        .getNodeToLabelsMap();
    Map<NodeId, Set<String>> nodesToLabels = client.getNodeToLabels();
    Assert.assertEquals(nodesToLabels, expectedNodesToLabels);
    Assert.assertEquals(nodesToLabels.size(), 1);

    client.stop();
    client.close();
  }

  private static class MockYarnClient extends YarnClientImpl {

    private ApplicationReport mockReport;
    private List<ApplicationReport> reports;
    private HashMap<ApplicationId, List<ApplicationAttemptReport>> attempts =
            new HashMap<>();
    private HashMap<ApplicationAttemptId, List<ContainerReport>> containers =
            new HashMap<>();
    private HashMap<ApplicationAttemptId, List<ContainerReport>> containersFromAHS =
      new HashMap<>();

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
    GetLabelsToNodesResponse mockLabelsToNodesResponse =
      mock(GetLabelsToNodesResponse.class);
    GetNodesToLabelsResponse mockNodeToLabelsResponse =
        mock(GetNodesToLabelsResponse.class);

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

        when(rmClient.getLabelsToNodes(any(GetLabelsToNodesRequest.class)))
            .thenReturn(mockLabelsToNodesResponse);
        
        when(rmClient.getNodeToLabels(any(GetNodesToLabelsRequest.class)))
            .thenReturn(mockNodeToLabelsResponse);

        historyClient = mock(AHSClient.class);

      } catch (Exception e) {
        Assert.fail("Unexpected exception caught: " + e);
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
          YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
      List<ApplicationReport> applicationReports = new ArrayList<>();
      applicationReports.add(newApplicationReport);
      List<ApplicationAttemptReport> appAttempts = new ArrayList<>();
      ApplicationAttemptReport attempt = ApplicationAttemptReport.newInstance(
          ApplicationAttemptId.newInstance(applicationId, 1),
          "host",
          124,
          "url",
          "oUrl",
          "diagnostics",
          YarnApplicationAttemptState.FINISHED,
          ContainerId.newContainerId(
                  newApplicationReport.getCurrentApplicationAttemptId(), 1), 0,
              0);
      appAttempts.add(attempt);
      ApplicationAttemptReport attempt1 = ApplicationAttemptReport.newInstance(
          ApplicationAttemptId.newInstance(applicationId, 2),
          "host",
          124,
          "url",
          "oUrl",
          "diagnostics",
          YarnApplicationAttemptState.FINISHED,
          ContainerId.newContainerId(
              newApplicationReport.getCurrentApplicationAttemptId(), 2));
      appAttempts.add(attempt1);
      attempts.put(applicationId, appAttempts);

      List<ContainerReport> containerReports = new ArrayList<>();
      ContainerReport container = ContainerReport.newInstance(
          ContainerId.newContainerId(attempt.getApplicationAttemptId(), 1), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "logURL", 0, ContainerState.RUNNING,
          "http://" + NodeId.newInstance("host", 2345).toString());
      containerReports.add(container);

      ContainerReport container1 = ContainerReport.newInstance(
          ContainerId.newContainerId(attempt.getApplicationAttemptId(), 2), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "logURL", 0, ContainerState.RUNNING,
          "http://" + NodeId.newInstance("host", 2345).toString());
      containerReports.add(container1);
      containers.put(attempt.getApplicationAttemptId(), containerReports);
      
      //add containers to be sent from AHS
      List<ContainerReport> containerReportsForAHS =
              new ArrayList<>();
      
      container = ContainerReport.newInstance(
          ContainerId.newContainerId(attempt.getApplicationAttemptId(), 1), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "logURL", 0, null,
          "http://" + NodeId.newInstance("host", 2345).toString());
      containerReportsForAHS.add(container);

      container1 = ContainerReport.newInstance(
          ContainerId.newContainerId(attempt.getApplicationAttemptId(), 2), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "HSlogURL", 0, null,
          "http://" + NodeId.newInstance("host", 2345).toString());
      containerReportsForAHS.add(container1);
      ContainerReport container2 = ContainerReport.newInstance(
          ContainerId.newContainerId(attempt.getApplicationAttemptId(),3), null,
          NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
          "diagnosticInfo", "HSlogURL", 0, ContainerState.COMPLETE,
          "http://" + NodeId.newInstance("host", 2345).toString());
      containerReportsForAHS.add(container2);
      containersFromAHS.put(attempt.getApplicationAttemptId(), containerReportsForAHS);

      ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
      ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
          applicationId2, ApplicationAttemptId.newInstance(applicationId2, 2),
          "user2", "queue2", "appname2", "host2", 125, null,
          YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2, 2,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN", 
        null);
      applicationReports.add(newApplicationReport2);

      ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
      ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
          applicationId3, ApplicationAttemptId.newInstance(applicationId3, 3),
          "user3", "queue3", "appname3", "host3", 126, null,
          YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3, 3,
          FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE",
        null);
      applicationReports.add(newApplicationReport3);

      ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
      ApplicationReport newApplicationReport4 =
          ApplicationReport.newInstance(
              applicationId4,
              ApplicationAttemptId.newInstance(applicationId4, 4),
              "user4", "queue4", "appname4", "host4", 127, null,
              YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4, 4,
              FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
              "NON-MAPREDUCE", null);
      applicationReports.add(newApplicationReport4);
      return applicationReports;
    }

    private List<ApplicationReport> getApplicationReports(
        List<ApplicationReport> applicationReports,
        Set<String> applicationTypes, EnumSet<YarnApplicationState> applicationStates) {

      List<ApplicationReport> appReports = new ArrayList<>();
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
    public Map<String, Set<NodeId>> getLabelsToNodes()
        throws YarnException, IOException {
      when(mockLabelsToNodesResponse.getLabelsToNodes()).thenReturn(
          getLabelsToNodesMap());
      return super.getLabelsToNodes();
    }

    @Override
    public Map<String, Set<NodeId>> getLabelsToNodes(Set<String> labels)
        throws YarnException, IOException {
      when(mockLabelsToNodesResponse.getLabelsToNodes()).thenReturn(
          getLabelsToNodesMap(labels));
      return super.getLabelsToNodes(labels);
    }

    public Map<String, Set<NodeId>> getLabelsToNodesMap() {
      Map<String, Set<NodeId>> map = new HashMap<>();
      Set<NodeId> setNodeIds =
          new HashSet<>(Arrays.asList(
          NodeId.newInstance("host1", 0), NodeId.newInstance("host2", 0)));
      map.put("x", setNodeIds);
      map.put("y", setNodeIds);
      map.put("z", setNodeIds);
      return map;
    }

    public Map<String, Set<NodeId>> getLabelsToNodesMap(Set<String> labels) {
      Map<String, Set<NodeId>> map = new HashMap<>();
      Set<NodeId> setNodeIds = new HashSet<>(Arrays.asList(
          NodeId.newInstance("host1", 0), NodeId.newInstance("host2", 0)));
      for (String label : labels) {
        map.put(label, setNodeIds);
      }
      return map;
    }

    @Override
    public Map<NodeId, Set<String>> getNodeToLabels() throws YarnException,
        IOException {
      when(mockNodeToLabelsResponse.getNodeToLabels()).thenReturn(
          getNodeToLabelsMap());
      return super.getNodeToLabels();
    }

    public Map<NodeId, Set<String>> getNodeToLabelsMap() {
      Map<NodeId, Set<String>> map = new HashMap<>();
      Set<String> setNodeLabels = new HashSet<>(Arrays.asList("x", "y"));
      map.put(NodeId.newInstance("host", 0), setNodeLabels);
      return map;
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
      when(historyClient.getContainers(any(ApplicationAttemptId.class)))
      .thenReturn(getContainersFromAHS(appAttemptId));
      return super.getContainers(appAttemptId);
    }

    private List<ContainerReport> getContainersFromAHS(
        ApplicationAttemptId appAttemptId) {
      return containersFromAHS.get(appAttemptId);
    }

    @Override
    public ContainerReport getContainerReport(ContainerId containerId)
        throws YarnException, IOException {
      try {
        ContainerReport container = getContainer(containerId, containers);
        when(mockContainerResponse.getContainerReport()).thenReturn(container);
      } catch (YarnException e) {
        when(rmClient.getContainerReport(any(GetContainerReportRequest.class)))
        .thenThrow(e).thenReturn(mockContainerResponse);
      }
      try {
        ContainerReport container =
            getContainer(containerId, containersFromAHS);
        when(historyClient.getContainerReport(any(ContainerId.class)))
            .thenReturn(container);
      } catch (YarnException e) {
        when(historyClient.getContainerReport(any(ContainerId.class)))
            .thenThrow(e);
      }

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

    private ContainerReport getContainer(
        ContainerId containerId,
        HashMap<ApplicationAttemptId, List<ContainerReport>> containersToAppAttemptMapping)
        throws YarnException {
      List<ContainerReport> containersForAppAttempt =
          containersToAppAttemptMapping.get(containerId
              .getApplicationAttemptId());
      if (containersForAppAttempt == null) {
        throw new ApplicationNotFoundException(containerId
            .getApplicationAttemptId().getApplicationId() + " is not found ");
      }
      Iterator<ContainerReport> iterator = containersForAppAttempt.iterator();
      while (iterator.hasNext()) {
        ContainerReport next = iterator.next();
        if (next.getContainerId().equals(containerId)) {
          return next;
        }
      }
      throw new ContainerNotFoundException(containerId + " is not found ");
    }
  }

  @Test(timeout = 30000)
  public void testAMMRTokens() throws Exception {
    MiniYARNCluster cluster = new MiniYARNCluster("testMRAMTokens", 1, 1, 1);
    YarnClient rmClient = null;
    try {
      cluster.init(getConf());
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      rmClient = YarnClient.createYarnClient();
      rmClient.init(yarnConf);
      rmClient.start();

      ApplicationId appId = createApp(rmClient, false);
      waitTillAccepted(rmClient, appId, false);
      //managed AMs don't return AMRM token
      Assert.assertNull(rmClient.getAMRMToken(appId));

      appId = createApp(rmClient, true);
      waitTillAccepted(rmClient, appId, true);
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
          waitTillAccepted(rmClient, appId, true);
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

  private void waitTillAccepted(YarnClient rmClient, ApplicationId appId,
      boolean unmanagedApplication)
    throws Exception {
    long start = System.currentTimeMillis();
    ApplicationReport report = rmClient.getApplicationReport(appId);
    while (YarnApplicationState.ACCEPTED != report.getYarnApplicationState()) {
      if (System.currentTimeMillis() - start > 20 * 1000) {
        throw new Exception(
            "App '" + appId + "' time out, failed to reach ACCEPTED state");
      }
      Thread.sleep(200);
      report = rmClient.getApplicationReport(appId);
    }
    Assert.assertEquals(unmanagedApplication, report.isUnmanagedApp());
  }

  @Test(timeout = 30000, expected = ApplicationNotFoundException.class)
  public void testShouldNotRetryForeverForNonNetworkExceptions() throws Exception {
    YarnConfiguration conf = getConf();
    conf.setInt(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, -1);

    ResourceManager rm = null;
    YarnClient yarnClient = null;
    try {
      // start rm
      rm = new ResourceManager();
      rm.init(conf);
      rm.start();

      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();

      // create invalid application id
      ApplicationId appId = ApplicationId.newInstance(1430126768L, 10645);

      // RM should throw ApplicationNotFoundException exception
      yarnClient.getApplicationReport(appId);
    } finally {
      if (yarnClient != null) {
        yarnClient.stop();
      }
      if (rm != null) {
        rm.stop();
      }
    }
  }

  @Test
  public void testSignalContainer() throws Exception {
    @SuppressWarnings("resource")
    final YarnClient client = new MockYarnClient();
    client.init(getConf());
    client.start();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 1);
    SignalContainerCommand command = SignalContainerCommand.OUTPUT_THREAD_DUMP;
    client.signalToContainer(containerId, command);
    final ArgumentCaptor<SignalContainerRequest> signalReqCaptor =
        ArgumentCaptor.forClass(SignalContainerRequest.class);
    verify(((MockYarnClient) client).getRMClient())
        .signalToContainer(signalReqCaptor.capture());
    SignalContainerRequest request = signalReqCaptor.getValue();
    Assert.assertEquals(containerId, request.getContainerId());
    Assert.assertEquals(command, request.getCommand());
  }

  private void testCreateTimelineClientWithError(
      float timelineVersion,
      boolean timelineServiceEnabled,
      boolean timelineClientBestEffort,
      Throwable mockErr,
      CreateTimelineClientErrorVerifier errVerifier) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        timelineServiceEnabled);
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_CLIENT_BEST_EFFORT,
        timelineClientBestEffort);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION,
        timelineVersion);
    MockYarnClient client = new MockYarnClient();
    MockYarnClient spyClient = spy(client);
    when(spyClient.createTimelineClient()).thenThrow(mockErr);
    CreateTimelineClientErrorVerifier verifier = spy(errVerifier);
    spyClient.init(conf);
    spyClient.start();

    ApplicationSubmissionContext context =
        mock(ApplicationSubmissionContext.class);
    ContainerLaunchContext containerContext =
        mock(ContainerLaunchContext.class);
    ApplicationId applicationId =
        ApplicationId.newInstance(System.currentTimeMillis(), 1);
    when(containerContext.getTokens()).thenReturn(null);
    when(context.getApplicationId()).thenReturn(applicationId);
    when(spyClient.isSecurityEnabled()).thenReturn(true);
    when(context.getAMContainerSpec()).thenReturn(containerContext);

    try {
      spyClient.submitApplication(context);
    } catch (Throwable e) {
      verifier.verifyError(e);
    } finally {
      // Make sure the verifier runs with expected times
      // This is required because in case throwable is swallowed
      // and verifyError never gets the chance to run
      verify(verifier, times(verifier.getExpectedTimes()))
          .verifyError(any(Throwable.class));
      spyClient.stop();
    }
  }

  private abstract class CreateTimelineClientErrorVerifier {
    // Verify verifyError gets executed with expected times
    private int times = 0;
    protected CreateTimelineClientErrorVerifier(int times) {
      this.times = times;
    }
    public int getExpectedTimes() {
      return this.times;
    }
    // Verification a throwable is in desired state
    // E.g verify type and error message
    public abstract void verifyError(Throwable e);
  }
}
