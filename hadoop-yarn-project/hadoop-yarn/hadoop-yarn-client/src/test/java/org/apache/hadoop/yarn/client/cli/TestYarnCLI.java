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
package org.apache.hadoop.yarn.client.cli;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.Options;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystemTestUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.jetty.util.log.Log;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class TestYarnCLI {

  private YarnClient client = mock(YarnClient.class);
  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;
  private static final Pattern SPACES_PATTERN =
      Pattern.compile("\\s+|\\n+|\\t+");

  @Before
  public void setup() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut = spy(new PrintStream(sysOutStream));
    sysErrStream = new ByteArrayOutputStream();
    sysErr = spy(new PrintStream(sysErrStream));
    System.setOut(sysOut);
  }
  
  @Test
  public void testGetApplicationReport() throws Exception {
    for (int i = 0; i < 2; ++i) {
      ApplicationCLI cli = createAndGetAppCLI();
      ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
      Map<String, Long> resourceSecondsMap = new HashMap<>();
      Map<String, Long> preemptedResoureSecondsMap = new HashMap<>();
      resourceSecondsMap.put(ResourceInformation.MEMORY_MB.getName(), 123456L);
      resourceSecondsMap.put(ResourceInformation.VCORES.getName(), 4567L);
      preemptedResoureSecondsMap
          .put(ResourceInformation.MEMORY_MB.getName(), 1111L);
      preemptedResoureSecondsMap
          .put(ResourceInformation.VCORES.getName(), 2222L);
      ApplicationResourceUsageReport usageReport = i == 0 ? null :
          ApplicationResourceUsageReport
              .newInstance(2, 0, null, null, null, resourceSecondsMap, 0, 0,
                  preemptedResoureSecondsMap);
      ApplicationReport newApplicationReport = ApplicationReport.newInstance(
          applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
          "user", "queue", "appname", "host", 124, null,
          YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
          FinalApplicationStatus.SUCCEEDED, usageReport, "N/A", 0.53789f, "YARN",
          null, null, false, Priority.newInstance(0), "high-mem", "high-mem");
      newApplicationReport.setLogAggregationStatus(LogAggregationStatus.SUCCEEDED);
      newApplicationReport.setPriority(Priority.newInstance(0));
      ApplicationTimeout timeout = ApplicationTimeout
          .newInstance(ApplicationTimeoutType.LIFETIME, "UNLIMITED", -1);
      newApplicationReport.setApplicationTimeouts(
          Collections.singletonMap(timeout.getTimeoutType(), timeout));

      when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
          newApplicationReport);
      int result = cli.run(new String[] { "application", "-status", applicationId.toString() });
      assertEquals(0, result);
      verify(client, times(1 + i)).getApplicationReport(applicationId);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter pw = new PrintWriter(baos);
      pw.println("Application Report : ");
      pw.println("\tApplication-Id : application_1234_0005");
      pw.println("\tApplication-Name : appname");
      pw.println("\tApplication-Type : YARN");
      pw.println("\tUser : user");
      pw.println("\tQueue : queue");
      pw.println("\tApplication Priority : 0");
      pw.println("\tStart-Time : 0");
      pw.println("\tFinish-Time : 0");
      pw.println("\tProgress : 53.79%");
      pw.println("\tState : FINISHED");
      pw.println("\tFinal-State : SUCCEEDED");
      pw.println("\tTracking-URL : N/A");
      pw.println("\tRPC Port : 124");
      pw.println("\tAM Host : host");
      pw.println("\tAggregate Resource Allocation : " +
          (i == 0 ? "N/A" : "123456 MB-seconds, 4567 vcore-seconds"));
      pw.println("\tAggregate Resource Preempted : " +
          (i == 0 ? "N/A" : "1111 MB-seconds, 2222 vcore-seconds"));
      pw.println("\tLog Aggregation Status : SUCCEEDED");
      pw.println("\tDiagnostics : diagnostics");
      pw.println("\tUnmanaged Application : false");
      pw.println("\tApplication Node Label Expression : high-mem");
      pw.println("\tAM container Node Label Expression : high-mem");
      pw.print("\tTimeoutType : LIFETIME");
      pw.print("\tExpiryTime : UNLIMITED");
      pw.println("\tRemainingTime : -1seconds");
      pw.println();
      pw.close();
      String appReportStr = baos.toString("UTF-8");
      Assert.assertEquals(appReportStr, sysOutStream.toString());
      sysOutStream.reset();
      verify(sysOut, times(1 + i)).println(isA(String.class));
    }
  }

  @Test
  public void testGetApplicationAttemptReport() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ApplicationAttemptReport attemptReport =
        ApplicationAttemptReport.newInstance(attemptId, "host", 124, "url",
            "oUrl", "diagnostics", YarnApplicationAttemptState.FINISHED,
            ContainerId.newContainerId(attemptId, 1), 1000l, 2000l);
    when(
        client
            .getApplicationAttemptReport(any(ApplicationAttemptId.class)))
        .thenReturn(attemptReport);
    int result = cli.run(new String[] { "applicationattempt", "-status",
        attemptId.toString() });
    assertEquals(0, result);
    verify(client).getApplicationAttemptReport(attemptId);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Application Attempt Report : ");
    pw.println("\tApplicationAttempt-Id : appattempt_1234_0005_000001");
    pw.println("\tState : FINISHED");
    pw.println("\tAMContainer : container_1234_0005_01_000001");
    pw.println("\tTracking-URL : url");
    pw.println("\tRPC Port : 124");
    pw.println("\tAM Host : host");
    pw.println("\tDiagnostics : diagnostics");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).println(isA(String.class));
  }
  
  @Test
  public void testGetApplicationAttempts() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ApplicationAttemptId attemptId1 = ApplicationAttemptId.newInstance(
        applicationId, 2);
    ApplicationAttemptReport attemptReport = ApplicationAttemptReport
        .newInstance(attemptId, "host", 124, "url", "oUrl", "diagnostics",
            YarnApplicationAttemptState.FINISHED, ContainerId.newContainerId(
                attemptId, 1));
    ApplicationAttemptReport attemptReport1 = ApplicationAttemptReport
        .newInstance(attemptId1, "host", 124, "url", "oUrl", "diagnostics",
            YarnApplicationAttemptState.FINISHED, ContainerId.newContainerId(
                attemptId1, 1));
    List<ApplicationAttemptReport> reports = new ArrayList<ApplicationAttemptReport>();
    reports.add(attemptReport);
    reports.add(attemptReport1);
    when(client.getApplicationAttempts(any(ApplicationId.class)))
        .thenReturn(reports);
    int result = cli.run(new String[] { "applicationattempt", "-list",
        applicationId.toString() });
    assertEquals(0, result);
    verify(client).getApplicationAttempts(applicationId);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Total number of application attempts :2");
    pw.print("         ApplicationAttempt-Id");
    pw.print("\t               State");
    pw.print("\t                    AM-Container-Id");
    pw.println("\t                       Tracking-URL");
    pw.print("   appattempt_1234_0005_000001");
    pw.print("\t            FINISHED");
    pw.print("\t      container_1234_0005_01_000001");
    pw.println("\t                                url");
    pw.print("   appattempt_1234_0005_000002");
    pw.print("\t            FINISHED");
    pw.print("\t      container_1234_0005_02_000001");
    pw.println("\t                                url");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
  }
  
  @Test
  public void testGetContainerReport() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    ContainerReport container = ContainerReport.newInstance(containerId, null,
        NodeId.newInstance("host", 1234), Priority.UNDEFINED, 1234, 5678,
        "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE,
        "http://" + NodeId.newInstance("host", 2345).toString());
    when(client.getContainerReport(any(ContainerId.class))).thenReturn(
        container);
    int result = cli.run(new String[] { "container", "-status",
        containerId.toString() });
    assertEquals(0, result);
    verify(client).getContainerReport(containerId);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Container Report : ");
    pw.println("\tContainer-Id : container_1234_0005_01_000001");
    pw.println("\tStart-Time : 1234");
    pw.println("\tFinish-Time : 5678");
    pw.println("\tState : COMPLETE");
    pw.println("\tExecution-Type : GUARANTEED");
    pw.println("\tLOG-URL : logURL");
    pw.println("\tHost : host:1234");
    pw.println("\tNodeHttpAddress : http://host:2345");
    pw.println("\tDiagnostics : diagnosticInfo");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).println(isA(String.class));
  }
  
  @Test
  public void testGetContainers() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
    ContainerId containerId1 = ContainerId.newContainerId(attemptId, 2);
    ContainerId containerId2 = ContainerId.newContainerId(attemptId, 3);
    long time1=1234,time2=5678;
    ContainerReport container = ContainerReport.newInstance(containerId, null,
        NodeId.newInstance("host", 1234), Priority.UNDEFINED, time1, time2,
        "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE,
        "http://" + NodeId.newInstance("host", 2345).toString());
    ContainerReport container1 = ContainerReport.newInstance(containerId1, null,
        NodeId.newInstance("host", 1234), Priority.UNDEFINED, time1, time2,
        "diagnosticInfo", "logURL", 0, ContainerState.COMPLETE,
        "http://" + NodeId.newInstance("host", 2345).toString());
    ContainerReport container2 = ContainerReport.newInstance(containerId2, null,
        NodeId.newInstance("host", 1234), Priority.UNDEFINED, time1,0,
        "diagnosticInfo", "", 0, ContainerState.RUNNING,
        "http://" + NodeId.newInstance("host", 2345).toString());
    List<ContainerReport> reports = new ArrayList<ContainerReport>();
    reports.add(container);
    reports.add(container1);
    reports.add(container2);
    when(client.getContainers(any(ApplicationAttemptId.class))).thenReturn(
        reports);
    sysOutStream.reset();
    int result = cli.run(new String[] { "container", "-list",
        attemptId.toString() });
    assertEquals(0, result);
    verify(client).getContainers(attemptId);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStreamWriter stream =
        new OutputStreamWriter(baos, "UTF-8");
    PrintWriter pw = new PrintWriter(stream);
    pw.println("Total number of containers :3");
    pw.printf(ApplicationCLI.CONTAINER_PATTERN, "Container-Id", "Start Time",
        "Finish Time", "State", "Host", "Node Http Address", "LOG-URL");
    pw.printf(ApplicationCLI.CONTAINER_PATTERN, "container_1234_0005_01_000001",
        Times.format(time1), Times.format(time2),
        "COMPLETE", "host:1234", "http://host:2345", "logURL");
    pw.printf(ApplicationCLI.CONTAINER_PATTERN, "container_1234_0005_01_000002",
        Times.format(time1), Times.format(time2),
        "COMPLETE", "host:1234", "http://host:2345", "logURL");
    pw.printf(ApplicationCLI.CONTAINER_PATTERN, "container_1234_0005_01_000003",
        Times.format(time1), "N/A", "RUNNING", "host:1234",
        "http://host:2345", "");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Log.getLog().info("ExpectedOutput");
    Log.getLog().info("["+appReportStr+"]");
    Log.getLog().info("OutputFrom command");
    String actualOutput = sysOutStream.toString("UTF-8");
    Log.getLog().info("["+actualOutput+"]");
    Assert.assertEquals(appReportStr, actualOutput);
  }
  
  @Test
  public void testGetApplicationReportException() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    when(client.getApplicationReport(any(ApplicationId.class))).thenThrow(
        new ApplicationNotFoundException("History file for application"
            + applicationId + " is not found"));
    int exitCode = cli.run(new String[] { "application", "-status",
        applicationId.toString() });
    verify(sysOut).println(
        "Application with id '" + applicationId
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);
  }

  @Test
  public void testGetApplications() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null,
        Sets.newHashSet("tag1", "tag3"), false, Priority.UNDEFINED, "", "");
    List<ApplicationReport> applicationReports =
        new ArrayList<ApplicationReport>();
    applicationReports.add(newApplicationReport);

    ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId2, ApplicationAttemptId.newInstance(applicationId2, 2),
        "user2", "queue2", "appname2", "host2", 125, null,
        YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2, 2,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN", 
        null, Sets.newHashSet("tag2", "tag3"), false, Priority.UNDEFINED,
        "", "");
    applicationReports.add(newApplicationReport2);

    ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
    ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
        applicationId3, ApplicationAttemptId.newInstance(applicationId3, 3),
        "user3", "queue3", "appname3", "host3", 126, null,
        YarnApplicationState.RUNNING, "diagnostics3", "url3", 3, 3, 3,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE", 
        null, Sets.newHashSet("tag1", "tag4"), false, Priority.UNDEFINED,
        "", "");
    applicationReports.add(newApplicationReport3);

    ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);
    ApplicationReport newApplicationReport4 = ApplicationReport.newInstance(
        applicationId4, ApplicationAttemptId.newInstance(applicationId4, 4),
        "user4", "queue4", "appname4", "host4", 127, null,
        YarnApplicationState.FAILED, "diagnostics4", "url4", 4, 4, 4,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.83789f,
        "NON-MAPREDUCE", null, Sets.newHashSet("tag1"), false,
        Priority.UNDEFINED, "", "");
    applicationReports.add(newApplicationReport4);

    ApplicationId applicationId5 = ApplicationId.newInstance(1234, 9);
    ApplicationReport newApplicationReport5 = ApplicationReport.newInstance(
        applicationId5, ApplicationAttemptId.newInstance(applicationId5, 5),
        "user5", "queue5", "appname5", "host5", 128, null,
        YarnApplicationState.ACCEPTED, "diagnostics5", "url5", 5, 5, 5,
        FinalApplicationStatus.KILLED, null, "N/A", 0.93789f, "HIVE", null,
        Sets.newHashSet("tag2", "tag4"), false, Priority.UNDEFINED, "", "");
    applicationReports.add(newApplicationReport5);

    ApplicationId applicationId6 = ApplicationId.newInstance(1234, 10);
    ApplicationReport newApplicationReport6 = ApplicationReport.newInstance(
        applicationId6, ApplicationAttemptId.newInstance(applicationId6, 6),
        "user6", "queue6", "appname6", "host6", 129, null,
        YarnApplicationState.SUBMITTED, "diagnostics6", "url6", 6, 6, 6,
        FinalApplicationStatus.KILLED, null, "N/A", 0.99789f, "PIG",
        null, new HashSet<String>(), false, Priority.UNDEFINED, "", "");
    applicationReports.add(newApplicationReport6);

    // Test command yarn application -list
    // if the set appStates is empty, RUNNING state will be automatically added
    // to the appStates list
    // the output of yarn application -list should be the same as
    // equals to yarn application -list --appStates RUNNING,ACCEPTED,SUBMITTED
    Set<String> appType1 = new HashSet<String>();
    EnumSet<YarnApplicationState> appState1 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState1.add(YarnApplicationState.RUNNING);
    appState1.add(YarnApplicationState.ACCEPTED);
    appState1.add(YarnApplicationState.SUBMITTED);
    Set<String> appTag = new HashSet<String>();
    when(client.getApplications(appType1, appState1, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType1, appState1, appTag, false));
    int result = cli.run(new String[] { "application", "-list" });
    assertEquals(0, result);
    verify(client).getApplications(appType1, appState1, appTag);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType1
        + ", states: " + appState1 + " and tags: " + appTag + ")" + ":" + 4);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0007\t            ");
    pw.print("appname3\t           MAPREDUCE\t     user3\t    ");
    pw.print("queue3\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         73.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0009\t            ");
    pw.print("appname5\t                HIVE\t     user5\t    ");
    pw.print("queue5\t          ACCEPTED\t            ");
    pw.print("KILLED\t         93.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0010\t            ");
    pw.print("appname6\t                 PIG\t     user6\t    ");
    pw.print("queue6\t         SUBMITTED\t            ");
    pw.print("KILLED\t         99.79%");
    pw.println("\t                                N/A");
    pw.close();
    String appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());

    //Test command yarn application -list --appTypes apptype1,apptype2
    //the output should be the same as
    // yarn application -list --appTypes apptyp1, apptype2 --appStates
    // RUNNING,ACCEPTED,SUBMITTED
    sysOutStream.reset();
    Set<String> appType2 = new HashSet<String>();
    appType2.add("YARN");
    appType2.add("NON-YARN");

    EnumSet<YarnApplicationState> appState2 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState2.add(YarnApplicationState.RUNNING);
    appState2.add(YarnApplicationState.ACCEPTED);
    appState2.add(YarnApplicationState.SUBMITTED);
    when(client.getApplications(appType2, appState2, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType2, appState2, appTag, false));
    result =
        cli.run(new String[] { "application", "-list", "-appTypes",
            "YARN, ,,  NON-YARN", "   ,, ,," });
    assertEquals(0, result);
    verify(client).getApplications(appType2, appState2, appTag);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType2
        + ", states: " + appState2 + " and tags: " + appTag + ")" + ":" + 1);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(2)).write(any(byte[].class), anyInt(), anyInt());

    //Test command yarn application -list --appStates appState1,appState2
    sysOutStream.reset();
    Set<String> appType3 = new HashSet<String>();

    EnumSet<YarnApplicationState> appState3 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState3.add(YarnApplicationState.FINISHED);
    appState3.add(YarnApplicationState.FAILED);

    when(client.getApplications(appType3, appState3, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType3, appState3, appTag, false));
    result =
        cli.run(new String[] { "application", "-list", "--appStates",
            "FINISHED ,, , FAILED", ",,FINISHED" });
    assertEquals(0, result);
    verify(client).getApplications(appType3, appState3, appTag);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType3
        + ", states: " + appState3 + " and tags: " + appTag + ")" + ":" + 2);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0008\t            ");
    pw.print("appname4\t       NON-MAPREDUCE\t     user4\t    ");
    pw.print("queue4\t            FAILED\t         ");
    pw.print("SUCCEEDED\t         83.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(3)).write(any(byte[].class), anyInt(), anyInt());

    // Test command yarn application -list --appTypes apptype1,apptype2
    // --appStates appstate1,appstate2
    sysOutStream.reset();
    Set<String> appType4 = new HashSet<String>();
    appType4.add("YARN");
    appType4.add("NON-YARN");

    EnumSet<YarnApplicationState> appState4 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState4.add(YarnApplicationState.FINISHED);
    appState4.add(YarnApplicationState.FAILED);

    when(client.getApplications(appType4, appState4, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType4, appState4, appTag, false));
    result =
        cli.run(new String[] { "application", "-list", "--appTypes",
            "YARN,NON-YARN", "--appStates", "FINISHED ,, , FAILED" });
    assertEquals(0, result);
    verify(client).getApplications(appType2, appState2, appTag);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType4
        + ", states: " + appState4 + " and tags: " + appTag + ")" + ":" + 1);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(4)).write(any(byte[].class), anyInt(), anyInt());

    //Test command yarn application -list --appStates with invalid appStates
    sysOutStream.reset();
    result =
        cli.run(new String[] { "application", "-list", "--appStates",
            "FINISHED ,, , INVALID" });
    assertEquals(-1, result);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("The application state  INVALID is invalid.");
    pw.print("The valid application state can be one of the following: ");
    StringBuilder sb = new StringBuilder();
    sb.append("ALL,");
    for(YarnApplicationState state : YarnApplicationState.values()) {
      sb.append(state+",");
    }
    String output = sb.toString();
    pw.println(output.substring(0, output.length()-1));
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(4)).write(any(byte[].class), anyInt(), anyInt());

    //Test command yarn application -list --appStates all
    sysOutStream.reset();
    Set<String> appType5 = new HashSet<String>();

    EnumSet<YarnApplicationState> appState5 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState5.add(YarnApplicationState.FINISHED);
    when(client.getApplications(appType5, appState5, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType5, appState5, appTag, true));
    result =
        cli.run(new String[] { "application", "-list", "--appStates",
            "FINISHED ,, , ALL" });
    assertEquals(0, result);
    verify(client).getApplications(appType5, appState5, appTag);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType5
        + ", states: " + appState5 + " and tags: " + appTag + ")" + ":" + 6);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0007\t            ");
    pw.print("appname3\t           MAPREDUCE\t     user3\t    ");
    pw.print("queue3\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         73.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0008\t            ");
    pw.print("appname4\t       NON-MAPREDUCE\t     user4\t    ");
    pw.print("queue4\t            FAILED\t         ");
    pw.print("SUCCEEDED\t         83.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0009\t            ");
    pw.print("appname5\t                HIVE\t     user5\t    ");
    pw.print("queue5\t          ACCEPTED\t            ");
    pw.print("KILLED\t         93.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0010\t            ");
    pw.print("appname6\t                 PIG\t     user6\t    ");
    pw.print("queue6\t         SUBMITTED\t            ");
    pw.print("KILLED\t         99.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(5)).write(any(byte[].class), anyInt(), anyInt());

    // Test command yarn application user case insensitive
    sysOutStream.reset();
    Set<String> appType6 = new HashSet<String>();
    appType6.add("YARN");
    appType6.add("NON-YARN");

    EnumSet<YarnApplicationState> appState6 =
        EnumSet.noneOf(YarnApplicationState.class);
    appState6.add(YarnApplicationState.FINISHED);
    when(client.getApplications(appType6, appState6, appTag)).thenReturn(
        getApplicationReports(
            applicationReports, appType6, appState6, appTag, false));
    result =
        cli.run(new String[] { "application", "-list", "-appTypes",
            "YARN, ,,  NON-YARN", "--appStates", "finished" });
    assertEquals(0, result);
    verify(client).getApplications(appType6, appState6, appTag);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType6
        + ", states: " + appState6 + " and tags: " + appTag + ")" + ":" + 1);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(6)).write(any(byte[].class), anyInt(), anyInt());

    // Test command yarn application with tags.
    sysOutStream.reset();
    Set<String> appTag1 = Sets.newHashSet("tag1");
    when(client.getApplications(appType1, appState1, appTag1)).thenReturn(
        getApplicationReports(
            applicationReports, appType1, appState1, appTag1, false));
    result =
        cli.run(new String[] { "application", "-list", "-appTags", "tag1" });
    assertEquals(0, result);
    verify(client).getApplications(appType1, appState1, appTag1);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType1
        + ", states: " + appState1 + " and tags: " + appTag1 + ")" + ":" + 2);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0007\t            ");
    pw.print("appname3\t           MAPREDUCE\t     user3\t    ");
    pw.print("queue3\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         73.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(7)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    EnumSet<YarnApplicationState> appState7 =
        EnumSet.of(YarnApplicationState.RUNNING, YarnApplicationState.FAILED);
    when(client.getApplications(appType1, appState7, appTag1)).thenReturn(
        getApplicationReports(
            applicationReports, appType1, appState7, appTag1, false));
    result = cli.run(
        new String[] { "application", "-list", "-appStates", "RUNNING,FAILED",
            "-appTags", "tag1" });
    assertEquals(0, result);
    verify(client).getApplications(appType1, appState7, appTag1);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType1
        + ", states: " + appState7 + " and tags: " + appTag1 + ")" + ":" + 3);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0007\t            ");
    pw.print("appname3\t           MAPREDUCE\t     user3\t    ");
    pw.print("queue3\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         73.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0008\t            ");
    pw.print("appname4\t       NON-MAPREDUCE\t     user4\t    ");
    pw.print("queue4\t            FAILED\t         ");
    pw.print("SUCCEEDED\t         83.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(8)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    Set<String> appType9 = Sets.newHashSet("YARN");
    Set<String> appTag2 = Sets.newHashSet("tag3");
    when(client.getApplications(appType9, appState1, appTag2)).thenReturn(
        getApplicationReports(
            applicationReports, appType9, appState1, appTag2, false));
    result = cli.run(new String[] { "application", "-list", "-appTypes", "YARN",
        "-appTags", "tag3" });
    assertEquals(0, result);
    verify(client).getApplications(appType9, appState1, appTag2);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType9
        + ", states: " + appState1 + " and tags: " + appTag2 + ")" + ":" + 1);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t           RUNNING\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(9)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    Set<String> appType10 = Sets.newHashSet("HIVE");
    Set<String> appTag3 = Sets.newHashSet("tag4");
    EnumSet<YarnApplicationState> appState10 =
        EnumSet.of(YarnApplicationState.ACCEPTED);
    when(client.getApplications(appType10, appState10, appTag3)).thenReturn(
        getApplicationReports(
            applicationReports, appType10, appState10, appTag3, false));
    result = cli.run(new String[] { "application", "-list", "-appTypes", "HIVE",
        "-appStates", "ACCEPTED", "-appTags", "tag4" });
    assertEquals(0, result);
    verify(client).getApplications(appType10, appState10, appTag3);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total number of applications (application-types: " + appType10
        + ", states: " + appState10 + " and tags: " + appTag3 + ")" + ":" + 1);
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0009\t            ");
    pw.print("appname5\t                HIVE\t     user5\t    ");
    pw.print("queue5\t          ACCEPTED\t            ");
    pw.print("KILLED\t         93.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(10)).write(any(byte[].class), anyInt(), anyInt());
  }

  private List<ApplicationReport> getApplicationReports(
      List<ApplicationReport> applicationReports,
      Set<String> appTypes, EnumSet<YarnApplicationState> appStates,
      Set<String> appTags, boolean allStates) {

    List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();

    if (allStates) {
      for(YarnApplicationState state : YarnApplicationState.values()) {
        appStates.add(state);
      }
    }
    for (ApplicationReport appReport : applicationReports) {
      if (appTypes != null && !appTypes.isEmpty()) {
        if (!appTypes.contains(appReport.getApplicationType())) {
          continue;
        }
      }

      if (appStates != null && !appStates.isEmpty()) {
        if (!appStates.contains(appReport.getYarnApplicationState())) {
          continue;
        }
      }

      if (appTags != null && !appTags.isEmpty()) {
        Set<String> tags = appReport.getApplicationTags();
        if (tags == null || tags.isEmpty()) {
          continue;
        }
        boolean match = false;
        for (String appTag : appTags) {
          if (tags.contains(appTag)) {
            match = true;
            break;
          }
        }
        if (!match) {
          continue;
        }
      }
      appReports.add(appReport);
    }
    return appReports;
  }

  @Test (timeout = 10000)
  public void testAppsHelpCommand() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationCLI spyCli = spy(cli);
    int result = spyCli.run(new String[] { "application", "-help" });
    Assert.assertTrue(result == 0);
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createApplicationCLIHelpMessage(),
        sysOutStream.toString());

    sysOutStream.reset();
    NodeId nodeId = NodeId.newInstance("host0", 0);
    result = cli.run(
        new String[] { "application", "-status", nodeId.toString(), "args" });
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createApplicationCLIHelpMessage(),
        sysOutStream.toString());
  }

  @Test (timeout = 10000)
  public void testAppAttemptsHelpCommand() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationCLI spyCli = spy(cli);
    int result = spyCli.run(new String[] { "applicationattempt", "-help" });
    Assert.assertTrue(result == 0);
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createApplicationAttemptCLIHelpMessage(),
        sysOutStream.toString());

    sysOutStream.reset();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    result = cli.run(
        new String[] {"applicationattempt", "-list", applicationId.toString(),
            "args" });
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createApplicationAttemptCLIHelpMessage(),
        sysOutStream.toString());

    sysOutStream.reset();
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 6);
    result = cli.run(
        new String[] { "applicationattempt", "-status", appAttemptId.toString(),
            "args" });
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createApplicationAttemptCLIHelpMessage(),
        sysOutStream.toString());
  }

  @Test (timeout = 10000)
  public void testContainersHelpCommand() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationCLI spyCli = spy(cli);
    int result = spyCli.run(new String[] { "container", "-help" });
    Assert.assertTrue(result == 0);
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createContainerCLIHelpMessage(),
        normalize(sysOutStream.toString()));

    sysOutStream.reset();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(applicationId, 6);
    result = cli.run(
        new String[] {"container", "-list", appAttemptId.toString(), "args" });
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createContainerCLIHelpMessage(),
        normalize(sysOutStream.toString()));

    sysOutStream.reset();
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 7);
    result = cli.run(
        new String[] { "container", "-status", containerId.toString(), "args" });
    verify(spyCli).printUsage(any(String.class), any(Options.class));
    Assert.assertEquals(createContainerCLIHelpMessage(),
        normalize(sysOutStream.toString()));
  }

  @Test (timeout = 5000)
  public void testNodesHelpCommand() throws Exception {
    NodeCLI nodeCLI = new NodeCLI();
    nodeCLI.setClient(client);
    nodeCLI.setSysOutPrintStream(sysOut);
    nodeCLI.setSysErrPrintStream(sysErr);
    nodeCLI.run(new String[] {});
    Assert.assertEquals(createNodeCLIHelpMessage(),
        sysOutStream.toString());
  }

  @Test
  public void testKillApplication() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);

    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport2);
    int result = cli.run(new String[] { "application","-kill", applicationId.toString() });
    assertEquals(0, result);
    verify(client, times(0)).killApplication(any(ApplicationId.class));
    verify(sysOut).println(
        "Application " + applicationId + " has already finished ");

    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    result = cli.run(new String[] { "application","-kill", applicationId.toString() });
    assertEquals(0, result);
    verify(client).killApplication(any(ApplicationId.class));
    verify(sysOut).println("Killing application application_1234_0005");

    doThrow(new ApplicationNotFoundException("Application with id '"
        + applicationId + "' doesn't exist in RM.")).when(client)
        .getApplicationReport(applicationId);
    cli = createAndGetAppCLI();
    try {
      int exitCode =
          cli.run(new String[] { "application","-kill", applicationId.toString() });
      verify(sysOut).println("Application with id '" + applicationId +
              "' doesn't exist in RM.");
      Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);
    } catch (ApplicationNotFoundException appEx) {
      Assert.fail("application -kill should not throw" +
          "ApplicationNotFoundException. " + appEx);
    } catch (Exception e) {
      Assert.fail("Unexpected exception: " + e);
    }
  }

  @Test
  public void testKillApplications() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId1 = ApplicationId.newInstance(1234, 5);
    ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
    ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
    ApplicationId applicationId4 = ApplicationId.newInstance(1234, 8);

    // Test Scenario 1: Both applications are FINISHED.
    ApplicationReport newApplicationReport1 = ApplicationReport.newInstance(
        applicationId1, ApplicationAttemptId.newInstance(applicationId1, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId2, ApplicationAttemptId.newInstance(applicationId2, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.34344f, "YARN", null);
    when(client.getApplicationReport(applicationId1)).thenReturn(
        newApplicationReport1);
    when(client.getApplicationReport(applicationId2)).thenReturn(
        newApplicationReport2);
    int result = cli.run(new String[]{"application", "-kill",
        applicationId1.toString() + " " + applicationId2.toString()});
    assertEquals(0, result);
    verify(client, times(0)).killApplication(applicationId1);
    verify(client, times(0)).killApplication(applicationId2);
    verify(sysOut).println(
        "Application " + applicationId1 + " has already finished ");
    verify(sysOut).println(
        "Application " + applicationId2 + " has already finished ");

    // Test Scenario 2: Both applications are RUNNING.
    ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
        applicationId1, ApplicationAttemptId.newInstance(applicationId1, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    ApplicationReport newApplicationReport4 = ApplicationReport.newInstance(
        applicationId2, ApplicationAttemptId.newInstance(applicationId2, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53345f, "YARN", null);
    when(client.getApplicationReport(applicationId1)).thenReturn(
        newApplicationReport3);
    when(client.getApplicationReport(applicationId2)).thenReturn(
        newApplicationReport4);
    result = cli.run(new String[]{"application", "-kill",
        applicationId1.toString() + " " + applicationId2.toString()});
    assertEquals(0, result);
    verify(client).killApplication(applicationId1);
    verify(client).killApplication(applicationId2);
    verify(sysOut).println(
        "Killing application application_1234_0005");
    verify(sysOut).println(
        "Killing application application_1234_0006");

    // Test Scenario 3: Both applications are not present.
    doThrow(new ApplicationNotFoundException("Application with id '"
        + applicationId3 + "' doesn't exist in RM.")).when(client)
        .getApplicationReport(applicationId3);
    doThrow(new ApplicationNotFoundException("Application with id '"
        + applicationId4 + "' doesn't exist in RM.")).when(client)
        .getApplicationReport(applicationId4);
    result = cli.run(new String[]{"application", "-kill",
        applicationId3.toString() + " " + applicationId4.toString()});
    Assert.assertNotEquals(0, result);
    verify(sysOut).println(
        "Application with id 'application_1234_0007' doesn't exist in RM.");
    verify(sysOut).println(
        "Application with id 'application_1234_0008' doesn't exist in RM.");

    // Test Scenario 4: one application is not present and other RUNNING
    doThrow(new ApplicationNotFoundException("Application with id '"
        + applicationId3 + "' doesn't exist in RM.")).when(client)
        .getApplicationReport(applicationId3);
    ApplicationReport newApplicationReport5 = ApplicationReport.newInstance(
        applicationId1, ApplicationAttemptId.newInstance(applicationId1, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53345f, "YARN", null);
    when(client.getApplicationReport(applicationId1)).thenReturn(
        newApplicationReport5);
    result = cli.run(new String[]{"application", "-kill",
        applicationId3.toString() + " " + applicationId1.toString()});
    Assert.assertEquals(0, result);

    // Test Scenario 5: kill operation with some other command.
    sysOutStream.reset();
    result = cli.run(new String[]{"application", "--appStates", "RUNNING",
        "-kill", applicationId3.toString() + " " + applicationId1.toString()});
    Assert.assertEquals(-1, result);
    Assert.assertEquals(createApplicationCLIHelpMessage(),
        sysOutStream.toString());
  }

  @Test
  public void testKillApplicationsOfDifferentEndStates() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId1 = ApplicationId.newInstance(1234, 5);
    ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);

    // Scenario: One application is FINISHED and other is RUNNING.
    ApplicationReport newApplicationReport5 = ApplicationReport.newInstance(
        applicationId1, ApplicationAttemptId.newInstance(applicationId1, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    ApplicationReport newApplicationReport6 = ApplicationReport.newInstance(
        applicationId2, ApplicationAttemptId.newInstance(applicationId2, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53345f, "YARN", null);
    when(client.getApplicationReport(applicationId1)).thenReturn(
        newApplicationReport5);
    when(client.getApplicationReport(applicationId2)).thenReturn(
        newApplicationReport6);
    int result = cli.run(new String[]{"application", "-kill",
        applicationId1.toString() + " " + applicationId2.toString()});
    assertEquals(0, result);
    verify(client, times(1)).killApplication(applicationId2);
    verify(sysOut).println(
        "Application " + applicationId1 + " has already finished ");
    verify(sysOut).println("Killing application application_1234_0006");
  }

  @Test
  public void testMoveApplicationAcrossQueues() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);

    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport2);
    int result = cli.run(new String[] { "application", "-movetoqueue",
        applicationId.toString(), "-queue", "targetqueue"});
    assertEquals(0, result);
    verify(client, times(0)).moveApplicationAcrossQueues(
        any(ApplicationId.class), any(String.class));
    verify(sysOut).println(
        "Application " + applicationId + " has already finished ");

    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    result = cli.run(new String[] { "application", "-movetoqueue",
        applicationId.toString(), "-queue", "targetqueue"});
    assertEquals(0, result);
    verify(client).moveApplicationAcrossQueues(any(ApplicationId.class),
        any(String.class));
    verify(sysOut).println("Moving application application_1234_0005 to queue targetqueue");
    verify(sysOut).println("Successfully completed move.");

    doThrow(new ApplicationNotFoundException("Application with id '"
        + applicationId + "' doesn't exist in RM.")).when(client)
        .moveApplicationAcrossQueues(applicationId, "targetqueue");
    cli = createAndGetAppCLI();
    try {
      result = cli.run(new String[] { "application", "-movetoqueue",
          applicationId.toString(), "-queue", "targetqueue"});
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof ApplicationNotFoundException);
      Assert.assertEquals("Application with id '" + applicationId +
          "' doesn't exist in RM.", ex.getMessage());
    }
  }

  @Test
  public void testMoveApplicationAcrossQueuesWithNewCommand() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);

    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class)))
        .thenReturn(newApplicationReport2);
    int result = cli.run(new String[]{"application", "-appId",
        applicationId.toString(), "-changeQueue", "targetqueue"});
    assertEquals(0, result);
    verify(client, times(0)).moveApplicationAcrossQueues(
        any(ApplicationId.class), any(String.class));
    verify(sysOut)
        .println("Application " + applicationId + " has already finished ");

    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN", null);
    when(client.getApplicationReport(any(ApplicationId.class)))
        .thenReturn(newApplicationReport);
    result = cli.run(new String[]{"application", "-appId",
        applicationId.toString(), "-changeQueue", "targetqueue"});
    assertEquals(0, result);
    verify(client).moveApplicationAcrossQueues(any(ApplicationId.class),
        any(String.class));
    verify(sysOut).println(
        "Moving application application_1234_0005 to queue targetqueue");
    verify(sysOut).println("Successfully completed move.");

    doThrow(new ApplicationNotFoundException(
        "Application with id '" + applicationId + "' doesn't exist in RM."))
            .when(client)
            .moveApplicationAcrossQueues(applicationId, "targetqueue");
    cli = createAndGetAppCLI();
    try {
      result = cli.run(new String[]{"application", "-appId",
          applicationId.toString(), "-changeQueue", "targetqueue"});
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof ApplicationNotFoundException);
      Assert.assertEquals(
          "Application with id '" + applicationId + "' doesn't exist in RM.",
          ex.getMessage());
    }
  }

  @Test
  public void testListClusterNodes() throws Exception {
    List<NodeReport> nodeReports = new ArrayList<NodeReport>();
    nodeReports.addAll(getNodeReports(1, NodeState.NEW));
    nodeReports.addAll(getNodeReports(2, NodeState.RUNNING));
    nodeReports.addAll(getNodeReports(1, NodeState.UNHEALTHY));
    nodeReports.addAll(getNodeReports(1, NodeState.DECOMMISSIONED));
    nodeReports.addAll(getNodeReports(1, NodeState.REBOOTED));
    nodeReports.addAll(getNodeReports(1, NodeState.LOST));

    NodeCLI cli = new NodeCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);

    Set<NodeState> nodeStates = new HashSet<NodeState>();
    nodeStates.add(NodeState.NEW);
    NodeState[] states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    int result = cli.run(new String[] {"-list", "-states", "NEW"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Total Nodes:1");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t            NEW\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    String nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.RUNNING);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states", "RUNNING"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:2");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host1:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(2)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    result = cli.run(new String[] {"-list"});
    assertEquals(0, result);
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(3)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    result = cli.run(new String[] {"-list", "-showDetails"});
    assertEquals(0, result);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:2");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.println("Detailed Node Information :");
    pw.println("\tConfigured Resources : <memory:0, vCores:0>");
    pw.println("\tAllocated Resources : <memory:0, vCores:0>");
    pw.println("\tResource Utilization by Node : PMem:2048 MB, VMem:4096 MB, VCores:8.0");
    pw.println("\tResource Utilization by Containers : PMem:1024 MB, VMem:2048 MB, VCores:4.0");
    pw.println("\tNode-Labels : ");
    pw.print("         host1:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.println("Detailed Node Information :");
    pw.println("\tConfigured Resources : <memory:0, vCores:0>");
    pw.println("\tAllocated Resources : <memory:0, vCores:0>");
    pw.println("\tResource Utilization by Node : PMem:2048 MB, VMem:4096 MB, VCores:8.0");
    pw.println("\tResource Utilization by Containers : PMem:1024 MB, VMem:2048 MB, VCores:4.0");
    pw.println("\tNode-Labels : ");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(4)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.UNHEALTHY);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states", "UNHEALTHY"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:1");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t      UNHEALTHY\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(5)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.DECOMMISSIONED);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states", "DECOMMISSIONED"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:1");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t DECOMMISSIONED\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(6)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.REBOOTED);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states", "REBOOTED"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:1");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t       REBOOTED\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(7)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.LOST);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states", "LOST"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:1");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t           LOST\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(8)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    nodeStates.add(NodeState.NEW);
    nodeStates.add(NodeState.RUNNING);
    nodeStates.add(NodeState.LOST);
    nodeStates.add(NodeState.REBOOTED);
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-states",
                                        "NEW,RUNNING,LOST,REBOOTED"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:5");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t            NEW\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host1:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t       REBOOTED\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t           LOST\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(9)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    nodeStates.clear();
    for (NodeState s : NodeState.values()) {
      nodeStates.add(s);
    }
    states = nodeStates.toArray(new NodeState[0]);
    when(client.getNodeReports(states))
        .thenReturn(getNodeReports(nodeReports, nodeStates));
    result = cli.run(new String[] {"-list", "-All"});
    assertEquals(0, result);
    verify(client).getNodeReports(states);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Nodes:7");
    pw.print("         Node-Id\t     Node-State\tNode-Http-Address\t");
    pw.println("Number-of-Running-Containers");
    pw.print("         host0:0\t            NEW\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host1:0\t        RUNNING\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t      UNHEALTHY\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t DECOMMISSIONED\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t       REBOOTED\t       host1:8888\t");
    pw.println("                           0");
    pw.print("         host0:0\t           LOST\t       host1:8888\t");
    pw.println("                           0");
    pw.close();
    nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(10)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    result = cli.run(new String[] { "-list", "-states", "InvalidState"});
    assertEquals(-1, result);
  }

  private List<NodeReport> getNodeReports(
      List<NodeReport> nodeReports,
      Set<NodeState> nodeStates) {
    List<NodeReport> reports = new ArrayList<NodeReport>();

    for (NodeReport nodeReport : nodeReports) {
      if (nodeStates.contains(nodeReport.getNodeState())) {
        reports.add(nodeReport);
      }
    }
    return reports;
  }

  @Test
  public void testNodeStatus() throws Exception {
    NodeId nodeId = NodeId.newInstance("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(
                    getNodeReports(3, NodeState.RUNNING, false));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Node Report : ");
    pw.println("\tNode-Id : host0:0");
    pw.println("\tRack : rack1");
    pw.println("\tNode-State : RUNNING");
    pw.println("\tNode-Http-Address : host1:8888");
    pw.println("\tLast-Health-Update : "
      + DateFormatUtils.format(new Date(0), "E dd/MMM/yy hh:mm:ss:SSzz"));
    pw.println("\tHealth-Report : ");
    pw.println("\tContainers : 0");
    pw.println("\tMemory-Used : 0MB");
    pw.println("\tMemory-Capacity : 0MB");
    pw.println("\tCPU-Used : 0 vcores");
    pw.println("\tCPU-Capacity : 0 vcores");
    pw.println("\tNode-Labels : a,b,c,x,y,z");
    pw.println("\tResource Utilization by Node : PMem:2048 MB, VMem:4096 MB, VCores:8.0");
    pw.println("\tResource Utilization by Containers : PMem:1024 MB, VMem:2048 MB, VCores:4.0");
    pw.close();
    String nodeStatusStr = baos.toString("UTF-8");
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatusStr);
  }
  
  @Test
  public void testNodeStatusWithEmptyNodeLabels() throws Exception {
    NodeId nodeId = NodeId.newInstance("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(
                    getNodeReports(3, NodeState.RUNNING));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Node Report : ");
    pw.println("\tNode-Id : host0:0");
    pw.println("\tRack : rack1");
    pw.println("\tNode-State : RUNNING");
    pw.println("\tNode-Http-Address : host1:8888");
    pw.println("\tLast-Health-Update : "
      + DateFormatUtils.format(new Date(0), "E dd/MMM/yy hh:mm:ss:SSzz"));
    pw.println("\tHealth-Report : ");
    pw.println("\tContainers : 0");
    pw.println("\tMemory-Used : 0MB");
    pw.println("\tMemory-Capacity : 0MB");
    pw.println("\tCPU-Used : 0 vcores");
    pw.println("\tCPU-Capacity : 0 vcores");
    pw.println("\tNode-Labels : ");
    pw.println("\tResource Utilization by Node : PMem:2048 MB, VMem:4096 MB, VCores:8.0");
    pw.println("\tResource Utilization by Containers : PMem:1024 MB, VMem:2048 MB, VCores:4.0");
    pw.close();
    String nodeStatusStr = baos.toString("UTF-8");
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatusStr);
  }

  @Test
  public void testNodeStatusWithEmptyResourceUtilization() throws Exception {
    NodeId nodeId = NodeId.newInstance("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(
                    getNodeReports(3, NodeState.RUNNING, false, true));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Node Report : ");
    pw.println("\tNode-Id : host0:0");
    pw.println("\tRack : rack1");
    pw.println("\tNode-State : RUNNING");
    pw.println("\tNode-Http-Address : host1:8888");
    pw.println("\tLast-Health-Update : "
      + DateFormatUtils.format(new Date(0), "E dd/MMM/yy hh:mm:ss:SSzz"));
    pw.println("\tHealth-Report : ");
    pw.println("\tContainers : 0");
    pw.println("\tMemory-Used : 0MB");
    pw.println("\tMemory-Capacity : 0MB");
    pw.println("\tCPU-Used : 0 vcores");
    pw.println("\tCPU-Capacity : 0 vcores");
    pw.println("\tNode-Labels : a,b,c,x,y,z");
    pw.println("\tResource Utilization by Node : ");
    pw.println("\tResource Utilization by Containers : ");
    pw.close();
    String nodeStatusStr = baos.toString("UTF-8");
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatusStr);
  }

  @Test
  public void testAbsentNodeStatus() throws Exception {
    NodeId nodeId = NodeId.newInstance("Absenthost0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(
                getNodeReports(0, NodeState.RUNNING));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(
      "Could not find the node report for node id : " + nodeId.toString());
  }

  @Test
  public void testAppCLIUsageInfo() throws Exception {
    verifyUsageInfo(new ApplicationCLI());
  }

  @Test
  public void testNodeCLIUsageInfo() throws Exception {
    verifyUsageInfo(new NodeCLI());
  }

  @Test
  public void testMissingArguments() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    int result = cli.run(new String[] { "application", "-status" });
    Assert.assertEquals(result, -1);
    Assert.assertEquals(String.format("Missing argument for options%n%1s",
        createApplicationCLIHelpMessage()), sysOutStream.toString());

    sysOutStream.reset();
    result = cli.run(new String[] { "applicationattempt", "-status" });
    Assert.assertEquals(result, -1);
    Assert.assertEquals(String.format("Missing argument for options%n%1s",
        createApplicationAttemptCLIHelpMessage()), sysOutStream.toString());

    sysOutStream.reset();
    result = cli.run(new String[] { "container", "-status" });
    Assert.assertEquals(result, -1);
    Assert.assertEquals(String.format("Missing argument for options %1s",
        createContainerCLIHelpMessage()), normalize(sysOutStream.toString()));

    sysOutStream.reset();
    NodeCLI nodeCLI = new NodeCLI();
    nodeCLI.setClient(client);
    nodeCLI.setSysOutPrintStream(sysOut);
    nodeCLI.setSysErrPrintStream(sysErr);
    result = nodeCLI.run(new String[] { "-status" });
    Assert.assertEquals(result, -1);
    Assert.assertEquals(String.format("Missing argument for options%n%1s",
        createNodeCLIHelpMessage()), sysOutStream.toString());
  }
  
  @Test
  public void testGetQueueInfo() throws Exception {
    QueueCLI cli = createAndGetQueueCLI();
    Set<String> nodeLabels = new HashSet<String>();
    nodeLabels.add("GPU");
    nodeLabels.add("JDK_7");
    QueueInfo queueInfo = QueueInfo.newInstance("queueA", 0.4f, 0.8f, 0.5f,
        null, null, QueueState.RUNNING, nodeLabels, "GPU", null, false, null,
        false);
    when(client.getQueueInfo(any(String.class))).thenReturn(queueInfo);
    int result = cli.run(new String[] { "-status", "queueA" });
    assertEquals(0, result);
    verify(client).getQueueInfo("queueA");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Queue Information : ");
    pw.println("Queue Name : " + "queueA");
    pw.println("\tState : " + "RUNNING");
    pw.println("\tCapacity : " + "40.0%");
    pw.println("\tCurrent Capacity : " + "50.0%");
    pw.println("\tMaximum Capacity : " + "80.0%");
    pw.println("\tDefault Node Label expression : " + "GPU");
    pw.println("\tAccessible Node Labels : " + "JDK_7,GPU");
    pw.println("\tPreemption : " + "enabled");
    pw.println("\tIntra-queue Preemption : " + "enabled");
    pw.close();
    String queueInfoStr = baos.toString("UTF-8");
    Assert.assertEquals(queueInfoStr, sysOutStream.toString());
  }

  @Test
  public void testGetQueueInfoOverrideIntraQueuePreemption() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        "org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity."
        + "ProportionalCapacityPreemptionPolicy");
    // Turn on cluster-wide intra-queue preemption
    conf.setBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED, true);
    // Disable intra-queue preemption for all queues
    conf.setBoolean(CapacitySchedulerConfiguration.PREFIX
        + "root.intra-queue-preemption.disable_preemption", true);
    // Enable intra-queue preemption for the a1 queue
    conf.setBoolean(CapacitySchedulerConfiguration.PREFIX
        + "root.a.a1.intra-queue-preemption.disable_preemption", false);
    MiniYARNCluster cluster =
        new MiniYARNCluster("testGetQueueInfoOverrideIntraQueuePreemption",
            2, 1, 1);

    YarnClient yarnClient = null;
    try {
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(yarnConf);
      yarnClient.start();

      QueueCLI cli = new QueueCLI();
      cli.setClient(yarnClient);
      cli.setSysOutPrintStream(sysOut);
      cli.setSysErrPrintStream(sysErr);
      sysOutStream.reset();
      // Get status for the root.a queue
      int result = cli.run(new String[] { "-status", "a" });
      assertEquals(0, result);
      String queueStatusOut = sysOutStream.toString();
      Assert.assertTrue(queueStatusOut
          .contains("\tPreemption : enabled"));
      // In-queue preemption is disabled at the "root.a" queue level
      Assert.assertTrue(queueStatusOut
          .contains("Intra-queue Preemption : disabled"));
      cli = new QueueCLI();
      cli.setClient(yarnClient);
      cli.setSysOutPrintStream(sysOut);
      cli.setSysErrPrintStream(sysErr);
      sysOutStream.reset();
      // Get status for the root.a.a1 queue
      result = cli.run(new String[] { "-status", "a1" });
      assertEquals(0, result);
      queueStatusOut = sysOutStream.toString();
      Assert.assertTrue(queueStatusOut
          .contains("\tPreemption : enabled"));
      // In-queue preemption is enabled at the "root.a.a1" queue level
      Assert.assertTrue(queueStatusOut
          .contains("Intra-queue Preemption : enabled"));
    } finally {
      // clean-up
      if (yarnClient != null) {
        yarnClient.stop();
      }
      cluster.stop();
      cluster.close();
    }
  }

  @Test
  public void testGetQueueInfoPreemptionEnabled() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        "org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity."
        + "ProportionalCapacityPreemptionPolicy");
    conf.setBoolean(
        CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED, true);
    MiniYARNCluster cluster =
        new MiniYARNCluster("testGetQueueInfoPreemptionEnabled", 2, 1, 1);

    YarnClient yarnClient = null;
    try {
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      yarnClient = YarnClient.createYarnClient();
      yarnClient.init(yarnConf);
      yarnClient.start();

      QueueCLI cli = new QueueCLI();
      cli.setClient(yarnClient);
      cli.setSysOutPrintStream(sysOut);
      cli.setSysErrPrintStream(sysErr);
      sysOutStream.reset();
      int result = cli.run(new String[] { "-status", "a1" });
      assertEquals(0, result);
      String queueStatusOut = sysOutStream.toString();
      Assert.assertTrue(queueStatusOut
          .contains("\tPreemption : enabled"));
      Assert.assertTrue(queueStatusOut
          .contains("Intra-queue Preemption : enabled"));
    } finally {
      // clean-up
      if (yarnClient != null) {
        yarnClient.stop();
      }
      cluster.stop();
      cluster.close();
    }
  }

  @Test
  public void testGetQueueInfoPreemptionDisabled() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();
    ReservationSystemTestUtil.setupQueueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.set(YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
        "org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity."
        + "ProportionalCapacityPreemptionPolicy");
    conf.setBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS, true);
    conf.setBoolean(PREFIX + "root.a.a1.disable_preemption", true);

    try (MiniYARNCluster cluster =
        new MiniYARNCluster("testReservationAPIs", 2, 1, 1);
         YarnClient yarnClient = YarnClient.createYarnClient()) {
      cluster.init(conf);
      cluster.start();
      final Configuration yarnConf = cluster.getConfig();
      yarnClient.init(yarnConf);
      yarnClient.start();

      QueueCLI cli = new QueueCLI();
      cli.setClient(yarnClient);
      cli.setSysOutPrintStream(sysOut);
      cli.setSysErrPrintStream(sysErr);
      sysOutStream.reset();
      int result = cli.run(new String[] { "-status", "a1" });
      assertEquals(0, result);
      String queueStatusOut = sysOutStream.toString();
      Assert.assertTrue(queueStatusOut
          .contains("\tPreemption : disabled"));
      Assert.assertTrue(queueStatusOut
          .contains("Intra-queue Preemption : disabled"));
    }
  }
  
  @Test
  public void testGetQueueInfoWithEmptyNodeLabel() throws Exception {
    QueueCLI cli = createAndGetQueueCLI();
    QueueInfo queueInfo = QueueInfo.newInstance("queueA", 0.4f, 0.8f, 0.5f,
        null, null, QueueState.RUNNING, null, null, null, true, null, true);
    when(client.getQueueInfo(any(String.class))).thenReturn(queueInfo);
    int result = cli.run(new String[] { "-status", "queueA" });
    assertEquals(0, result);
    verify(client).getQueueInfo("queueA");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Queue Information : ");
    pw.println("Queue Name : " + "queueA");
    pw.println("\tState : " + "RUNNING");
    pw.println("\tCapacity : " + "40.0%");
    pw.println("\tCurrent Capacity : " + "50.0%");
    pw.println("\tMaximum Capacity : " + "80.0%");
    pw.println("\tDefault Node Label expression : "
        + NodeLabel.DEFAULT_NODE_LABEL_PARTITION);
    pw.println("\tAccessible Node Labels : ");
    pw.println("\tPreemption : " + "disabled");
    pw.println("\tIntra-queue Preemption : " + "disabled");
    pw.close();
    String queueInfoStr = baos.toString("UTF-8");
    Assert.assertEquals(queueInfoStr, sysOutStream.toString());
  }
  
  @Test
  public void testGetQueueInfoWithNonExistedQueue() throws Exception {
    String queueName = "non-existed-queue";
    QueueCLI cli = createAndGetQueueCLI();
    when(client.getQueueInfo(any(String.class))).thenReturn(null);
    int result = cli.run(new String[] { "-status", queueName });
    assertEquals(-1, result);;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Cannot get queue from RM by queueName = " + queueName
        + ", please check.");
    pw.close();
    String queueInfoStr = baos.toString("UTF-8");
    Assert.assertEquals(queueInfoStr, sysOutStream.toString());
  }

  @Test
  public void testGetApplicationAttemptReportException() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId1 = ApplicationAttemptId.newInstance(
        applicationId, 1);
    when(client.getApplicationAttemptReport(attemptId1)).thenThrow(
        new ApplicationNotFoundException("History file for application"
            + applicationId + " is not found"));

    int exitCode = cli.run(new String[] { "applicationattempt", "-status",
        attemptId1.toString() });
    verify(sysOut).println(
        "Application for AppAttempt with id '" + attemptId1
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);

    ApplicationAttemptId attemptId2 = ApplicationAttemptId.newInstance(
        applicationId, 2);
    when(client.getApplicationAttemptReport(attemptId2)).thenThrow(
        new ApplicationAttemptNotFoundException(
            "History file for application attempt" + attemptId2
                + " is not found"));

    exitCode = cli.run(new String[] { "applicationattempt", "-status",
        attemptId2.toString() });
    verify(sysOut).println(
        "Application Attempt with id '" + attemptId2
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);
  }

  @Test
  public void testGetContainerReportException() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    long cntId = 1;
    ContainerId containerId1 = ContainerId.newContainerId(attemptId, cntId++);
    when(client.getContainerReport(containerId1)).thenThrow(
        new ApplicationNotFoundException("History file for application"
            + applicationId + " is not found"));

    int exitCode = cli.run(new String[] { "container", "-status",
        containerId1.toString() });
    verify(sysOut).println(
        "Application for Container with id '" + containerId1
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);
    ContainerId containerId2 = ContainerId.newContainerId(attemptId, cntId++);
    when(client.getContainerReport(containerId2)).thenThrow(
        new ApplicationAttemptNotFoundException(
            "History file for application attempt" + attemptId
                + " is not found"));

    exitCode = cli.run(new String[] { "container", "-status",
        containerId2.toString() });
    verify(sysOut).println(
        "Application Attempt for Container with id '" + containerId2
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);

    ContainerId containerId3 = ContainerId.newContainerId(attemptId, cntId++);
    when(client.getContainerReport(containerId3)).thenThrow(
        new ContainerNotFoundException("History file for container"
            + containerId3 + " is not found"));
    exitCode = cli.run(new String[] { "container", "-status",
        containerId3.toString() });
    verify(sysOut).println(
        "Container with id '" + containerId3
            + "' doesn't exist in RM or Timeline Server.");
    Assert.assertNotSame("should return non-zero exit code.", 0, exitCode);
  }

  @Test(timeout = 60000)
  public void testUpdateApplicationPriority() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 6);

    ApplicationReport appReport =
        ApplicationReport.newInstance(applicationId,
            ApplicationAttemptId.newInstance(applicationId, 1), "user",
            "queue", "appname", "host", 124, null,
            YarnApplicationState.RUNNING, "diagnostics", "url", 0, 0, 0,
            FinalApplicationStatus.UNDEFINED, null, "N/A", 0.53789f, "YARN",
            null);
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        appReport);

    int result =
        cli.run(new String[] { "application", "-appId",
            applicationId.toString(),
        "-updatePriority", "1" });
    Assert.assertEquals(result, 0);
    verify(client).updateApplicationPriority(any(ApplicationId.class),
        any(Priority.class));

  }

  @Test
  public void testFailApplicationAttempt() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    int exitCode =
        cli.run(new String[] { "applicationattempt", "-fail",
            "appattempt_1444199730803_0003_000001" });
    Assert.assertEquals(0, exitCode);

    verify(client).failApplicationAttempt(any(ApplicationAttemptId.class));
    verifyNoMoreInteractions(client);
  }

  private void verifyUsageInfo(YarnCLI cli) throws Exception {
    cli.setSysErrPrintStream(sysErr);
    cli.run(new String[] { "application" });
    verify(sysErr).println("Invalid Command Usage : ");
  }
  
  private List<NodeReport> getNodeReports(int noOfNodes, NodeState state) {
    return getNodeReports(noOfNodes, state, true, false);
  }

  private List<NodeReport> getNodeReports(int noOfNodes, NodeState state,
      boolean emptyNodeLabel) {
    return getNodeReports(noOfNodes, state, emptyNodeLabel, false);
  }

  private List<NodeReport> getNodeReports(int noOfNodes, NodeState state,
      boolean emptyNodeLabel, boolean emptyResourceUtilization) {
    List<NodeReport> nodeReports = new ArrayList<NodeReport>();

    for (int i = 0; i < noOfNodes; i++) {
      Set<String> nodeLabels = null;
      if (!emptyNodeLabel) {
        // node labels is not ordered, but when we output it, it should be
        // ordered
        nodeLabels = ImmutableSet.of("c", "b", "a", "x", "z", "y");
      }
      NodeReport nodeReport = NodeReport.newInstance(NodeId
        .newInstance("host" + i, 0), state, "host" + 1 + ":8888",
          "rack1", Records.newRecord(Resource.class), Records
              .newRecord(Resource.class), 0, "", 0, nodeLabels, null, null);
      if (!emptyResourceUtilization) {
        ResourceUtilization containersUtilization = ResourceUtilization
            .newInstance(1024, 2048, 4);
        ResourceUtilization nodeUtilization = ResourceUtilization.newInstance(
            2048, 4096, 8);
        nodeReport.setAggregatedContainersUtilization(containersUtilization);
        nodeReport.setNodeUtilization(nodeUtilization);
      }
      nodeReports.add(nodeReport);
    }
    return nodeReports;
  }

  private ApplicationCLI createAndGetAppCLI() {
    ApplicationCLI cli = new ApplicationCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    return cli;
  }
  
  private QueueCLI createAndGetQueueCLI() {
    QueueCLI cli = new QueueCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    return cli;
  }

  private String createApplicationCLIHelpMessage() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: application");
    pw.println(" -appId <Application ID>                  Specify Application Id to be");
    pw.println("                                          operated");
    pw.println(" -appStates <States>                      Works with -list to filter");
    pw.println("                                          applications based on input");
    pw.println("                                          comma-separated list of");
    pw.println("                                          application states. The valid");
    pw.println("                                          application state can be one of");
    pw.println("                                          the following:");
    pw.println("                                          ALL,NEW,NEW_SAVING,SUBMITTED,ACC");
    pw.println("                                          EPTED,RUNNING,FINISHED,FAILED,KI");
    pw.println("                                          LLED");
    pw.println(" -appTags <Tags>                          Works with -list to filter");
    pw.println("                                          applications based on input");
    pw.println("                                          comma-separated list of");
    pw.println("                                          application tags.");
    pw.println(" -appTypes <Types>                        Works with -list to filter");
    pw.println("                                          applications based on input");
    pw.println("                                          comma-separated list of");
    pw.println("                                          application types.");
    pw.println(" -autoFinalize                            Works with -upgrade and");
    pw.println("                                          -initiate options to initiate");
    pw.println("                                          the upgrade of the application");
    pw.println("                                          with the ability to finalize the");
    pw.println("                                          upgrade automatically.");
    pw.println(" -cancel                                  Works with -upgrade option to");
    pw.println("                                          cancel current upgrade.");
    pw.println(" -changeQueue <Queue Name>                Moves application to a new");
    pw.println("                                          queue. ApplicationId can be");
    pw.println("                                          passed using 'appId' option.");
    pw.println("                                          'movetoqueue' command is");
    pw.println("                                          deprecated, this new command");
    pw.println("                                          'changeQueue' performs same");
    pw.println("                                          functionality.");
    pw.println(" -component <Component Name> <Count>      Works with -flex option to");
    pw.println("                                          change the number of");
    pw.println("                                          components/containers running");
    pw.println("                                          for an application /");
    pw.println("                                          long-running service. Supports");
    pw.println("                                          absolute or relative changes,");
    pw.println("                                          such as +1, 2, or -3.");
    pw.println(" -components <Components>                 Works with -upgrade option to");
    pw.println("                                          trigger the upgrade of specified");
    pw.println("                                          components of the application.");
    pw.println("                                          Multiple components should be");
    pw.println("                                          separated by commas.");
    pw.println(" -decommission <Application Name>         Decommissions component");
    pw.println("                                          instances for an application /");
    pw.println("                                          long-running service. Requires");
    pw.println("                                          -instances option. Supports");
    pw.println("                                          -appTypes option to specify");
    pw.println("                                          which client implementation to");
    pw.println("                                          use.");
    pw.println(" -destroy <Application Name>              Destroys a saved application");
    pw.println("                                          specification and removes all");
    pw.println("                                          application data permanently.");
    pw.println("                                          Supports -appTypes option to");
    pw.println("                                          specify which client");
    pw.println("                                          implementation to use.");
    pw.println(" -enableFastLaunch <Destination Folder>   Uploads AM dependencies to HDFS");
    pw.println("                                          to make future launches faster.");
    pw.println("                                          Supports -appTypes option to");
    pw.println("                                          specify which client");
    pw.println("                                          implementation to use.");
    pw.println("                                          Optionally a destination folder");
    pw.println("                                          for the tarball can be");
    pw.println("                                          specified.");
    pw.println(" -express <arg>                           Works with -upgrade option to");
    pw.println("                                          perform express upgrade.  It");
    pw.println("                                          requires the upgraded");
    pw.println("                                          application specification file.");
    pw.println(" -finalize                                Works with -upgrade option to");
    pw.println("                                          finalize the upgrade.");
    pw.println(" -flex <Application Name or ID>           Changes number of running");
    pw.println("                                          containers for a component of an");
    pw.println("                                          application / long-running");
    pw.println("                                          service. Requires -component");
    pw.println("                                          option. If name is provided,");
    pw.println("                                          appType must be provided unless");
    pw.println("                                          it is the default yarn-service.");
    pw.println("                                          If ID is provided, the appType");
    pw.println("                                          will be looked up. Supports");
    pw.println("                                          -appTypes option to specify");
    pw.println("                                          which client implementation to");
    pw.println("                                          use.");
    pw.println(" -help                                    Displays help for all commands.");
    pw.println(" -initiate <File Name>                    Works with -upgrade option to");
    pw.println("                                          initiate the application");
    pw.println("                                          upgrade. It requires the");
    pw.println("                                          upgraded application");
    pw.println("                                          specification file.");
    pw.println(" -instances <Component Instances>         Works with -upgrade option to");
    pw.println("                                          trigger the upgrade of specified");
    pw.println("                                          component instances of the");
    pw.println("                                          application. Also works with");
    pw.println("                                          -decommission option to");
    pw.println("                                          decommission specified component");
    pw.println("                                          instances. Multiple instances");
    pw.println("                                          should be separated by commas.");
    pw.println(" -kill <Application ID>                   Kills the application. Set of");
    pw.println("                                          applications can be provided");
    pw.println("                                          separated with space");
    pw.println(" -launch <Application Name> <File Name>   Launches application from");
    pw.println("                                          specification file (saves");
    pw.println("                                          specification and starts");
    pw.println("                                          application). Options");
    pw.println("                                          -updateLifetime and -changeQueue");
    pw.println("                                          can be specified to alter the");
    pw.println("                                          values provided in the file.");
    pw.println("                                          Supports -appTypes option to");
    pw.println("                                          specify which client");
    pw.println("                                          implementation to use.");
    pw.println(" -list                                    List applications. Supports");
    pw.println("                                          optional use of -appTypes to");
    pw.println("                                          filter applications based on");
    pw.println("                                          application type, -appStates to");
    pw.println("                                          filter applications based on");
    pw.println("                                          application state and -appTags");
    pw.println("                                          to filter applications based on");
    pw.println("                                          application tag.");
    pw.println(" -movetoqueue <Application ID>            Moves the application to a");
    pw.println("                                          different queue. Deprecated");
    pw.println("                                          command. Use 'changeQueue'");
    pw.println("                                          instead.");
    pw.println(" -queue <Queue Name>                      Works with the movetoqueue");
    pw.println("                                          command to specify which queue");
    pw.println("                                          to move an application to.");
    pw.println(" -save <Application Name> <File Name>     Saves specification file for an");
    pw.println("                                          application. Options");
    pw.println("                                          -updateLifetime and -changeQueue");
    pw.println("                                          can be specified to alter the");
    pw.println("                                          values provided in the file.");
    pw.println("                                          Supports -appTypes option to");
    pw.println("                                          specify which client");
    pw.println("                                          implementation to use.");
    pw.println(" -start <Application Name>                Starts a previously saved");
    pw.println("                                          application. Supports -appTypes");
    pw.println("                                          option to specify which client");
    pw.println("                                          implementation to use.");
    pw.println(" -status <Application Name or ID>         Prints the status of the");
    pw.println("                                          application. If app ID is");
    pw.println("                                          provided, it prints the generic");
    pw.println("                                          YARN application status. If name");
    pw.println("                                          is provided, it prints the");
    pw.println("                                          application specific status");
    pw.println("                                          based on app's own");
    pw.println("                                          implementation, and -appTypes");
    pw.println("                                          option must be specified unless");
    pw.println("                                          it is the default yarn-service");
    pw.println("                                          type.");
    pw.println(" -stop <Application Name or ID>           Stops application gracefully");
    pw.println("                                          (may be started again later). If");
    pw.println("                                          name is provided, appType must");
    pw.println("                                          be provided unless it is the");
    pw.println("                                          default yarn-service. If ID is");
    pw.println("                                          provided, the appType will be");
    pw.println("                                          looked up. Supports -appTypes");
    pw.println("                                          option to specify which client");
    pw.println("                                          implementation to use.");
    pw.println(" -updateLifetime <Timeout>                update timeout of an application");
    pw.println("                                          from NOW. ApplicationId can be");
    pw.println("                                          passed using 'appId' option.");
    pw.println("                                          Timeout value is in seconds.");
    pw.println(" -updatePriority <Priority>               update priority of an");
    pw.println("                                          application. ApplicationId can");
    pw.println("                                          be passed using 'appId' option.");
    pw.println(" -upgrade <Application Name>              Upgrades an");
    pw.println("                                          application/long-running");
    pw.println("                                          service. It requires either");
    pw.println("                                          -initiate, -instances, or");
    pw.println("                                          -finalize options.");
    pw.close();
    String appsHelpStr = baos.toString("UTF-8");
    return appsHelpStr;
  }

  private String createApplicationAttemptCLIHelpMessage() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: applicationattempt");
    pw.println(" -fail <Application Attempt ID>     Fails application attempt.");
    pw.println(" -help                              Displays help for all commands.");
    pw.println(" -list <Application ID>             List application attempts for");
    pw.println("                                    application.");
    pw.println(" -status <Application Attempt ID>   Prints the status of the application");
    pw.println("                                    attempt.");
    pw.close();
    String appsHelpStr = baos.toString("UTF-8");
    return appsHelpStr;
  }

  private String createContainerCLIHelpMessage() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: container");
    pw.println(" -appTypes <Types>                Works with -list to specify the app type when application name is provided.");
    pw.println(" -components <arg>                Works with -list to filter instances based on input comma-separated list of component names.");
    pw.println(" -help                            Displays help for all commands.");
    pw.println(" -list <Application Name or Attempt ID>   List containers for application attempt  when application attempt ID is provided. When application name is provided, then it finds the instances of the application based on app's own implementation, and -appTypes option must be specified unless it is the default yarn-service type. With app name, it supports optional use of -version to filter instances based on app version, -components to filter instances based on component names, -states to filter instances based on instance state.");
    pw.println(" -signal <container ID [signal command]> Signal the container.");
    pw.println("The available signal commands are ");
    pw.println(java.util.Arrays.asList(SignalContainerCommand.values()));
    pw.println("                                 Default command is OUTPUT_THREAD_DUMP.");
    pw.println(" -states <arg>                    Works with -list to filter instances based on input comma-separated list of instance states.");
    pw.println(" -status <Container ID>           Prints the status of the container.");
    pw.println(" -version <arg>                   Works with -list to filter instances based on input application version. ");
    pw.close();
    try {
      return normalize(baos.toString("UTF-8"));
    } catch (UnsupportedEncodingException infeasible) {
      return infeasible.toString();
    }
  }

  private String createNodeCLIHelpMessage() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("usage: node");
    pw.println(" -all               Works with -list to list all nodes.");
    pw.println(" -help              Displays help for all commands.");
    pw.println(" -list              List all running nodes. Supports optional use of");
    pw.println("                    -states to filter nodes based on node state, all -all");
    pw.println("                    to list all nodes, -showDetails to display more");
    pw.println("                    details about each node.");
    pw.println(" -showDetails       Works with -list to show more details about each node.");
    pw.println(" -states <States>   Works with -list to filter nodes based on input");
    pw.println("                    comma-separated list of node states. The valid node");
    pw.println("                    state can be one of the following:");
    pw.println("                    NEW,RUNNING,UNHEALTHY,DECOMMISSIONED,LOST,REBOOTED,DEC");
    pw.println("                    OMMISSIONING,SHUTDOWN.");
    pw.println(" -status <NodeId>   Prints the status report of the node.");
    pw.close();
    String nodesHelpStr = baos.toString("UTF-8");
    return nodesHelpStr;
  }

  private static String normalize(String s) {
    return SPACES_PATTERN.matcher(s).replaceAll(" "); // single space
  }

  @Test
  public void testAppAttemptReportWhileContainerIsNotAssigned()
      throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationAttemptId attemptId =
        ApplicationAttemptId.newInstance(applicationId, 1);
    ApplicationAttemptReport attemptReport =
        ApplicationAttemptReport.newInstance(attemptId, "host", 124, "url",
            "oUrl", "diagnostics", YarnApplicationAttemptState.SCHEDULED, null,
            1000l, 2000l);
    when(client.getApplicationAttemptReport(any(ApplicationAttemptId.class)))
        .thenReturn(attemptReport);
    int result =
        cli.run(new String[] { "applicationattempt", "-status",
            attemptId.toString() });
    assertEquals(0, result);
    result =
        cli.run(new String[] { "applicationattempt", "-list",
            applicationId.toString() });
    assertEquals(0, result);
  }

  @Test(timeout = 60000)
  public void testUpdateApplicationTimeout() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 6);

    UpdateApplicationTimeoutsResponse response =
        mock(UpdateApplicationTimeoutsResponse.class);
    String formatISO8601 =
        Times.formatISO8601(System.currentTimeMillis() + 5 * 1000);
    when(response.getApplicationTimeouts()).thenReturn(Collections
        .singletonMap(ApplicationTimeoutType.LIFETIME, formatISO8601));

    when(client
        .updateApplicationTimeouts(any(UpdateApplicationTimeoutsRequest.class)))
            .thenReturn(response);

    int result = cli.run(new String[] { "application", "-appId",
        applicationId.toString(), "-updateLifetime", "10" });
    Assert.assertEquals(result, 0);
    verify(client)
        .updateApplicationTimeouts(any(UpdateApplicationTimeoutsRequest.class));
  }
}
