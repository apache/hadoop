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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.cli.Options;

public class TestYarnCLI {

  private YarnClient client = mock(YarnClient.class);
  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  @Before
  public void setup() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut = spy(new PrintStream(sysOutStream));
    sysErrStream = new ByteArrayOutputStream();
    sysErr = spy(new PrintStream(sysErrStream));
  }
  
  @Test
  public void testGetApplicationReport() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN");
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    int result = cli.run(new String[] { "-status", applicationId.toString() });
    assertEquals(0, result);
    verify(client).getApplicationReport(applicationId);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Application Report : ");
    pw.println("\tApplication-Id : application_1234_0005");
    pw.println("\tApplication-Name : appname");
    pw.println("\tApplication-Type : YARN");
    pw.println("\tUser : user");
    pw.println("\tQueue : queue");
    pw.println("\tStart-Time : 0");
    pw.println("\tFinish-Time : 0");
    pw.println("\tProgress : 53.79%");
    pw.println("\tState : FINISHED");
    pw.println("\tFinal-State : SUCCEEDED");
    pw.println("\tTracking-URL : N/A");
    pw.println("\tRPC Port : 124");
    pw.println("\tAM Host : host");
    pw.println("\tDiagnostics : diagnostics");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).println(isA(String.class));
  }

  @Test
  public void testGetApplicationReportException() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    when(client.getApplicationReport(any(ApplicationId.class))).thenThrow(
        new ApplicationNotFoundException("Application with id '"
            + applicationId + "' doesn't exist in RM."));
    try {
      cli.run(new String[] { "-status", applicationId.toString() });
      Assert.fail();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof ApplicationNotFoundException);
      Assert.assertEquals("Application with id '" + applicationId
          + "' doesn't exist in RM.", ex.getMessage());
    }
  }

  @Test
  public void testGetApplications() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    ApplicationReport newApplicationReport = ApplicationReport.newInstance(
        applicationId, ApplicationAttemptId.newInstance(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.53789f, "YARN");
    List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
    applicationReports.add(newApplicationReport);

    ApplicationId applicationId2 = ApplicationId.newInstance(1234, 6);
    ApplicationReport newApplicationReport2 = ApplicationReport.newInstance(
        applicationId2, ApplicationAttemptId.newInstance(applicationId2, 2),
        "user2", "queue2", "appname2", "host2", 125, null,
        YarnApplicationState.FINISHED, "diagnostics2", "url2", 2, 2,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.63789f, "NON-YARN");
    applicationReports.add(newApplicationReport2);

    ApplicationId applicationId3 = ApplicationId.newInstance(1234, 7);
    ApplicationReport newApplicationReport3 = ApplicationReport.newInstance(
        applicationId3, ApplicationAttemptId.newInstance(applicationId3, 3),
        "user3", "queue3", "appname3", "host3", 126, null,
        YarnApplicationState.FINISHED, "diagnostics3", "url3", 3, 3,
        FinalApplicationStatus.SUCCEEDED, null, "N/A", 0.73789f, "MAPREDUCE");
    applicationReports.add(newApplicationReport3);

    Set<String> appType1 = new HashSet<String>();
    appType1.add("YARN");

    when(client.getApplications(appType1)).thenReturn(
        getApplicationReports(applicationReports, appType1));
    int result = cli.run(new String[] { "-list", "-appTypes", "YARN" });
    assertEquals(0, result);
    verify(client).getApplications(appType1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Total Applications:1");
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.close();
    String appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    Set<String> appType2 = new HashSet<String>();
    appType2.add("YARN");
    appType2.add("FOO-YARN");
    when(client.getApplications(appType2)).thenReturn(
        getApplicationReports(applicationReports, appType2));
    cli.run(new String[] { "-list", "-appTypes", "YARN , ,, ,FOO-YARN",
        ",,,,, YARN,," });
    assertEquals(0, result);
    verify(client).getApplications(appType2);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Applications:1");
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(2)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    Set<String> appType3 = new HashSet<String>();
    appType3.add("YARN");
    appType3.add("NON-YARN");
    when(client.getApplications(appType3)).thenReturn(
        getApplicationReports(applicationReports, appType3));

    result = cli.run(new String[] { "-list", "-appTypes", "YARN,NON-YARN" });
    assertEquals(0, result);
    verify(client).getApplications(appType3);
    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Applications:2");
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(3)).write(any(byte[].class), anyInt(), anyInt());

    sysOutStream.reset();
    Set<String> appType4 = new HashSet<String>();
    when(client.getApplications(appType4)).thenReturn(
        getApplicationReports(applicationReports, appType4));
    result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getApplications(appType4);

    baos = new ByteArrayOutputStream();
    pw = new PrintWriter(baos);
    pw.println("Total Applications:3");
    pw.print("                Application-Id\t    Application-Name");
    pw.print("\t    Application-Type");
    pw.print("\t      User\t     Queue\t             State\t       ");
    pw.print("Final-State\t       Progress");
    pw.println("\t                       Tracking-URL");
    pw.print("         application_1234_0005\t             ");
    pw.print("appname\t                YARN\t      user\t     ");
    pw.print("queue\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         53.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0006\t            ");
    pw.print("appname2\t            NON-YARN\t     user2\t    ");
    pw.print("queue2\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         63.79%");
    pw.println("\t                                N/A");
    pw.print("         application_1234_0007\t            ");
    pw.print("appname3\t           MAPREDUCE\t     user3\t    ");
    pw.print("queue3\t          FINISHED\t         ");
    pw.print("SUCCEEDED\t         73.79%");
    pw.println("\t                                N/A");
    pw.close();
    appsReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appsReportStr, sysOutStream.toString());
    verify(sysOut, times(4)).write(any(byte[].class), anyInt(), anyInt());
  }

  private List<ApplicationReport> getApplicationReports(
      List<ApplicationReport> applicationReports,
      Set<String> appTypes) {

    List<ApplicationReport> appReports = new ArrayList<ApplicationReport>();
    boolean bypassFilter = appTypes.isEmpty();

    for (ApplicationReport appReport : applicationReports) {
      if (!(bypassFilter || appTypes.contains(
          appReport.getApplicationType()))) {
        continue;
      }
      appReports.add(appReport);
    }
    return appReports;
  }

  @Test (timeout = 10000)
  public void testHelpCommand() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationCLI spyCli = spy(cli);
    int result = spyCli.run(new String[] { "-help" });
    Assert.assertTrue(result == 0);
    verify(spyCli).printUsage(any(Options.class));

    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    result =
        cli.run(new String[] { "-kill", applicationId.toString(), "args" });
    verify(spyCli).printUsage(any(Options.class));

    NodeId nodeId = NodeId.newInstance("host0", 0);
    result = cli.run(new String[] { "-status", nodeId.toString(), "args" });
    verify(spyCli).printUsage(any(Options.class));
  }

  @Test
  public void testKillApplication() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = ApplicationId.newInstance(1234, 5);
    int result = cli.run(new String[] { "-kill", applicationId.toString() });
    assertEquals(0, result);
    verify(client).killApplication(any(ApplicationId.class));
    verify(sysOut).println("Killing application application_1234_0005");
  }

  @Test
  public void testListClusterNodes() throws Exception {
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports(NodeState.RUNNING)).thenReturn(
        getNodeReports(3));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    int result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getNodeReports(NodeState.RUNNING);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Total Nodes:3");
    pw.print("         Node-Id\tNode-State\tNode-Http-Address\t");
    pw.println("Running-Containers");
    pw.print("         host0:0\t   RUNNING\t       host1:8888");
    pw.println("\t                 0");
    pw.print("         host1:0\t   RUNNING\t       host1:8888");
    pw.println("\t                 0");
    pw.print("         host2:0\t   RUNNING\t       host1:8888");
    pw.println("\t                 0");
    pw.close();
    String nodesReportStr = baos.toString("UTF-8");
    Assert.assertEquals(nodesReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void testNodeStatus() throws Exception {
    NodeId nodeId = NodeId.newInstance("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(3));
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
    pw.println("\tMemory-Used : 0M");
    pw.println("\tMemory-Capacity : 0");
    pw.close();
    String nodeStatusStr = baos.toString("UTF-8");
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatusStr);
  }

  @Test
  public void testAbsentNodeStatus() throws Exception {
    NodeId nodeId = NodeId.newInstance("Absenthost0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(0));
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

  private void verifyUsageInfo(YarnCLI cli) throws Exception {
    cli.setSysErrPrintStream(sysErr);
    cli.run(new String[0]);
    verify(sysErr).println("Invalid Command Usage : ");
  }

  private List<NodeReport> getNodeReports(int noOfNodes) {
    List<NodeReport> nodeReports = new ArrayList<NodeReport>();

    for (int i = 0; i < noOfNodes; i++) {
      NodeReport nodeReport = NodeReport.newInstance(NodeId
        .newInstance("host" + i, 0), NodeState.RUNNING, "host" + 1 + ":8888",
          "rack1", Records.newRecord(Resource.class), Records
              .newRecord(Resource.class), 0, "", 0);
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

}
