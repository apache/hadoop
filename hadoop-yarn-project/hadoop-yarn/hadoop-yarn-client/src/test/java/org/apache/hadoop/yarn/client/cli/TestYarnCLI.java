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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

public class TestYarnCLI {

 private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

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
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    ApplicationReport newApplicationReport = BuilderUtils.newApplicationReport(
        applicationId, 
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A");
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    int result = cli.run(new String[] { "-status", applicationId.toString() });
    assertEquals(0, result);
    verify(client).getApplicationReport(applicationId);
    String appReportStr = "Application Report : \n\t"
        + "Application-Id : application_1234_0005\n\t"
        + "Application-Name : appname\n\tUser : user\n\t"
        + "Queue : queue\n\tStart-Time : 0\n\tFinish-Time : 0\n\t"
        + "State : FINISHED\n\tFinal-State : SUCCEEDED\n\t"
        + "Tracking-URL : N/A\n\tDiagnostics : diagnostics\n";
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).println(isA(String.class));
  }

  @Test
  public void testGetAllApplications() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    ApplicationReport newApplicationReport = BuilderUtils.newApplicationReport(
        applicationId, 
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A");
    List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
    applicationReports.add(newApplicationReport);
    when(client.getApplicationList()).thenReturn(applicationReports);
    int result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getApplicationList();

    StringBuffer appsReportStrBuf = new StringBuffer();
    appsReportStrBuf.append("Total Applications:1\n");
    appsReportStrBuf
        .append("                Application-Id\t    Application-Name"
            + "\t      User\t     Queue\t             State\t       "
            + "Final-State\t                       Tracking-URL\n");
    appsReportStrBuf.append("         application_1234_0005\t             "
        + "appname\t      user\t     queue\t          FINISHED\t         "
        + "SUCCEEDED\t                                N/A\n");
    Assert.assertEquals(appsReportStrBuf.toString(), sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void testKillApplication() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    int result = cli.run(new String[] { "-kill", applicationId.toString() });
    assertEquals(0, result);
    verify(client).killApplication(any(ApplicationId.class));
    verify(sysOut).println("Killing application application_1234_0005");
  }

  @Test
  public void testListClusterNodes() throws Exception {
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(3));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    int result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getNodeReports();
    StringBuffer nodesReportStr = new StringBuffer();
    nodesReportStr.append("Total Nodes:3");
    nodesReportStr
        .append("\n         Node-Id\tNode-Http-Address\t"
            + "Health-Status(isNodeHealthy)\tRunning-Containers");
    nodesReportStr.append("\n         host0:0\t       host1:8888"
        + "\t                     false\t                 0");
    nodesReportStr.append("\n         host1:0\t       host1:8888"
        + "\t                     false\t                 0");
    nodesReportStr.append("\n         host2:0\t       host1:8888"
        + "\t                     false\t                 0\n");
    Assert.assertEquals(nodesReportStr.toString(), sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void testNodeStatus() throws Exception {
    NodeId nodeId = BuilderUtils.newNodeId("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(3));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    String nodeStatusStr = "Node Report : \n\tNode-Id : host0:0\n\t"
        + "Rack : rack1\n\t"
        + "Node-Http-Address : host1:8888\n\tHealth-Status(isNodeHealthy) "
        + ": false\n\tLast-Last-Health-Update : 0\n\tHealth-Report : null"
        + "\n\tContainers : 0\n\tMemory-Used : 0M\n\tMemory-Capacity : 0";
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatusStr);
  }

  @Test
  public void testAbsentNodeStatus() throws Exception {
    NodeId nodeId = BuilderUtils.newNodeId("Absenthost0", 0);
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
      NodeReport nodeReport = newNodeReport(BuilderUtils
      .newNodeId("host" + i, 0), "host" + 1 + ":8888",
      "rack1", Records.newRecord(Resource.class), Records
      .newRecord(Resource.class), 0, Records
      .newRecord(NodeHealthStatus.class));
      nodeReports.add(nodeReport);
    }
    return nodeReports;
  }

  private static NodeReport newNodeReport(NodeId nodeId, 
      String httpAddress, String rackName, Resource used, Resource capability,
      int numContainers, NodeHealthStatus nodeHealthStatus) {
    NodeReport nodeReport = recordFactory.newRecordInstance(NodeReport.class);
    nodeReport.setNodeId(nodeId);
    nodeReport.setHttpAddress(httpAddress);
    nodeReport.setRackName(rackName);
    nodeReport.setUsed(used);
    nodeReport.setCapability(capability);
    nodeReport.setNumContainers(numContainers);
    nodeReport.setNodeHealthStatus(nodeHealthStatus);
    return nodeReport;
  }     


  private ApplicationCLI createAndGetAppCLI() {
    ApplicationCLI cli = new ApplicationCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    return cli;
  }

}
