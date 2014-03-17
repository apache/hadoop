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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.junit.Before;
import org.junit.Test;

public class TestLogsCLI {
  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  
  ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  @Before
  public void setUp() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut =  new PrintStream(sysOutStream);
    System.setOut(sysOut);
    
    sysErrStream = new ByteArrayOutputStream();
    sysErr = new PrintStream(sysErrStream);
    System.setErr(sysErr);
  }

  @Test(timeout = 5000l)
  public void testFailResultCodes() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.setClass("fs.file.impl", LocalFileSystem.class, FileSystem.class);
    LogCLIHelpers cliHelper = new LogCLIHelpers();
    cliHelper.setConf(conf);
    YarnClient mockYarnClient = createMockYarnClient(YarnApplicationState.FINISHED);
    LogsCLI dumper = new LogsCLIForTest(mockYarnClient);
    dumper.setConf(conf);
    
    // verify dumping a non-existent application's logs returns a failure code
    int exitCode = dumper.run( new String[] {
        "-applicationId", "application_0_0" } );
    assertTrue("Should return an error code", exitCode != 0);
    
    // verify dumping a non-existent container log is a failure code 
    exitCode = cliHelper.dumpAContainersLogs("application_0_0", "container_0_0",
        "nonexistentnode:1234", "nobody");
    assertTrue("Should return an error code", exitCode != 0);
  }

  @Test(timeout = 5000l)
  public void testInvalidApplicationId() throws Exception {
    Configuration conf = new YarnConfiguration();
    YarnClient mockYarnClient = createMockYarnClient(YarnApplicationState.FINISHED);
    LogsCLI cli = new LogsCLIForTest(mockYarnClient);
    cli.setConf(conf);
    
    int exitCode = cli.run( new String[] { "-applicationId", "not_an_app_id"});
    assertTrue(exitCode == -1);
    assertTrue(sysErrStream.toString().startsWith("Invalid ApplicationId specified"));
  }

  @Test(timeout = 5000l)
  public void testUnknownApplicationId() throws Exception {
    Configuration conf = new YarnConfiguration();
    YarnClient mockYarnClient = createMockYarnClientUnknownApp();
    LogsCLI cli = new LogsCLIForTest(mockYarnClient);
    cli.setConf(conf);

    int exitCode = cli.run(new String[] { "-applicationId",
        ApplicationId.newInstance(1, 1).toString() });

    // Error since no logs present for the app.
    assertTrue(exitCode != 0);
    assertTrue(sysErrStream.toString().startsWith(
        "Unable to get ApplicationState"));
  }

  @Test(timeout = 5000l)
  public void testHelpMessage() throws Exception {
    Configuration conf = new YarnConfiguration();
    YarnClient mockYarnClient = createMockYarnClient(YarnApplicationState.FINISHED);
    LogsCLI dumper = new LogsCLIForTest(mockYarnClient);
    dumper.setConf(conf);

    int exitCode = dumper.run(new String[]{});
    assertTrue(exitCode == -1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    pw.println("Retrieve logs for completed YARN applications.");
    pw.println("usage: yarn logs -applicationId <application ID> [OPTIONS]");
    pw.println();
    pw.println("general options are:");
    pw.println(" -appOwner <Application Owner>   AppOwner (assumed to be current user if");
    pw.println("                                 not specified)");
    pw.println(" -containerId <Container ID>     ContainerId (must be specified if node");
    pw.println("                                 address is specified)");
    pw.println(" -nodeAddress <Node Address>     NodeAddress in the format nodename:port");
    pw.println("                                 (must be specified if container id is");
    pw.println("                                 specified)");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
  }
  
  private YarnClient createMockYarnClient(YarnApplicationState appState)
      throws YarnException, IOException {
    YarnClient mockClient = mock(YarnClient.class);
    ApplicationReport mockAppReport = mock(ApplicationReport.class);
    doReturn(appState).when(mockAppReport).getYarnApplicationState();
    doReturn(mockAppReport).when(mockClient).getApplicationReport(
        any(ApplicationId.class));
    return mockClient;
  }

  private YarnClient createMockYarnClientUnknownApp() throws YarnException,
      IOException {
    YarnClient mockClient = mock(YarnClient.class);
    doThrow(new YarnException("Unknown AppId")).when(mockClient)
        .getApplicationReport(any(ApplicationId.class));
    return mockClient;
  }

  private static class LogsCLIForTest extends LogsCLI {
    
    private YarnClient yarnClient;
    
    public LogsCLIForTest(YarnClient yarnClient) {
      super();
      this.yarnClient = yarnClient;
    }

    protected YarnClient createYarnClient() {
      return yarnClient;
    }
  }
}
