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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
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
    pw.println(" -help                           Displays help for all commands.");
    pw.println(" -nodeAddress <Node Address>     NodeAddress in the format nodename:port");
    pw.println("                                 (must be specified if container id is");
    pw.println("                                 specified)");
    pw.close();
    String appReportStr = baos.toString("UTF-8");
    Assert.assertEquals(appReportStr, sysOutStream.toString());
  }
  
  @Test (timeout = 15000)
  public void testFetchApplictionLogs() throws Exception {
    String remoteLogRootDir = "target/logs/";
    Configuration configuration = new Configuration();
    configuration.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    configuration
      .set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogRootDir);
    configuration.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
    configuration.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
    FileSystem fs = FileSystem.get(configuration);

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ApplicationId appId = ApplicationIdPBImpl.newInstance(0, 1);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptIdPBImpl.newInstance(appId, 1);
    ContainerId containerId0 = ContainerIdPBImpl.newContainerId(appAttemptId, 0);
    ContainerId containerId1 = ContainerIdPBImpl.newContainerId(appAttemptId, 1);
    ContainerId containerId2 = ContainerIdPBImpl.newContainerId(appAttemptId, 2);
    NodeId nodeId = NodeId.newInstance("localhost", 1234);

    // create local logs
    String rootLogDir = "target/LocalLogs";
    Path rootLogDirPath = new Path(rootLogDir);
    if (fs.exists(rootLogDirPath)) {
      fs.delete(rootLogDirPath, true);
    }
    assertTrue(fs.mkdirs(rootLogDirPath));

    Path appLogsDir = new Path(rootLogDirPath, appId.toString());
    if (fs.exists(appLogsDir)) {
      fs.delete(appLogsDir, true);
    }
    assertTrue(fs.mkdirs(appLogsDir));
    List<String> rootLogDirs = Arrays.asList(rootLogDir);

    // create container logs in localLogDir
    createContainerLogInLocalDir(appLogsDir, containerId1, fs);
    createContainerLogInLocalDir(appLogsDir, containerId2, fs);

    Path path =
        new Path(remoteLogRootDir + ugi.getShortUserName()
            + "/logs/application_0_0001");
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    assertTrue(fs.mkdirs(path));

    // upload container logs into remote directory
    // the first two logs is empty. When we try to read first two logs,
    // we will meet EOF exception, but it will not impact other logs.
    // Other logs should be read successfully.
    uploadEmptyContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId0, path, fs);
    uploadEmptyContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId1, path, fs);
    uploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId1, path, fs);
    uploadContainerLogIntoRemoteDir(ugi, configuration, rootLogDirs, nodeId,
      containerId2, path, fs);

    YarnClient mockYarnClient =
        createMockYarnClient(YarnApplicationState.FINISHED);
    LogsCLI cli = new LogsCLIForTest(mockYarnClient);
    cli.setConf(configuration);

    int exitCode = cli.run(new String[] { "-applicationId", appId.toString() });
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000001!"));
    assertTrue(sysOutStream.toString().contains(
      "Hello container_0_0001_01_000002!"));
    sysOutStream.reset();

    // uploaded two logs for container1. The first log is empty.
    // The second one is not empty.
    // We can still successfully read logs for container1.
    exitCode =
        cli.run(new String[] { "-applicationId", appId.toString(),
            "-nodeAddress", nodeId.toString(), "-containerId",
            containerId1.toString() });
    assertTrue(exitCode == 0);
    assertTrue(sysOutStream.toString().contains(
        "Hello container_0_0001_01_000001!"));
    assertTrue(sysOutStream.toString().contains("Log Upload Time"));
    assertTrue(!sysOutStream.toString().contains(
      "Logs for container " + containerId1.toString()
          + " are not present in this log-file."));
    sysOutStream.reset();

    // Uploaded the empty log for container0.
    // We should see the message showing the log for container0
    // are not present.
    exitCode =
        cli.run(new String[] { "-applicationId", appId.toString(),
            "-nodeAddress", nodeId.toString(), "-containerId",
            containerId0.toString() });
    assertTrue(exitCode == -1);
    assertTrue(sysOutStream.toString().contains(
      "Logs for container " + containerId0.toString()
          + " are not present in this log-file."));

    fs.delete(new Path(remoteLogRootDir), true);
    fs.delete(new Path(rootLogDir), true);
  }

  private static void createContainerLogInLocalDir(Path appLogsDir,
      ContainerId containerId, FileSystem fs) throws Exception {
    Path containerLogsDir = new Path(appLogsDir, containerId.toString());
    if (fs.exists(containerLogsDir)) {
      fs.delete(containerLogsDir, true);
    }
    assertTrue(fs.mkdirs(containerLogsDir));
    Writer writer =
        new FileWriter(new File(containerLogsDir.toString(), "sysout"));
    writer.write("Hello " + containerId + "!");
    writer.close();
  }

  private static void uploadContainerLogIntoRemoteDir(UserGroupInformation ugi,
      Configuration configuration, List<String> rootLogDirs, NodeId nodeId,
      ContainerId containerId, Path appDir, FileSystem fs) throws Exception {
    Path path =
        new Path(appDir, LogAggregationUtils.getNodeString(nodeId)
            + System.currentTimeMillis());
    AggregatedLogFormat.LogWriter writer =
        new AggregatedLogFormat.LogWriter(configuration, path, ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    Map<ApplicationAccessType, String> appAcls =
        new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
    writer.writeApplicationACLs(appAcls);
    writer.append(new AggregatedLogFormat.LogKey(containerId),
      new AggregatedLogFormat.LogValue(rootLogDirs, containerId,
        UserGroupInformation.getCurrentUser().getShortUserName()));
    writer.close();
  }

  private static void uploadEmptyContainerLogIntoRemoteDir(UserGroupInformation ugi,
      Configuration configuration, List<String> rootLogDirs, NodeId nodeId,
      ContainerId containerId, Path appDir, FileSystem fs) throws Exception {
    Path path =
        new Path(appDir, LogAggregationUtils.getNodeString(nodeId)
            + System.currentTimeMillis());
    AggregatedLogFormat.LogWriter writer =
        new AggregatedLogFormat.LogWriter(configuration, path, ugi);
    writer.writeApplicationOwner(ugi.getUserName());

    Map<ApplicationAccessType, String> appAcls =
        new HashMap<ApplicationAccessType, String>();
    appAcls.put(ApplicationAccessType.VIEW_APP, ugi.getUserName());
    writer.writeApplicationACLs(appAcls);
    DataOutputStream out = writer.getWriter().prepareAppendKey(-1);
    new AggregatedLogFormat.LogKey(containerId).write(out);
    out.close();
    out = writer.getWriter().prepareAppendValue(-1);
    new AggregatedLogFormat.LogValue(rootLogDirs, containerId,
      UserGroupInformation.getCurrentUser().getShortUserName()).write(out,
      new HashSet<File>());
    out.close();
    writer.close();
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
