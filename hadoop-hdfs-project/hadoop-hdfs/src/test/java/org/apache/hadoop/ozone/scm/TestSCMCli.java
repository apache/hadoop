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
package org.apache.hadoop.ozone.scm;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.cli.ResultCode;
import org.apache.hadoop.ozone.scm.cli.SCMCLI;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.client.ContainerOperationClient;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.ozone.scm.cli.ResultCode.EXECUTION_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
/**
 * This class tests the CLI of SCM.
 */
public class TestSCMCli {
  private static SCMCLI cli;

  private static MiniOzoneClassicCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static StorageContainerManager scm;
  private static ScmClient containerOperationClient;

  private static ByteArrayOutputStream outContent;
  private static PrintStream outStream;
  private static ByteArrayOutputStream errContent;
  private static PrintStream errStream;
  private static XceiverClientManager xceiverClientManager;

  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(3)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    xceiverClientManager = new XceiverClientManager(conf);
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    containerOperationClient = new ContainerOperationClient(
        storageContainerLocationClient, new XceiverClientManager(conf));
    outContent = new ByteArrayOutputStream();
    outStream = new PrintStream(outContent);
    errContent = new ByteArrayOutputStream();
    errStream = new PrintStream(errContent);
    cli = new SCMCLI(containerOperationClient, outStream, errStream);
    scm = cluster.getStorageContainerManager();
  }

  private int runCommandAndGetOutput(String[] cmd,
      ByteArrayOutputStream out,
      ByteArrayOutputStream err) throws Exception {
    PrintStream cmdOutStream = System.out;
    PrintStream cmdErrStream = System.err;
    if(out != null) {
      cmdOutStream = new PrintStream(out);
    }
    if (err != null) {
      cmdErrStream = new PrintStream(err);
    }
    ScmClient client = new ContainerOperationClient(
        storageContainerLocationClient, new XceiverClientManager(conf));
    SCMCLI scmCLI = new SCMCLI(client, cmdOutStream, cmdErrStream);
    return scmCLI.run(cmd);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testCreateContainer() throws Exception {
    String containerName =  "containerTestCreate";
    try {
      scm.getContainer(containerName);
      fail("should not be able to get the container");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains(
          "Specified key does not exist. key : " + containerName));
    }
    String[] args = {"-container", "-create", "-c", containerName};
    assertEquals(ResultCode.SUCCESS, cli.run(args));
    Pipeline container = scm.getContainer(containerName);
    assertNotNull(container);
    assertEquals(containerName, container.getContainerName());
  }

  private boolean containerExist(String containerName) {
    try {
      Pipeline scmPipeline = scm.getContainer(containerName);
      return scmPipeline != null
          && containerName.equals(scmPipeline.getContainerName());
    } catch (IOException e) {
      return false;
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    String containerName;
    ContainerData containerData;
    Pipeline pipeline;
    String[] delCmd;
    ByteArrayOutputStream testErr;
    int exitCode;

    // ****************************************
    // 1. Test to delete a non-empty container.
    // ****************************************
    // Create an non-empty container
    containerName = "non-empty-container";
    pipeline = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, containerName);

    ContainerData cdata = ContainerData
        .getFromProtBuf(containerOperationClient.readContainer(pipeline), conf);
    KeyUtils.getDB(cdata, conf).put(containerName.getBytes(),
        "someKey".getBytes());
    Assert.assertTrue(containerExist(containerName));

    // Gracefully delete a container should fail because it is open.
    delCmd = new String[] {"-container", "-delete", "-c", containerName};
    testErr = new ByteArrayOutputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(testErr.toString()
        .contains("Deleting an open container is not allowed."));
    Assert.assertTrue(containerExist(containerName));

    // Close the container
    containerOperationClient.closeContainer(pipeline);

    // Gracefully delete a container should fail because it is not empty.
    testErr = new ByteArrayOutputStream();
    int exitCode2 = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode2);
    assertTrue(testErr.toString()
        .contains("Container cannot be deleted because it is not empty."));
    Assert.assertTrue(containerExist(containerName));

    // Try force delete again.
    delCmd = new String[] {"-container", "-delete", "-c", containerName, "-f"};
    exitCode = runCommandAndGetOutput(delCmd, out, null);
    assertEquals("Expected success, found:", ResultCode.SUCCESS, exitCode);
    assertFalse(containerExist(containerName));

    // ****************************************
    // 2. Test to delete an empty container.
    // ****************************************
    // Create an empty container
    containerName = "empty-container";
    pipeline = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, containerName);
    containerOperationClient.closeContainer(pipeline);
    Assert.assertTrue(containerExist(containerName));

    // Successfully delete an empty container.
    delCmd = new String[] {"-container", "-delete", "-c", containerName};
    exitCode = runCommandAndGetOutput(delCmd, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    assertFalse(containerExist(containerName));

    // After the container is deleted,
    // a same name container can now be recreated.
    containerOperationClient.createContainer(xceiverClientManager.getType(),
        OzoneProtos.ReplicationFactor.ONE, containerName);
    Assert.assertTrue(containerExist(containerName));

    // ****************************************
    // 3. Test to delete a non-exist container.
    // ****************************************
    containerName = "non-exist-container";
    delCmd = new String[] {"-container", "-delete", "-c", containerName};
    testErr = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(testErr.toString()
        .contains("Specified key does not exist."));
  }

  @Test
  public void testInfoContainer() throws Exception {
    // The cluster has one Datanode server.
    DatanodeID datanodeID = cluster.getDataNodes().get(0).getDatanodeId();
    String formatStr =
        "Container Name: %s\n" +
        "Container State: %s\n" +
        "Container DB Path: %s\n" +
        "Container Path: %s\n" +
        "Container Metadata: {%s}\n" +
        "LeaderID: %s\n" +
        "Datanodes: [%s]\n";

    String formatStrWithHash =
        "Container Name: %s\n" +
        "Container State: %s\n" +
        "Container Hash: %s\n" +
        "Container DB Path: %s\n" +
        "Container Path: %s\n" +
        "Container Metadata: {%s}\n" +
        "LeaderID: %s\n" +
        "Datanodes: [%s]\n";

    // Test a non-exist container
    String cname = "nonExistContainer";
    String[] info = {"-container", "-info", cname};
    int exitCode = runCommandAndGetOutput(info, null, null);
    assertEquals("Expected Execution Error, Did not find that.",
        EXECUTION_ERROR, exitCode);

    // Create an empty container.
    cname = "ContainerTestInfo1";
    Pipeline pipeline = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, cname);
    ContainerData data = ContainerData
        .getFromProtBuf(containerOperationClient.readContainer(pipeline), conf);

    info = new String[]{"-container", "-info", "-c", cname};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals("Expected Success, did not find it.", ResultCode.SUCCESS,
            exitCode);

    String openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    String expected = String.format(formatStr, cname, openStatus,
        data.getDBPath(), data.getContainerPath(), "",
        datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());

    out.reset();

    // Create an non-empty container
    cname = "ContainerTestInfo2";
    pipeline = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, cname);
    data = ContainerData
        .getFromProtBuf(containerOperationClient.readContainer(pipeline), conf);
    KeyUtils.getDB(data, conf).put(cname.getBytes(), "someKey".getBytes());

    info = new String[]{"-container", "-info", "-c", cname};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String.format(formatStr, cname, openStatus,
        data.getDBPath(), data.getContainerPath(), "",
        datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());

    out.reset();


    // Close last container and test info again.
    containerOperationClient.closeContainer(pipeline);

    info = new String[] {"-container", "-info", "-c", cname};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    data = ContainerData
        .getFromProtBuf(containerOperationClient.readContainer(pipeline), conf);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String.format(formatStrWithHash, cname, openStatus,
        data.getHash(), data.getDBPath(), data.getContainerPath(),
        "", datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());
  }

  @Test
  public void testNonExistCommand() throws Exception {
    PrintStream init = System.out;
    ByteArrayOutputStream testContent = new ByteArrayOutputStream();
    PrintStream testPrintOut = new PrintStream(testContent);
    System.setOut(testPrintOut);
    String[] args = {"-nothingUseful"};
    assertEquals(ResultCode.UNRECOGNIZED_CMD, cli.run(args));
    assertTrue(errContent.toString()
        .contains("Unrecognized options:[-nothingUseful]"));
    String expectedOut =
        "usage: hdfs scmcli <commands> [<options>]\n" +
        "where <commands> can be one of the following\n" +
        " -container   Container related options\n";
    assertEquals(expectedOut, testContent.toString());
    System.setOut(init);
  }

  @Test
  public void testListContainerCommand() throws Exception {
    // Create 20 containers for testing.
    String prefix = "ContainerForTesting";
    for (int index = 0; index < 20; index++) {
      String containerName = String.format("%s%02d", prefix, index);
      containerOperationClient.createContainer(xceiverClientManager.getType(),
          OzoneProtos.ReplicationFactor.ONE, containerName);
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();

    // Test without -start, -prefix and -count
    String[] args = new String[] {"-container", "-list"};
    int exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(err.toString()
        .contains("Expecting container count"));

    out.reset();
    err.reset();

    // Test with -start and -count, the value of -count is negative.
    args = new String[] {"-container", "-list",
        "-start", prefix + 0, "-count", "-1"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(err.toString()
        .contains("-count should not be negative"));

    out.reset();
    err.reset();

    String startName = String.format("%s%02d", prefix, 0);

    // Test with -start and -count.
    args = new String[] {"-container", "-list", "-start",
        startName, "-count", "10"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    for (int index = 0; index < 10; index++) {
      String containerName = String.format("%s%02d", prefix, index);
      assertTrue(out.toString().contains(containerName));
    }

    out.reset();
    err.reset();

    // Test with -start, -prefix and -count.
    startName = String.format("%s%02d", prefix, 0);
    String prefixName = String.format("%s0", prefix);
    args = new String[] {"-container", "-list", "-start",
        startName, "-prefix", prefixName, "-count", "20"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    for (int index = 0; index < 10; index++) {
      String containerName = String.format("%s%02d", prefix, index);
      assertTrue(out.toString().contains(containerName));
    }

    out.reset();
    err.reset();

    startName = String.format("%s%02d", prefix, 0);
    prefixName = String.format("%s0", prefix);
    args = new String[] {"-container", "-list", "-start",
        startName, "-prefix", prefixName, "-count", "4"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    for (int index = 0; index < 4; index++) {
      String containerName = String.format("%s%02d", prefix, index);
      assertTrue(out.toString().contains(containerName));
    }

    out.reset();
    err.reset();

    prefixName = String.format("%s0", prefix);
    args = new String[] {"-container", "-list",
        "-prefix", prefixName, "-count", "6"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    for (int index = 0; index < 6; index++) {
      String containerName = String.format("%s%02d", prefix, index);
      assertTrue(out.toString().contains(containerName));
    }

    out.reset();
    err.reset();

    // Test with -start and -prefix, while -count doesn't exist.
    prefixName = String.format("%s%02d", prefix, 20);
    args = new String[] {"-container", "-list", "-start",
        startName, "-prefix", prefixName, "-count", "10"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    assertTrue(out.toString().isEmpty());
  }

  @Test
  public void testCloseContainer() throws Exception {
    String containerName =  "containerTestClose";
    String[] args = {"-container", "-create", "-c", containerName};
    assertEquals(ResultCode.SUCCESS, cli.run(args));
    Pipeline container = scm.getContainer(containerName);
    assertNotNull(container);
    assertEquals(containerName, container.getContainerName());

    ContainerInfo containerInfo = scm.getContainerInfo(containerName);
    assertEquals(OPEN, containerInfo.getState());

    String[] args1 = {"-container", "-close", "-c", containerName};
    assertEquals(ResultCode.SUCCESS, cli.run(args1));

    containerInfo = scm.getContainerInfo(containerName);
    assertEquals(CLOSED, containerInfo.getState());

    // closing this container again will trigger an error.
    assertEquals(EXECUTION_ERROR, cli.run(args1));
  }

  @Test
  public void testHelp() throws Exception {
    // TODO : this test assertion may break for every new help entry added
    // may want to disable this test some time later. For now, mainly to show
    // case the format of help output.
    PrintStream init = System.out;
    ByteArrayOutputStream testContent = new ByteArrayOutputStream();
    PrintStream testPrintOut = new PrintStream(testContent);
    System.setOut(testPrintOut);
    String[] args = {"-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args));
    String expected =
        "usage: hdfs scmcli <commands> [<options>]\n" +
        "where <commands> can be one of the following\n" +
        " -container   Container related options\n";
    assertEquals(expected, testContent.toString());
    testContent.reset();

    String[] args1 = {"-container", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args1));
    String expected1 =
        "usage: hdfs scm -container <commands> <options>\n" +
        "where <commands> can be one of the following\n" +
        " -close    Close container\n" +
        " -create   Create container\n" +
        " -delete   Delete container\n" +
        " -info     Info container\n" +
        " -list     List container\n";

    assertEquals(expected1, testContent.toString());
    testContent.reset();

    String[] args2 = {"-container", "-create", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args2));
    String expected2 =
        "usage: hdfs scm -container -create <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container name\n";
    assertEquals(expected2, testContent.toString());
    testContent.reset();

    String[] args3 = {"-container", "-delete", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args3));
    String expected3 =
        "usage: hdfs scm -container -delete <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container name\n" +
        " -f         forcibly delete a container\n";
    assertEquals(expected3, testContent.toString());
    testContent.reset();

    String[] args4 = {"-container", "-info", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args4));
    String expected4 =
        "usage: hdfs scm -container -info <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container name\n";
    assertEquals(expected4, testContent.toString());
    testContent.reset();

    String[] args5 = {"-container", "-list", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args5));
    String expected5 =
        "usage: hdfs scm -container -list <option>\n" +
            "where <option> can be the following\n" +
            " -count <arg>    Specify count number, required\n" +
            " -prefix <arg>   Specify prefix container name\n" +
            " -start <arg>    Specify start container name\n";
    assertEquals(expected5, testContent.toString());
    testContent.reset();

    System.setOut(init);
  }
}
