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

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ResultCode;
import org.apache.hadoop.hdds.scm.cli.SCMCLI;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;

import static org.apache.hadoop.hdds.scm.cli.ResultCode.EXECUTION_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * This class tests the CLI of SCM.
 */
@Ignore ("Needs to be fixed for new SCM and Storage design")
public class TestSCMCli {
  private static SCMCLI cli;

  private static MiniOzoneCluster cluster;
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
  private static String containerOwner = "OZONE";

  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    xceiverClientManager = new XceiverClientManager(conf);
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
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
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testCreateContainer() throws Exception {
    ByteArrayOutputStream testContent = new ByteArrayOutputStream();
    PrintStream testPrintOut = new PrintStream(testContent);
    System.setOut(testPrintOut);
    String[] args = {"-container", "-create"};
    assertEquals(ResultCode.SUCCESS, cli.run(args));
    assertEquals("", testContent.toString());
  }

  private boolean containerExist(long containerID) {
    try {
      ContainerInfo container = scm.getClientProtocolServer()
          .getContainerWithPipeline(containerID).getContainerInfo();
      return container != null
          && containerID == container.getContainerID();
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
    ContainerWithPipeline container = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    KeyValueContainerData kvData = KeyValueContainerData
        .getFromProtoBuf(containerOperationClient.readContainer(
            container.getContainerInfo().getContainerID(), container
                .getPipeline()));
    KeyUtils.getDB(kvData, conf)
        .put(Longs.toByteArray(container.getContainerInfo().getContainerID()),
            "someKey".getBytes());
    Assert.assertTrue(containerExist(container.getContainerInfo()
        .getContainerID()));

    // Gracefully delete a container should fail because it is open.
    delCmd = new String[]{"-container", "-delete", "-c",
        Long.toString(container.getContainerInfo().getContainerID())};
    testErr = new ByteArrayOutputStream();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(testErr.toString()
        .contains("Deleting an open container is not allowed."));
    Assert.assertTrue(
        containerExist(container.getContainerInfo().getContainerID()));

    // Close the container
    containerOperationClient.closeContainer(
        container.getContainerInfo().getContainerID());

    // Gracefully delete a container should fail because it is not empty.
    testErr = new ByteArrayOutputStream();
    int exitCode2 = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode2);
    assertTrue(testErr.toString()
        .contains("Container cannot be deleted because it is not empty."));
    Assert.assertTrue(
        containerExist(container.getContainerInfo().getContainerID()));

    // Try force delete again.
    delCmd = new String[]{"-container", "-delete", "-c",
        Long.toString(container.getContainerInfo().getContainerID()), "-f"};
    exitCode = runCommandAndGetOutput(delCmd, out, null);
    assertEquals("Expected success, found:", ResultCode.SUCCESS, exitCode);
    assertFalse(containerExist(container.getContainerInfo().getContainerID()));

    // ****************************************
    // 2. Test to delete an empty container.
    // ****************************************
    // Create an empty container
    ContainerWithPipeline emptyContainer = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    containerOperationClient
        .closeContainer(emptyContainer.getContainerInfo().getContainerID());
    Assert.assertTrue(
        containerExist(emptyContainer.getContainerInfo().getContainerID()));

    // Successfully delete an empty container.
    delCmd = new String[]{"-container", "-delete", "-c",
        Long.toString(emptyContainer.getContainerInfo().getContainerID())};
    exitCode = runCommandAndGetOutput(delCmd, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    assertFalse(
        containerExist(emptyContainer.getContainerInfo().getContainerID()));

    // After the container is deleted,
    // another container can now be recreated.
    ContainerWithPipeline newContainer = containerOperationClient.
        createContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    Assert.assertTrue(
        containerExist(newContainer.getContainerInfo().getContainerID()));

    // ****************************************
    // 3. Test to delete a non-exist container.
    // ****************************************
    long nonExistContainerID = ContainerTestHelper.getTestContainerID();
    delCmd = new String[]{"-container", "-delete", "-c",
        Long.toString(nonExistContainerID)};
    testErr = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, out, testErr);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(testErr.toString()
        .contains("Specified key does not exist."));
  }

  @Test
  public void testInfoContainer() throws Exception {
    // The cluster has one Datanode server.
    DatanodeDetails datanodeDetails = cluster.getHddsDatanodes().get(0)
        .getDatanodeDetails();
    String formatStr =
        "Container id: %s\n" +
        "Container State: %s\n" +
        "Container DB Path: %s\n" +
        "Container Path: %s\n" +
        "Container Metadata: {%s}\n" +
        "LeaderID: %s\n" +
        "Datanodes: [%s]\n";

    // Test a non-exist container
    String containerID =
        Long.toString(ContainerTestHelper.getTestContainerID());
    String[] info = {"-container", "-info", containerID};
    int exitCode = runCommandAndGetOutput(info, null, null);
    assertEquals("Expected Execution Error, Did not find that.",
        EXECUTION_ERROR, exitCode);

    // Create an empty container.
    ContainerWithPipeline container = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    KeyValueContainerData data = KeyValueContainerData
        .getFromProtoBuf(containerOperationClient.
            readContainer(container.getContainerInfo().getContainerID(),
                container.getPipeline()));
    info = new String[]{"-container", "-info", "-c",
        Long.toString(container.getContainerInfo().getContainerID())};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals("Expected Success, did not find it.", ResultCode.SUCCESS,
        exitCode);

    String openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    String expected =
        String.format(formatStr, container.getContainerInfo().getContainerID
                (), openStatus, data.getDbFile().getPath(), data
                .getContainerPath(), "", datanodeDetails.getHostName(),
            datanodeDetails.getHostName());

    assertEquals(expected, out.toString());

    out.reset();

    // Create an non-empty container
    container = containerOperationClient
        .createContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    data = KeyValueContainerData
        .getFromProtoBuf(containerOperationClient.readContainer(
            container.getContainerInfo().getContainerID(), container
                .getPipeline()));
    KeyUtils.getDB(data, conf)
        .put(containerID.getBytes(), "someKey".getBytes());

    info = new String[]{"-container", "-info", "-c",
        Long.toString(container.getContainerInfo().getContainerID())};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";

    expected = String.format(formatStr, container.getContainerInfo()
            .getContainerID(), openStatus, data.getDbFile().getPath(), data
            .getContainerPath(), "", datanodeDetails.getHostName(),
        datanodeDetails.getHostName());
    assertEquals(expected, out.toString());

    out.reset();

    // Close last container and test info again.
    containerOperationClient
        .closeContainer(container.getContainerInfo().getContainerID());

    info = new String[]{"-container", "-info", "-c",
        Long.toString(container.getContainerInfo().getContainerID())};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    data = KeyValueContainerData
        .getFromProtoBuf(containerOperationClient.readContainer(
            container.getContainerInfo().getContainerID(), container
                .getPipeline()));

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String
        .format(formatStr, container.getContainerInfo().getContainerID(),
            openStatus, data.getDbFile().getPath(), data.getContainerPath(), "",
            datanodeDetails.getHostName(), datanodeDetails.getHostName());
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
    List<ContainerInfo> containers = new ArrayList<>();
    for (int index = 0; index < 20; index++) {
      ContainerWithPipeline container = containerOperationClient.createContainer(
          xceiverClientManager.getType(), HddsProtos.ReplicationFactor.ONE,
          containerOwner);
      containers.add(container.getContainerInfo());
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

    long startContainerID = containers.get(0).getContainerID();
    String startContainerIDStr = Long.toString(startContainerID);
    // Test with -start and -count, the value of -count is negative.
    args = new String[] {"-container", "-list",
        "-start", startContainerIDStr, "-count", "-1"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(EXECUTION_ERROR, exitCode);
    assertTrue(err.toString()
        .contains("-count should not be negative"));

    out.reset();
    err.reset();

    // Test with -start and -count.
    args = new String[] {"-container", "-list", "-start",
        startContainerIDStr, "-count", "10"};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.SUCCESS, exitCode);
    for (int index = 1; index < 10; index++) {
      String containerID = Long.toString(
          containers.get(index).getContainerID());
      assertTrue(out.toString().contains(containerID));
    }

    out.reset();
    err.reset();

    // Test with -start, while -count doesn't exist.
    args = new String[] {"-container", "-list", "-start",
        startContainerIDStr};
    exitCode = runCommandAndGetOutput(args, out, err);
    assertEquals(ResultCode.EXECUTION_ERROR, exitCode);
    assertTrue(err.toString().contains(
        "java.io.IOException: Expecting container count"));
  }

  @Test
  public void testCloseContainer() throws Exception {
    long containerID = containerOperationClient.createContainer(
        xceiverClientManager.getType(), HddsProtos.ReplicationFactor.ONE,
        containerOwner).getContainerInfo().getContainerID();
    ContainerInfo container = scm.getClientProtocolServer()
        .getContainerWithPipeline(containerID).getContainerInfo();
    assertNotNull(container);
    assertEquals(containerID, container.getContainerID());

    ContainerInfo containerInfo = scm.getContainerInfo(containerID);
    assertEquals(OPEN, containerInfo.getState());

    String[] args1 = {"-container", "-close", "-c",
        Long.toString(containerID)};
    assertEquals(ResultCode.SUCCESS, cli.run(args1));

    containerInfo = scm.getContainerInfo(containerID);
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
        "usage: hdfs scm -container -create\n\n";
    assertEquals(expected2, testContent.toString());
    testContent.reset();

    String[] args3 = {"-container", "-delete", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args3));
    String expected3 =
        "usage: hdfs scm -container -delete <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container id\n" +
        " -f         forcibly delete a container\n";
    assertEquals(expected3, testContent.toString());
    testContent.reset();

    String[] args4 = {"-container", "-info", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args4));
    String expected4 =
        "usage: hdfs scm -container -info <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container id\n";
    assertEquals(expected4, testContent.toString());
    testContent.reset();

    String[] args5 = {"-container", "-list", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args5));
    String expected5 = "usage: hdfs scm -container -list <option>\n"
        + "where <option> can be the following\n"
        + " -count <arg>   Specify count number, required\n"
        + " -start <arg>   Specify start container id\n";
    assertEquals(expected5, testContent.toString());
    testContent.reset();

    System.setOut(init);
  }
}
