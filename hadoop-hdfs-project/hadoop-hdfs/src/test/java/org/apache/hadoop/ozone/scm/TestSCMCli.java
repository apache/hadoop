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
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.scm.cli.ResultCode;
import org.apache.hadoop.ozone.scm.cli.SCMCLI;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.client.ContainerOperationClient;
import org.apache.hadoop.scm.client.ScmClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class tests the CLI of SCM.
 */
public class TestSCMCli {
  private static SCMCLI cli;

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  private static StorageContainerManager scm;
  private static ContainerManager containerManager;

  private static ByteArrayOutputStream outContent;
  private static PrintStream outStream;
  private static ByteArrayOutputStream errContent;
  private static PrintStream errStream;

  @Rule
  public Timeout globalTimeout = new Timeout(30000);

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .setHandlerType("distributed").build();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    ScmClient client = new ContainerOperationClient(
        storageContainerLocationClient, new XceiverClientManager(conf));
    outContent = new ByteArrayOutputStream();
    outStream = new PrintStream(outContent);
    errContent = new ByteArrayOutputStream();
    errStream = new PrintStream(errContent);
    cli = new SCMCLI(client, outStream, errStream);
    scm = cluster.getStorageContainerManager();
    containerManager = cluster.getDataNodes().get(0)
        .getOzoneContainerManager().getContainerManager();
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
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
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
    pipeline = scm.allocateContainer(containerName);
    containerData = new ContainerData(containerName);
    containerManager.createContainer(pipeline, containerData);
    ContainerData cdata = containerManager.readContainer(containerName);
    KeyUtils.getDB(cdata, conf).put(containerName.getBytes(),
        "someKey".getBytes());
    Assert.assertTrue(containerExist(containerName));

    // Gracefully delete a container should fail because it is open.
    delCmd = new String[] {"-container", "-del", containerName};
    testErr = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, null, testErr);
    assertEquals(ResultCode.EXECUTION_ERROR, exitCode);
    assertTrue(testErr.toString()
        .contains("Deleting an open container is not allowed."));
    Assert.assertTrue(containerExist(containerName));

    // Close the container
    containerManager.closeContainer(containerName);

    // Gracefully delete a container should fail because it is not empty.
    testErr = new ByteArrayOutputStream();
    int exitCode2 = runCommandAndGetOutput(delCmd, null, testErr);
    assertEquals(ResultCode.EXECUTION_ERROR, exitCode2);
    assertTrue(testErr.toString()
        .contains("Container cannot be deleted because it is not empty."));
    Assert.assertTrue(containerExist(containerName));

    // Try force delete again.
    delCmd = new String[] {"-container", "-del", containerName, "-f"};
    exitCode = runCommandAndGetOutput(delCmd, null, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    Assert.assertFalse(containerExist(containerName));

    // ****************************************
    // 2. Test to delete an empty container.
    // ****************************************
    // Create an empty container
    containerName = "empty-container";
    pipeline = scm.allocateContainer(containerName);
    containerData = new ContainerData(containerName);
    containerManager.createContainer(pipeline, containerData);
    containerManager.closeContainer(containerName);
    Assert.assertTrue(containerExist(containerName));

    // Successfully delete an empty container.
    delCmd = new String[] {"-container", "-del", containerName};
    exitCode = runCommandAndGetOutput(delCmd, null, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    Assert.assertFalse(containerExist(containerName));

    // After the container is deleted,
    // a same name container can now be recreated.
    pipeline = scm.allocateContainer(containerName);
    containerManager.createContainer(pipeline, containerData);
    Assert.assertTrue(containerExist(containerName));

    // ****************************************
    // 3. Test to delete a non-exist container.
    // ****************************************
    containerName = "non-exist-container";
    delCmd = new String[] {"-container", "-del", containerName};
    testErr = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(delCmd, null, testErr);
    assertEquals(ResultCode.EXECUTION_ERROR, exitCode);
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
    assertEquals(ResultCode.EXECUTION_ERROR, exitCode);

    // Create an empty container.
    cname = "ContainerTestInfo1";
    Pipeline pipeline = scm.allocateContainer(cname);
    ContainerData data = new ContainerData(cname);
    containerManager.createContainer(pipeline, data);

    info = new String[]{"-container", "-info", cname};
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);

    String openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    String expected = String.format(formatStr, cname, openStatus,
        data.getDBPath(), data.getContainerPath(), "",
        datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());

    out.reset();

    // Create an non-empty container
    cname = "ContainerTestInfo2";
    pipeline = scm.allocateContainer(cname);
    data = new ContainerData(cname);
    containerManager.createContainer(pipeline, data);
    KeyUtils.getDB(data, conf).put(cname.getBytes(),
        "someKey".getBytes());

    info = new String[]{"-container", "-info", cname};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String.format(formatStr, cname, openStatus,
        data.getDBPath(), data.getContainerPath(), "",
        datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());

    out.reset();

    // Create a container with some meta data.
    cname = "ContainerTestInfo3";
    pipeline = scm.allocateContainer(cname);
    data = new ContainerData(cname);
    data.addMetadata("VOLUME", "shire");
    data.addMetadata("owner", "bilbo");
    containerManager.createContainer(pipeline, data);
    KeyUtils.getDB(data, conf).put(cname.getBytes(),
        "someKey".getBytes());

    List<String> metaList = data.getAllMetadata().entrySet().stream()
        .map(entry -> entry.getKey() + ":" + entry.getValue())
        .collect(Collectors.toList());
    String metadataStr = StringUtils.join(", ", metaList);

    info = new String[]{"-container", "-info", cname};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String.format(formatStr, cname, openStatus,
        data.getDBPath(), data.getContainerPath(), metadataStr,
        datanodeID.getHostName(), datanodeID.getHostName());
    assertEquals(expected, out.toString());

    out.reset();

    // Close last container and test info again.
    containerManager.closeContainer(cname);

    info = new String[]{"-container", "-info", cname};
    exitCode = runCommandAndGetOutput(info, out, null);
    assertEquals(ResultCode.SUCCESS, exitCode);
    data = containerManager.readContainer(cname);

    openStatus = data.isOpen() ? "OPEN" : "CLOSED";
    expected = String.format(formatStrWithHash, cname, openStatus,
        data.getHash(), data.getDBPath(), data.getContainerPath(),
        metadataStr, datanodeID.getHostName(), datanodeID.getHostName());
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
        "usage: hdfs scm <commands> [<options>]\n" +
        "where <commands> can be one of the following\n" +
        " -container   Container related options\n";
    assertEquals(expectedOut, testContent.toString());
    System.setOut(init);
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
        "usage: hdfs scm <commands> [<options>]\n" +
        "where <commands> can be one of the following\n" +
        " -container   Container related options\n";
    assertEquals(expected, testContent.toString());
    testContent.reset();

    String[] args1 = {"-container", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args1));
    String expected1 =
        "usage: hdfs scm -container <commands> <options>\n" +
        "where <commands> can be one of the following\n" +
        " -create       Create container\n" +
        " -del <arg>    Delete container\n" +
        " -info <arg>   Info container\n";

    assertEquals(expected1, testContent.toString());
    testContent.reset();

    String[] args2 = {"-container", "-create", "-help"};
    assertEquals(ResultCode.SUCCESS, cli.run(args2));
    String expected2 =
        "usage: hdfs scm -container -create <option>\n" +
        "where <option> is\n" +
        " -c <arg>   Specify container name\n";
    assertEquals(expected2, testContent.toString());
    System.setOut(init);
  }
}
