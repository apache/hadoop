/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.container.common;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.DeleteBlocksCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionResponseProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine
    .DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine
    .EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.states.endpoint
    .HeartbeatEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint
    .RegisterEndpointTask;
import org.apache.hadoop.ozone.container.common.states.endpoint
    .VersionEndpointTask;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.UUID;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils
    .createEndpoint;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the endpoints.
 */
public class TestEndPoint {
  private static InetSocketAddress serverAddress;
  private static RPC.Server scmServer;
  private static ScmTestMock scmServerImpl;
  private static File testDir;
  private static Configuration config;

  @AfterClass
  public static void tearDown() throws Exception {
    if (scmServer != null) {
      scmServer.stop();
    }
    FileUtil.fullyDelete(testDir);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    serverAddress = SCMTestUtils.getReuseableAddress();
    scmServerImpl = new ScmTestMock();
    scmServer = SCMTestUtils.startScmRpcServer(SCMTestUtils.getConf(),
        scmServerImpl, serverAddress, 10);
    testDir = PathUtils.getTestDir(TestEndPoint.class);
    config = SCMTestUtils.getConf();
    config.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    config.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    config
        .setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    config.set(HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL, "1s");
  }

  @Test
  /**
   * This test asserts that we are able to make a version call to SCM server
   * and gets back the expected values.
   */
  public void testGetVersion() throws Exception {
    try (EndpointStateMachine rpcEndPoint =
             createEndpoint(SCMTestUtils.getConf(),
                 serverAddress, 1000)) {
      SCMVersionResponseProto responseProto = rpcEndPoint.getEndPoint()
          .getVersion(null);
      Assert.assertNotNull(responseProto);
      Assert.assertEquals(VersionInfo.DESCRIPTION_KEY,
          responseProto.getKeys(0).getKey());
      Assert.assertEquals(VersionInfo.getLatestVersion().getDescription(),
          responseProto.getKeys(0).getValue());
    }
  }

  @Test
  /**
   * We make getVersion RPC call, but via the VersionEndpointTask which is
   * how the state machine would make the call.
   */
  public void testGetVersionTask() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, 1000)) {
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails), null);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      Assert.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          newState);

      // Now rpcEndpoint should remember the version it got from SCM
      Assert.assertNotNull(rpcEndPoint.getVersion());
    }
  }

  @Test
  public void testCheckVersionResponse() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, 1000)) {
      GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
          .captureLogs(VersionEndpointTask.LOG);
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails), null);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // if version call worked the endpoint should automatically move to the
      // next state.
      Assert.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          newState);

      // Now rpcEndpoint should remember the version it got from SCM
      Assert.assertNotNull(rpcEndPoint.getVersion());

      // Now change server scmId, so datanode scmId  will be
      // different from SCM server response scmId
      String newScmId = UUID.randomUUID().toString();
      scmServerImpl.setScmId(newScmId);
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      newState = versionTask.call();
      Assert.assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN,
            newState);
      List<HddsVolume> volumesList = ozoneContainer.getVolumeSet()
          .getFailedVolumesList();
      Assert.assertTrue(volumesList.size() == 1);
      File expectedScmDir = new File(volumesList.get(0).getHddsRootDir(),
          scmServerImpl.getScmId());
      Assert.assertTrue(logCapturer.getOutput().contains("expected scm " +
          "directory " + expectedScmDir.getAbsolutePath() + " does not " +
          "exist"));
      Assert.assertTrue(ozoneContainer.getVolumeSet().getVolumesList().size()
          == 0);
      Assert.assertTrue(ozoneContainer.getVolumeSet().getFailedVolumesList()
          .size() == 1);

    }
  }



  @Test
  /**
   * This test makes a call to end point where there is no SCM server. We
   * expect that versionTask should be able to handle it.
   */
  public void testGetVersionToInvalidEndpoint() throws Exception {
    OzoneConfiguration conf = SCMTestUtils.getConf();
    InetSocketAddress nonExistentServerAddress = SCMTestUtils
        .getReuseableAddress();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        nonExistentServerAddress, 1000)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails), null);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);
      EndpointStateMachine.EndPointStates newState = versionTask.call();

      // This version call did NOT work, so endpoint should remain in the same
      // state.
      Assert.assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
          newState);
    }
  }

  @Test
  /**
   * This test makes a getVersionRPC call, but the DummyStorageServer is
   * going to respond little slowly. We will assert that we are still in the
   * GETVERSION state after the timeout.
   */
  public void testGetVersionAssertRpcTimeOut() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 100;
    OzoneConfiguration conf = SCMTestUtils.getConf();

    try (EndpointStateMachine rpcEndPoint = createEndpoint(conf,
        serverAddress, (int) rpcTimeout)) {
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.GETVERSION);
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      OzoneContainer ozoneContainer = new OzoneContainer(
          datanodeDetails, conf, getContext(datanodeDetails), null);
      VersionEndpointTask versionTask = new VersionEndpointTask(rpcEndPoint,
          conf, ozoneContainer);

      scmServerImpl.setRpcResponseDelay(1500);
      long start = Time.monotonicNow();
      EndpointStateMachine.EndPointStates newState = versionTask.call();
      long end = Time.monotonicNow();
      scmServerImpl.setRpcResponseDelay(0);
      Assert.assertThat(end - start, lessThanOrEqualTo(rpcTimeout + tolerance));
      Assert.assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
          newState);
    }
  }

  @Test
  public void testRegister() throws Exception {
    DatanodeDetails nodeToRegister = TestUtils.randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint = createEndpoint(
        SCMTestUtils.getConf(), serverAddress, 1000)) {
      SCMRegisteredResponseProto responseProto = rpcEndPoint.getEndPoint()
          .register(nodeToRegister.getProtoBufMessage(), TestUtils
                  .createNodeReport(
                      getStorageReports(nodeToRegister.getUuid())),
              TestUtils.getRandomContainerReports(10),
                  TestUtils.getRandomPipelineReports());
      Assert.assertNotNull(responseProto);
      Assert.assertEquals(nodeToRegister.getUuidString(),
          responseProto.getDatanodeUUID());
      Assert.assertNotNull(responseProto.getClusterID());
      Assert.assertEquals(10, scmServerImpl.
          getContainerCountsForDatanode(nodeToRegister));
      Assert.assertEquals(1, scmServerImpl.getNodeReportsCount(nodeToRegister));
    }
  }

  private StorageReportProto getStorageReports(UUID id) {
    String storagePath = testDir.getAbsolutePath() + "/" + id;
    return TestUtils.createStorageReport(id, storagePath, 100, 10, 90, null);
  }

  private EndpointStateMachine registerTaskHelper(InetSocketAddress scmAddress,
      int rpcTimeout, boolean clearDatanodeDetails) throws Exception {
    Configuration conf = SCMTestUtils.getConf();
    EndpointStateMachine rpcEndPoint =
        createEndpoint(conf,
            scmAddress, rpcTimeout);
    rpcEndPoint.setState(EndpointStateMachine.EndPointStates.REGISTER);
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getNodeReport()).thenReturn(TestUtils
        .createNodeReport(getStorageReports(UUID.randomUUID())));
    ContainerController controller = Mockito.mock(ContainerController.class);
    when(controller.getContainerReport()).thenReturn(
        TestUtils.getRandomContainerReports(10));
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getPipelineReport()).thenReturn(
            TestUtils.getRandomPipelineReports());
    RegisterEndpointTask endpointTask =
        new RegisterEndpointTask(rpcEndPoint, conf, ozoneContainer,
            mock(StateContext.class));
    if (!clearDatanodeDetails) {
      DatanodeDetails datanodeDetails = TestUtils.randomDatanodeDetails();
      endpointTask.setDatanodeDetails(datanodeDetails);
    }
    endpointTask.call();
    return rpcEndPoint;
  }

  @Test
  public void testRegisterTask() throws Exception {
    try (EndpointStateMachine rpcEndpoint =
             registerTaskHelper(serverAddress, 1000, false)) {
      // Successful register should move us to Heartbeat state.
      Assert.assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterToInvalidEndpoint() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
             registerTaskHelper(address, 1000, false)) {
      Assert.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterNoContainerID() throws Exception {
    InetSocketAddress address = SCMTestUtils.getReuseableAddress();
    try (EndpointStateMachine rpcEndpoint =
             registerTaskHelper(address, 1000, true)) {
      // No Container ID, therefore we tell the datanode that we would like to
      // shutdown.
      Assert.assertEquals(EndpointStateMachine.EndPointStates.SHUTDOWN,
          rpcEndpoint.getState());
    }
  }

  @Test
  public void testRegisterRpcTimeout() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 200;
    scmServerImpl.setRpcResponseDelay(1500);
    long start = Time.monotonicNow();
    registerTaskHelper(serverAddress, 1000, false).close();
    long end = Time.monotonicNow();
    scmServerImpl.setRpcResponseDelay(0);
    Assert.assertThat(end - start, lessThanOrEqualTo(rpcTimeout + tolerance));
  }

  @Test
  public void testHeartbeat() throws Exception {
    DatanodeDetails dataNode = TestUtils.randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
             createEndpoint(SCMTestUtils.getConf(),
                 serverAddress, 1000)) {
      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(TestUtils.createNodeReport(
              getStorageReports(UUID.randomUUID())))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      Assert.assertNotNull(responseProto);
      Assert.assertEquals(0, responseProto.getCommandsCount());
    }
  }

  @Test
  public void testHeartbeatWithCommandStatusReport() throws Exception {
    DatanodeDetails dataNode = TestUtils.randomDatanodeDetails();
    try (EndpointStateMachine rpcEndPoint =
        createEndpoint(SCMTestUtils.getConf(),
            serverAddress, 1000)) {
      // Add some scmCommands for heartbeat response
      addScmCommands();


      SCMHeartbeatRequestProto request = SCMHeartbeatRequestProto.newBuilder()
          .setDatanodeDetails(dataNode.getProtoBufMessage())
          .setNodeReport(TestUtils.createNodeReport(
              getStorageReports(UUID.randomUUID())))
          .build();

      SCMHeartbeatResponseProto responseProto = rpcEndPoint.getEndPoint()
          .sendHeartbeat(request);
      assertNotNull(responseProto);
      assertEquals(3, responseProto.getCommandsCount());
      assertEquals(0, scmServerImpl.getCommandStatusReportCount());

      // Send heartbeat again from heartbeat endpoint task
      final StateContext stateContext = heartbeatTaskHelper(
          serverAddress, 3000);
      Map<Long, CommandStatus> map = stateContext.getCommandStatusMap();
      assertNotNull(map);
      assertEquals("Should have 2 objects", 2, map.size());
      assertTrue(map.containsKey(Long.valueOf(2)));
      assertTrue(map.containsKey(Long.valueOf(3)));
      assertTrue(map.get(Long.valueOf(2)).getType()
          .equals(Type.replicateContainerCommand));
      assertTrue(
          map.get(Long.valueOf(3)).getType().equals(Type.deleteBlocksCommand));
      assertTrue(map.get(Long.valueOf(2)).getStatus().equals(Status.PENDING));
      assertTrue(map.get(Long.valueOf(3)).getStatus().equals(Status.PENDING));

      scmServerImpl.clearScmCommandRequests();
    }
  }

  private void addScmCommands() {
    SCMCommandProto closeCommand = SCMCommandProto.newBuilder()
        .setCloseContainerCommandProto(
            CloseContainerCommandProto.newBuilder().setCmdId(1)
        .setContainerID(1)
        .setPipelineID(PipelineID.randomId().getProtobuf())
        .build())
        .setCommandType(Type.closeContainerCommand)
        .build();
    SCMCommandProto replicationCommand = SCMCommandProto.newBuilder()
        .setReplicateContainerCommandProto(
            ReplicateContainerCommandProto.newBuilder()
        .setCmdId(2)
        .setContainerID(2)
        .build())
        .setCommandType(Type.replicateContainerCommand)
        .build();
    SCMCommandProto deleteBlockCommand = SCMCommandProto.newBuilder()
        .setDeleteBlocksCommandProto(
            DeleteBlocksCommandProto.newBuilder()
                .setCmdId(3)
                .addDeletedBlocksTransactions(
                    DeletedBlocksTransaction.newBuilder()
                        .setContainerID(45)
                        .setCount(1)
                        .setTxID(23)
                        .build())
                .build())
        .setCommandType(Type.deleteBlocksCommand)
        .build();
    scmServerImpl.addScmCommandRequest(closeCommand);
    scmServerImpl.addScmCommandRequest(deleteBlockCommand);
    scmServerImpl.addScmCommandRequest(replicationCommand);
  }

  private StateContext heartbeatTaskHelper(InetSocketAddress scmAddress,
      int rpcTimeout) throws Exception {
    Configuration conf = SCMTestUtils.getConf();
    conf.set(DFS_DATANODE_DATA_DIR_KEY, testDir.getAbsolutePath());
    conf.set(OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    // Mini Ozone cluster will not come up if the port is not true, since
    // Ratis will exit if the server port cannot be bound. We can remove this
    // hard coding once we fix the Ratis default behaviour.
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);


    // Create a datanode state machine for stateConext used by endpoint task
    try (DatanodeStateMachine stateMachine = new DatanodeStateMachine(
        TestUtils.randomDatanodeDetails(), conf, null);
         EndpointStateMachine rpcEndPoint =
            createEndpoint(conf, scmAddress, rpcTimeout)) {
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto =
          TestUtils.randomDatanodeDetails().getProtoBufMessage();
      rpcEndPoint.setState(EndpointStateMachine.EndPointStates.HEARTBEAT);

      final StateContext stateContext =
          new StateContext(conf, DatanodeStateMachine.DatanodeStates.RUNNING,
              stateMachine);

      HeartbeatEndpointTask endpointTask =
          new HeartbeatEndpointTask(rpcEndPoint, conf, stateContext);
      endpointTask.setDatanodeDetailsProto(datanodeDetailsProto);
      endpointTask.call();
      Assert.assertNotNull(endpointTask.getDatanodeDetailsProto());

      Assert.assertEquals(EndpointStateMachine.EndPointStates.HEARTBEAT,
          rpcEndPoint.getState());
      return stateContext;
    }
  }

  @Test
  public void testHeartbeatTask() throws Exception {
    heartbeatTaskHelper(serverAddress, 1000);
  }

  @Test
  public void testHeartbeatTaskToInvalidNode() throws Exception {
    InetSocketAddress invalidAddress = SCMTestUtils.getReuseableAddress();
    heartbeatTaskHelper(invalidAddress, 1000);
  }

  @Test
  public void testHeartbeatTaskRpcTimeOut() throws Exception {
    final long rpcTimeout = 1000;
    final long tolerance = 200;
    scmServerImpl.setRpcResponseDelay(1500);
    long start = Time.monotonicNow();
    InetSocketAddress invalidAddress = SCMTestUtils.getReuseableAddress();
    heartbeatTaskHelper(invalidAddress, 1000);
    long end = Time.monotonicNow();
    scmServerImpl.setRpcResponseDelay(0);
    Assert.assertThat(end - start,
        lessThanOrEqualTo(rpcTimeout + tolerance));
  }

  private StateContext getContext(DatanodeDetails datanodeDetails) {
    DatanodeStateMachine stateMachine = Mockito.mock(
        DatanodeStateMachine.class);
    StateContext context = Mockito.mock(StateContext.class);
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanodeDetails);
    Mockito.when(context.getParent()).thenReturn(stateMachine);
    return context;
  }

}
