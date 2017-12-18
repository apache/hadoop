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

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys
    .OZONE_SCM_HEARTBEAT_RPC_TIMEOUT;
import static org.junit.Assert.assertTrue;

/**
 * Tests the datanode state machine class and its states.
 */
public class TestDatanodeStateMachine {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDatanodeStateMachine.class);
  private final int scmServerCount = 3;
  private List<String> serverAddresses;
  private List<RPC.Server> scmServers;
  private List<ScmTestMock> mockServers;
  private ExecutorService executorService;
  private Configuration conf;
  private File testRoot;

  @Before
  public void setUp() throws Exception {
    conf = SCMTestUtils.getConf();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_RPC_TIMEOUT, 500,
        TimeUnit.MILLISECONDS);
    conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_RANDOM_PORT, true);
    serverAddresses = new LinkedList<>();
    scmServers = new LinkedList<>();
    mockServers = new LinkedList<>();
    for (int x = 0; x < scmServerCount; x++) {
      int port = SCMTestUtils.getReuseableAddress().getPort();
      String address = "127.0.0.1";
      serverAddresses.add(address + ":" + port);
      ScmTestMock mock = new ScmTestMock();

      scmServers.add(SCMTestUtils.startScmRpcServer(conf, mock,
          new InetSocketAddress(address, port), 10));
      mockServers.add(mock);
    }

    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES,
        serverAddresses.toArray(new String[0]));

    String path = GenericTestUtils
        .getTempPath(TestDatanodeStateMachine.class.getSimpleName());
    testRoot = new File(path);
    if (!testRoot.mkdirs()) {
      LOG.info("Required directories {} already exist.", testRoot);
    }

    File dataDir = new File(testRoot, "data");
    conf.set(DFS_DATANODE_DATA_DIR_KEY, dataDir.getAbsolutePath());
    if (!dataDir.mkdirs()) {
      LOG.info("Data dir create failed.");
    }
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        new File(testRoot, "scm").getAbsolutePath());
    path = Paths.get(path.toString(),
        TestDatanodeStateMachine.class.getSimpleName() + ".id").toString();
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ID, path);
    executorService = HadoopExecutors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Test Data Node State Machine Thread - %d").build());
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (executorService != null) {
        executorService.shutdown();
        try {
          if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
          }

          if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            LOG.error("Unable to shutdown properly.");
          }
        } catch (InterruptedException e) {
          LOG.error("Error attempting to shutdown.", e);
          executorService.shutdownNow();
        }
      }
      for (RPC.Server s : scmServers) {
        s.stop();
      }
    } catch (Exception e) {
      //ignore all execption from the shutdown
    } finally {
      testRoot.delete();
    }
  }

  /**
   * Assert that starting statemachine executes the Init State.
   *
   * @throws InterruptedException
   */
  @Test
  public void testStartStopDatanodeStateMachine() throws IOException,
      InterruptedException, TimeoutException {
    try (DatanodeStateMachine stateMachine =
        new DatanodeStateMachine(DFSTestUtil.getLocalDatanodeID(), conf)) {
      stateMachine.startDaemon();
      SCMConnectionManager connectionManager =
          stateMachine.getConnectionManager();
      GenericTestUtils.waitFor(() -> connectionManager.getValues().size() == 3,
          1000, 30000);

      stateMachine.stopDaemon();
      assertTrue(stateMachine.isDaemonStopped());
    }
  }

  /**
   * This test explores the state machine by invoking each call in sequence just
   * like as if the state machine would call it. Because this is a test we are
   * able to verify each of the assumptions.
   * <p>
   * Here is what happens at High level.
   * <p>
   * 1. We start the datanodeStateMachine in the INIT State.
   * <p>
   * 2. We invoke the INIT state task.
   * <p>
   * 3. That creates a set of RPC endpoints that are ready to connect to SCMs.
   * <p>
   * 4. We assert that we have moved to the running state for the
   * DatanodeStateMachine.
   * <p>
   * 5. We get the task for the Running State - Executing that running state,
   * makes the first network call in of the state machine. The Endpoint is in
   * the GETVERSION State and we invoke the task.
   * <p>
   * 6. We assert that this call was a success by checking that each of the
   * endponts now have version response that it got from the SCM server that it
   * was talking to and also each of the mock server serviced one RPC call.
   * <p>
   * 7. Since the Register is done now, next calls to get task will return
   * HeartbeatTask, which sends heartbeats to SCM. We assert that we get right
   * task from sub-system below.
   *
   * @throws IOException
   */
  @Test
  public void testDatanodeStateContext() throws IOException,
      InterruptedException, ExecutionException, TimeoutException {
    // There is no mini cluster started in this test,
    // create a ID file so that state machine could load a fake datanode ID.
    File idPath = new File(
        conf.get(ScmConfigKeys.OZONE_SCM_DATANODE_ID));
    idPath.delete();
    DatanodeID dnID = DFSTestUtil.getLocalDatanodeID();
    dnID.setContainerPort(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT_DEFAULT);
    ContainerUtils.writeDatanodeIDTo(dnID, idPath);

    try (DatanodeStateMachine stateMachine = new DatanodeStateMachine(
        DFSTestUtil.getLocalDatanodeID(), conf)) {
      DatanodeStateMachine.DatanodeStates currentState =
          stateMachine.getContext().getState();
      Assert.assertEquals(DatanodeStateMachine.DatanodeStates.INIT,
          currentState);

      DatanodeState<DatanodeStateMachine.DatanodeStates> task =
          stateMachine.getContext().getTask();
      Assert.assertEquals(InitDatanodeState.class, task.getClass());

      task.execute(executorService);
      DatanodeStateMachine.DatanodeStates newState =
          task.await(2, TimeUnit.SECONDS);

      for (EndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getValues()) {
        // We assert that each of the is in State GETVERSION.
        Assert.assertEquals(EndpointStateMachine.EndPointStates.GETVERSION,
            endpoint.getState());
      }

      // The Datanode has moved into Running State, since endpoints are created.
      // We move to running state when we are ready to issue RPC calls to SCMs.
      Assert.assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      // If we had called context.execute instead of calling into each state
      // this would have happened automatically.
      stateMachine.getContext().setState(newState);
      task = stateMachine.getContext().getTask();
      Assert.assertEquals(RunningDatanodeState.class, task.getClass());

      // This execute will invoke getVersion calls against all SCM endpoints
      // that we know of.

      task.execute(executorService);
      newState = task.await(10, TimeUnit.SECONDS);
      // If we are in running state, we should be in running.
      Assert.assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      for (EndpointStateMachine endpoint :
          stateMachine.getConnectionManager().getValues()) {

        // Since the earlier task.execute called into GetVersion, the
        // endPointState Machine should move to REGISTER state.
        Assert.assertEquals(EndpointStateMachine.EndPointStates.REGISTER,
            endpoint.getState());

        // We assert that each of the end points have gotten a version from the
        // SCM Server.
        Assert.assertNotNull(endpoint.getVersion());
      }

      // We can also assert that all mock servers have received only one RPC
      // call at this point of time.
      for (ScmTestMock mock : mockServers) {
        Assert.assertEquals(1, mock.getRpcCount());
      }

      // This task is the Running task, but running task executes tasks based
      // on the state of Endpoints, hence this next call will be a Register at
      // the endpoint RPC level.
      task = stateMachine.getContext().getTask();
      task.execute(executorService);
      newState = task.await(2, TimeUnit.SECONDS);

      // If we are in running state, we should be in running.
      Assert.assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);

      for (ScmTestMock mock : mockServers) {
        Assert.assertEquals(2, mock.getRpcCount());
      }

      // This task is the Running task, but running task executes tasks based
      // on the state of Endpoints, hence this next call will be a
      // HeartbeatTask at the endpoint RPC level.
      task = stateMachine.getContext().getTask();
      task.execute(executorService);
      newState = task.await(2, TimeUnit.SECONDS);

      // If we are in running state, we should be in running.
      Assert.assertEquals(DatanodeStateMachine.DatanodeStates.RUNNING,
          newState);


      for (ScmTestMock mock : mockServers) {
        Assert.assertEquals(1, mock.getHeartbeatCount());
        // Assert that heartbeat did indeed carry that State that we said
        // have in the datanode.
        Assert.assertEquals(mock.getReportState().getState().getNumber(),
            StorageContainerDatanodeProtocolProtos.ReportState.states
                .noContainerReports.getNumber());
      }
    }
  }

  /**
   * Test state transition with a list of invalid scm configurations,
   * and verify the state transits to SHUTDOWN each time.
   */
  @Test
  public void testDatanodeStateMachineWithInvalidConfiguration()
      throws Exception {
    LinkedList<Map.Entry<String, String>> confList =
        new LinkedList<Map.Entry<String, String>>();
    confList.add(Maps.immutableEntry(ScmConfigKeys.OZONE_SCM_NAMES, ""));

    // Invalid ozone.scm.names
    /** Empty **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, ""));
    /** Invalid schema **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "x..y"));
    /** Invalid port **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm:xyz"));
    /** Port out of range **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_NAMES, "scm:123456"));
    // Invalid ozone.scm.datanode.id
    /** Empty **/
    confList.add(Maps.immutableEntry(
        ScmConfigKeys.OZONE_SCM_DATANODE_ID, ""));

    confList.forEach((entry) -> {
      Configuration perTestConf = new Configuration(conf);
      perTestConf.setStrings(entry.getKey(), entry.getValue());
      LOG.info("Test with {} = {}", entry.getKey(), entry.getValue());
      try (DatanodeStateMachine stateMachine = new DatanodeStateMachine(
          DFSTestUtil.getLocalDatanodeID(), perTestConf)) {
        DatanodeStateMachine.DatanodeStates currentState =
            stateMachine.getContext().getState();
        Assert.assertEquals(DatanodeStateMachine.DatanodeStates.INIT,
            currentState);
        DatanodeState<DatanodeStateMachine.DatanodeStates> task =
            stateMachine.getContext().getTask();
        task.execute(executorService);
        DatanodeStateMachine.DatanodeStates newState =
            task.await(2, TimeUnit.SECONDS);
        Assert.assertEquals(DatanodeStateMachine.DatanodeStates.SHUTDOWN,
            newState);
      } catch (Exception e) {
        Assert.fail("Unexpected exception found");
      }
    });
  }
}
