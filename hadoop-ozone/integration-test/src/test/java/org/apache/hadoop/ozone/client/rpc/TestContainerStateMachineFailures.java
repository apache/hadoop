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

package org.apache.hadoop.ozone.client.rpc;

import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys
    .HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.
    ContainerDataProto.State.UNHEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.
    OZONE_SCM_PIPELINE_DESTROY_TIMEOUT;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the containerStateMachine failure handling.
 */

public class TestContainerStateMachineFailures {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static OzoneClient client;
  private static ObjectStore objectStore;
  private static String volumeName;
  private static String bucketName;
  private static String path;
  private static XceiverClientManager xceiverClientManager;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    path = GenericTestUtils
        .getTempPath(TestContainerStateMachineFailures.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();


    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 10,
        TimeUnit.SECONDS);
    conf.setInt(OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_MAX_RETRIES_KEY, 10);
    conf.setTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_RETRY_INTERVAL_KEY,
        1, TimeUnit.SECONDS);
    conf.setLong(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);
    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).setHbInterval(200)
            .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
    xceiverClientManager = new XceiverClientManager(conf);
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerStateMachineFailures() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    byte[] testData = "ratis".getBytes();
    // First write and flush creates a container in the datanode
    key.write(testData);
    key.flush();
    key.write(testData);
    KeyOutputStream groupOutputStream =
        (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    // delete the container dir
    FileUtil.fullyDelete(new File(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID()).getContainerData()
            .getContainerPath()));
    try {
      // there is only 1 datanode in the pipeline, the pipeline will be closed
      // and allocation to new pipeline will fail as there is no other dn in
      // the cluster
      key.close();
    } catch(IOException ioe) {
      Assert.assertTrue(ioe instanceof OMException);
    }
    long containerID = omKeyLocationInfo.getContainerID();

    // Make sure the container is marked unhealthy
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(containerID)
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    OzoneContainer ozoneContainer = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer();
    // make sure the missing containerSet is empty
    HddsDispatcher dispatcher = (HddsDispatcher) ozoneContainer.getDispatcher();
    Assert.assertTrue(dispatcher.getMissingContainerSet().isEmpty());

    // restart the hdds datanode, container should not in the regular set
    cluster.restartHddsDatanode(0, true);
    ozoneContainer = cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContainer();
    Assert
        .assertNull(ozoneContainer.getContainerSet().getContainer(containerID));
  }

  @Test
  public void testUnhealthyContainer() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    ContainerData containerData =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    KeyValueContainerData keyValueContainerData =
        (KeyValueContainerData) containerData;
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getChunksPath()));
    try {
      // there is only 1 datanode in the pipeline, the pipeline will be closed
      // and allocation to new pipeline will fail as there is no other dn in
      // the cluster
      key.close();
    } catch(IOException ioe) {
      Assert.assertTrue(ioe instanceof OMException);
    }

    long containerID = omKeyLocationInfo.getContainerID();

    // Make sure the container is marked unhealthy
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    // Check metadata in the .container file
    File containerFile = new File(keyValueContainerData.getMetadataPath(),
        containerID + OzoneConsts.CONTAINER_EXTENSION);

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    // restart the hdds datanode and see if the container is listed in the
    // in the missing container set and not in the regular set
    cluster.restartHddsDatanode(0, true);
    // make sure the container state is still marked unhealthy after restart
    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    OzoneContainer ozoneContainer;
    ozoneContainer = cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
        .getContainer();
    HddsDispatcher dispatcher = (HddsDispatcher) ozoneContainer.getDispatcher();
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(
        cluster.getHddsDatanodes().get(0).getDatanodeDetails().getUuidString());
    Assert.assertEquals(ContainerProtos.Result.CONTAINER_UNHEALTHY,
        dispatcher.dispatch(request.build(), null).getResult());
  }

  @Test
  public void testApplyTransactionFailure() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    ContainerData containerData =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    KeyValueContainerData keyValueContainerData =
        (KeyValueContainerData) containerData;
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) ContainerTestHelper.getStateMachine(cluster);
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    Assert.assertTrue(parentPath.getParent().toFile().listFiles().length > 0);
    FileInfo snapshot = storage.findLatestSnapshot().getFile();
    Assert.assertNotNull(snapshot);
    long containerID = omKeyLocationInfo.getContainerID();
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getContainerPath()));
    Pipeline pipeline = cluster.getStorageContainerLocationClient()
        .getContainerWithPipeline(containerID).getPipeline();
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    // close container transaction will fail over Ratis and will initiate
    // a pipeline close action

    // Since the applyTransaction failure is propagated to Ratis,
    // stateMachineUpdater will it exception while taking the next snapshot
    // and should shutdown the RaftServerImpl. The client request will fail
    // with RaftRetryFailureException.
    try {
      xceiverClient.sendCommand(request.build());
      Assert.fail("Expected exception not thrown");
    } catch (IOException e) {
      Assert.assertTrue(HddsClientUtils
          .checkForException(e) instanceof RaftRetryFailureException);
    }
    // Make sure the container is marked unhealthy
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
    try {
      // try to take a new snapshot, ideally it should just fail
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      Assert.assertTrue(ioe instanceof StateMachineException);
    }
    // Make sure the latest snapshot is same as the previous one
    FileInfo latestSnapshot = storage.findLatestSnapshot().getFile();
    Assert.assertTrue(snapshot.getPath().equals(latestSnapshot.getPath()));
  }

  @Test
  public void testApplyTransactionIdempotencyWithClosedContainer()
      throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    ContainerData containerData =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    key.close();
    ContainerStateMachine stateMachine =
        (ContainerStateMachine) ContainerTestHelper.getStateMachine(cluster);
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    // Since the snapshot threshold is set to 1, since there are
    // applyTransactions, we should see snapshots
    Assert.assertTrue(parentPath.getParent().toFile().listFiles().length > 0);
    FileInfo snapshot = storage.findLatestSnapshot().getFile();
    Assert.assertNotNull(snapshot);
    long containerID = omKeyLocationInfo.getContainerID();
    Pipeline pipeline = cluster.getStorageContainerLocationClient()
        .getContainerWithPipeline(containerID).getPipeline();
    XceiverClientSpi xceiverClient =
        xceiverClientManager.acquireClient(pipeline);
    ContainerProtos.ContainerCommandRequestProto.Builder request =
        ContainerProtos.ContainerCommandRequestProto.newBuilder();
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    request.setCmdType(ContainerProtos.Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(
        ContainerProtos.CloseContainerRequestProto.getDefaultInstance());
    try {
      xceiverClient.sendCommand(request.build());
    } catch (IOException e) {
      Assert.fail("Exception should not be thrown");
    }
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.CLOSED);
    Assert.assertTrue(stateMachine.isStateMachineHealthy());
    try {
      stateMachine.takeSnapshot();
    } catch (IOException ioe) {
      Assert.fail("Exception should not be thrown");
    }
    FileInfo latestSnapshot = storage.findLatestSnapshot().getFile();
    Assert.assertFalse(snapshot.getPath().equals(latestSnapshot.getPath()));
  }

  @Test
  public void testValidateBCSIDOnDnRestart() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes());
    key.flush();
    key.write("ratis".getBytes());
    KeyOutputStream groupOutputStream = (KeyOutputStream) key.getOutputStream();
    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
    ContainerData containerData =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    KeyValueContainerData keyValueContainerData =
        (KeyValueContainerData) containerData;
    key.close();

    long containerID = omKeyLocationInfo.getContainerID();
    cluster.shutdownHddsDatanode(
        cluster.getHddsDatanodes().get(0).getDatanodeDetails());
    // delete the container db file
    FileUtil.fullyDelete(new File(keyValueContainerData.getContainerPath()));
    cluster.restartHddsDatanode(
        cluster.getHddsDatanodes().get(0).getDatanodeDetails(), true);
    OzoneContainer ozoneContainer =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer();
    // make sure the missing containerSet is not empty
    HddsDispatcher dispatcher = (HddsDispatcher) ozoneContainer.getDispatcher();
    Assert.assertTrue(!dispatcher.getMissingContainerSet().isEmpty());
    Assert
        .assertTrue(dispatcher.getMissingContainerSet().contains(containerID));

    // write a new key
    key = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey("ratis", 1024, ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis1".getBytes());
    key.flush();
    groupOutputStream = (KeyOutputStream) key.getOutputStream();
    locationInfoList = groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    omKeyLocationInfo = locationInfoList.get(0);
    key.close();
    containerID = omKeyLocationInfo.getContainerID();
    containerData = cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
        .getContainer().getContainerSet()
        .getContainer(omKeyLocationInfo.getContainerID()).getContainerData();
    Assert.assertTrue(containerData instanceof KeyValueContainerData);
    keyValueContainerData = (KeyValueContainerData) containerData;
    ReferenceCountedDB db = BlockUtils.
        getDB(keyValueContainerData, conf);
    byte[] blockCommitSequenceIdKey =
        DFSUtil.string2Bytes(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID_PREFIX);

    // modify the bcsid for the container in the ROCKS DB thereby inducing
    // corruption
    db.getStore().put(blockCommitSequenceIdKey, Longs.toByteArray(0));
    db.decrementReference();
    // shutdown of dn will take a snapsot which will persist the valid BCSID
    // recorded in the container2BCSIDMap in ContainerStateMachine
    cluster.shutdownHddsDatanode(
        cluster.getHddsDatanodes().get(0).getDatanodeDetails());
    // after the restart, there will be a mismatch in BCSID of what is recorded
    // in the and what is there in RockSDB and hence the container would be
    // marked unhealthy
    cluster.restartHddsDatanode(
        cluster.getHddsDatanodes().get(0).getDatanodeDetails(), true);
    // Make sure the container is marked unhealthy
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
  }
}