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

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.
    HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
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
  private static int chunkSize;

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
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, 10,
        TimeUnit.SECONDS);
    conf.setQuietMode(false);
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).setHbInterval(200)
            .build();
    cluster.waitForClusterToBeReady();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getClient(conf);
    objectStore = client.getObjectStore();
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
    long written = 0;
    // First write and flush creates a container in the datanode
    key.write(testData);
    written += testData.length;
    key.flush();
    key.write(testData);
    written += testData.length;

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName("ratis")
        .build();
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
    key.close();
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

    OzoneKeyDetails keyDetails = objectStore.getVolume(volumeName)
        .getBucket(bucketName).getKey("ratis");

    /**
     * Ensure length of data stored in key is equal to number of bytes written.
     */
    Assert.assertTrue("Number of bytes stored in the key is not equal " +
        "to number of bytes written.", keyDetails.getDataSize() == written);

    /**
     * Pending data from the second write should get written to a new container
     * during key.close() because the first container is UNHEALTHY by that time
     */
    Assert.assertTrue("Expect Key to be stored in 2 separate containers",
        keyDetails.getOzoneKeyLocations().size() == 2);
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

    //get the name of a valid container
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName).
        setBucketName(bucketName).setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE).setKeyName("ratis")
        .build();
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

    key.close();

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
}