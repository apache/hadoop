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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;

/**
 * Tests DeleteContainerCommand Handler.
 */
public class TestDeleteContainerHandler {


  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      try {
        cluster.shutdown();
      } catch (Exception e) {
        // do nothing.
      }
    }
  }

  @Test(timeout = 60000)
  public void testDeleteContainerRequestHandler() throws Exception {

    //the easiest way to create an open container is creating a key
    OzoneClient client = OzoneClientFactory.getClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("test");
    objectStore.getVolume("test").createBucket("test");
    OzoneOutputStream key = objectStore.getVolume("test").getBucket("test")
        .createKey("test", 1024, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>());
    key.write("test".getBytes());
    key.close();

    //get the name of a valid container
    OmKeyArgs keyArgs =
        new OmKeyArgs.Builder().setVolumeName("test").setBucketName("test")
            .setType(HddsProtos.ReplicationType.RATIS)
            .setFactor(HddsProtos.ReplicationFactor.THREE).setDataSize(1024)
            .setKeyName("test").build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    ContainerID containerId = ContainerID.valueof(
        omKeyLocationInfo.getContainerID());
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(containerId);
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());

    // Take first node from the datanode list, and close the container (As we
    // have only three, the container will be created on three nodes)
    // We need to close the container because delete container only happens
    // on closed containers.

    HddsDatanodeService hddsDatanodeService =
        cluster.getHddsDatanodes().get(0);

    Assert.assertFalse(isContainerClosed(hddsDatanodeService,
        containerId.getId()));

    DatanodeDetails datanodeDetails = hddsDatanodeService.getDatanodeDetails();

    //send the order to close the container
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getUuid(),
            new CloseContainerCommand(containerId.getId(), pipeline.getId()));

    GenericTestUtils.waitFor(() ->
            isContainerClosed(hddsDatanodeService, containerId.getId()),
        500, 5 * 1000);

    //double check if it's really closed (waitFor also throws an exception)
    Assert.assertTrue(isContainerClosed(hddsDatanodeService,
        containerId.getId()));

    // Check container exists before sending delete container command
    Assert.assertFalse(isContainerDeleted(hddsDatanodeService,
        containerId.getId()));

    // send delete container to one of the datanode
    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails.getUuid(),
            new DeleteContainerCommand(containerId.getId()));

    GenericTestUtils.waitFor(() ->
            isContainerDeleted(hddsDatanodeService, containerId.getId()),
        500, 5 * 1000);

    // On another node, where container is open try to delete container
    HddsDatanodeService hddsDatanodeService1 =
        cluster.getHddsDatanodes().get(1);
    DatanodeDetails datanodeDetails1 =
        hddsDatanodeService1.getDatanodeDetails();

    cluster.getStorageContainerManager().getScmNodeManager()
        .addDatanodeCommand(datanodeDetails1.getUuid(),
            new DeleteContainerCommand(containerId.getId()));

    // Here it should not delete it, and the container should exist in the
    // containerset

    int count = 1;
    // Checking for 10 seconds, whether it is containerSet, as after command
    // is issued, giving some time for it to process.
    while (!isContainerDeleted(hddsDatanodeService1, containerId.getId())) {
      Thread.sleep(1000);
      count++;
      if (count == 10) {
        break;
      }
    }

    Assert.assertFalse(isContainerDeleted(hddsDatanodeService1,
        containerId.getId()));

  }

  /**
   * Checks whether is closed or not on a datanode.
   * @param hddsDatanodeService
   * @param containerID
   * @return true - if container is closes, else returns false.
   */
  private Boolean isContainerClosed(HddsDatanodeService hddsDatanodeService,
      long containerID) {
    ContainerData containerData;
    containerData =hddsDatanodeService
        .getDatanodeStateMachine().getContainer().getContainerSet()
        .getContainer(containerID).getContainerData();
    return !containerData.isOpen();
  }

  /**
   * Checks whether container is deleted from the datanode or not.
   * @param hddsDatanodeService
   * @param containerID
   * @return true - if container is deleted, else returns false
   */
  private Boolean isContainerDeleted(HddsDatanodeService hddsDatanodeService,
      long containerID) {
    Container container;
    // if container is not in container set, it means container got deleted.
    container = hddsDatanodeService
        .getDatanodeStateMachine().getContainer().getContainerSet()
        .getContainer(containerID);
    return container == null;
  }
}
