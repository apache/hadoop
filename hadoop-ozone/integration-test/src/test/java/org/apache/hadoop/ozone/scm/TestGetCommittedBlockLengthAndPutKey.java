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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.
    ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.container.common.helpers.
    StorageContainerException;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.
    ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.
    SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.protocolPB.
    StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import java.util.UUID;

/**
 * Test Container calls.
 */
public class TestGetCommittedBlockLengthAndPutKey {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;
  private static String containerOwner = "OZONE";

  @BeforeClass
  public static void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    cluster =
        MiniOzoneCluster.newBuilder(ozoneConfig).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void tesGetCommittedBlockLength() throws Exception {
    ContainerProtos.GetCommittedBlockLengthResponseProto response;
    String traceID = UUID.randomUUID().toString();
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    //create the container
    ContainerProtocolCalls.createContainer(client, containerID, traceID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data =
        RandomStringUtils.random(RandomUtils.nextInt(0, 1024)).getBytes();
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    client.sendCommand(writeChunkRequest);
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    client.sendCommand(putKeyRequest);
    response = ContainerProtocolCalls
        .getCommittedBlockLength(client, blockID, traceID);
    // make sure the block ids in the request and response are same.
    Assert.assertTrue(
        BlockID.getFromProtobuf(response.getBlockID()).equals(blockID));
    Assert.assertTrue(response.getBlockLength() == data.length);
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void testGetCommittedBlockLengthForInvalidBlock() throws Exception {
    String traceID = UUID.randomUUID().toString();
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(xceiverClientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    long containerID = container.getContainerInfo().getContainerID();
    XceiverClientSpi client = xceiverClientManager
        .acquireClient(container.getPipeline());
    ContainerProtocolCalls.createContainer(client, containerID, traceID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    // move the container to closed state
    ContainerProtocolCalls.closeContainer(client, containerID, traceID, null);
    try {
      // There is no block written inside the container. The request should
      // fail.
      ContainerProtocolCalls.getCommittedBlockLength(client, blockID, traceID);
      Assert.fail("Expected exception not thrown");
    } catch (StorageContainerException sce) {
      Assert.assertTrue(sce.getMessage().contains("Unable to find the block"));
    }
    xceiverClientManager.releaseClient(client, false);
  }

  @Test
  public void tesPutKeyResposne() throws Exception {
    ContainerProtos.PutBlockResponseProto response;
    String traceID = UUID.randomUUID().toString();
    ContainerWithPipeline container = storageContainerLocationClient
        .allocateContainer(HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    long containerID = container.getContainerInfo().getContainerID();
    Pipeline pipeline = container.getPipeline();
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    //create the container
    ContainerProtocolCalls.createContainer(client, containerID, traceID, null);

    BlockID blockID = ContainerTestHelper.getTestBlockID(containerID);
    byte[] data =
        RandomStringUtils.random(RandomUtils.nextInt(0, 1024)).getBytes();
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper
            .getWriteChunkRequest(container.getPipeline(), blockID,
                data.length);
    client.sendCommand(writeChunkRequest);
    // Now, explicitly make a putKey request for the block.
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper
            .getPutBlockRequest(pipeline, writeChunkRequest.getWriteChunk());
    response = client.sendCommand(putKeyRequest).getPutBlock();
    Assert.assertEquals(
        response.getCommittedBlockLength().getBlockLength(), data.length);
    Assert.assertTrue(response.getCommittedBlockLength().getBlockID()
        .getBlockCommitSequenceId() > 0);
    BlockID responseBlockID = BlockID
        .getFromProtobuf(response.getCommittedBlockLength().getBlockID());
    blockID
        .setBlockCommitSequenceId(responseBlockID.getBlockCommitSequenceId());
    // make sure the block ids in the request and response are same.
    // This will also ensure that closing the container committed the block
    // on the Datanodes.
    Assert.assertEquals(responseBlockID, blockID);
    xceiverClientManager.releaseClient(client, false);
  }
}