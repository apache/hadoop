/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.XceiverClient;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URL;

/**
 * Tests ozone containers.
 */
public class TestOzoneContainer {
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300000);

  @Test
  public void testCreateOzoneContainer() throws Exception {
    String containerName = OzoneUtils.getRequestID();
    OzoneConfiguration conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);

    MiniOzoneCluster cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType("local").build();

    // We don't start Ozone Container via data node, we will do it
    // independently in our test path.
    Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline(
        containerName);
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());
    OzoneContainer container = new OzoneContainer(conf);
    container.start();

    XceiverClient client = new XceiverClient(pipeline, conf);
    client.connect();
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(containerName);
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
    container.stop();
    cluster.shutdown();

  }

  @Test
  public void testOzoneContainerViaDataNode() throws Exception {
    String keyName = OzoneUtils.getRequestID();
    String containerName = OzoneUtils.getRequestID();
    OzoneConfiguration conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);

    // Start ozone container Via Datanode create.

    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline(containerName);
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());

    MiniOzoneCluster cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType("local").build();

    // This client talks to ozone container via datanode.
    XceiverClient client = new XceiverClient(pipeline, conf);
    client.connect();

    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(containerName);
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    // Write Chunk
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper.getWriteChunkRequest(pipeline, containerName,
            keyName, 1024);

    response = client.sendCommand(writeChunkRequest);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    // Read Chunk
    request = ContainerTestHelper.getReadChunkRequest(writeChunkRequest
        .getWriteChunk());

    response = client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    // Put Key
    ContainerProtos.ContainerCommandRequestProto putKeyRequest =
        ContainerTestHelper.getPutKeyRequest(writeChunkRequest.getWriteChunk());

    response = client.sendCommand(putKeyRequest);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    // Get Key
    request = ContainerTestHelper.getKeyRequest(putKeyRequest.getPutKey());
    response = client.sendCommand(request);
    ContainerTestHelper.verifyGetKey(request, response);


    // Delete Key
    request =
        ContainerTestHelper.getDeleteKeyRequest(putKeyRequest.getPutKey());
    response = client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    //Delete Chunk
    request = ContainerTestHelper.getDeleteChunkRequest(writeChunkRequest
        .getWriteChunk());

    response = client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    client.close();
    cluster.shutdown();

  }

  @Test
  public void testBothGetandPutSmallFile() throws Exception {
    String keyName = OzoneUtils.getRequestID();
    String containerName = OzoneUtils.getRequestID();
    OzoneConfiguration conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);

    // Start ozone container Via Datanode create.

    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline(containerName);
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());

    MiniOzoneCluster cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType("local").build();

    // This client talks to ozone container via datanode.
    XceiverClient client = new XceiverClient(pipeline, conf);
    client.connect();

    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(containerName);
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));


    ContainerProtos.ContainerCommandRequestProto smallFileRequest =
        ContainerTestHelper.getWriteSmallFileRequest(pipeline, containerName,
            keyName, 1024);


    response = client.sendCommand(smallFileRequest);
    Assert.assertNotNull(response);
    Assert.assertTrue(smallFileRequest.getTraceID()
        .equals(response.getTraceID()));

    ContainerProtos.ContainerCommandRequestProto getSmallFileRequest =
        ContainerTestHelper.getReadSmallFileRequest(smallFileRequest
            .getPutSmallFile().getKey());
    response = client.sendCommand(getSmallFileRequest);
    Assert.assertArrayEquals(
        smallFileRequest.getPutSmallFile().getData().toByteArray(),
        response.getGetSmallFile().getData().getData().toByteArray());

    cluster.shutdown();


  }

}
