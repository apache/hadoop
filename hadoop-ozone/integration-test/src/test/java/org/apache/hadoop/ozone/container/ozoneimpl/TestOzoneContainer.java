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

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdsl.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.XceiverClient;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
    OzoneConfiguration conf = newOzoneConfiguration();
    OzoneContainer container = null;
    MiniOzoneCluster cluster = null;
    try {
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      // We don't start Ozone Container via data node, we will do it
      // independently in our test path.
      Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline(
          containerName);
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getLeader().getContainerPort());
      conf.setBoolean(OzoneConfigKeys.DFS_CONTAINER_IPC_RANDOM_PORT, false);
      container = new OzoneContainer(DFSTestUtil.getLocalDatanodeID(1),
          conf);
      container.start();

      XceiverClient client = new XceiverClient(pipeline, conf);
      client.connect();
      createContainerForTesting(client, containerName);
    } finally {
      if (container != null) {
        container.stop();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static OzoneConfiguration newOzoneConfiguration() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    ContainerTestHelper.setOzoneLocalStorageRoot(
        TestOzoneContainer.class, conf);
    return conf;
  }

  @Test
  public void testOzoneContainerViaDataNode() throws Exception {
    MiniOzoneCluster cluster = null;
    try {
      String containerName = OzoneUtils.getRequestID();
      OzoneConfiguration conf = newOzoneConfiguration();

      // Start ozone container Via Datanode create.

      Pipeline pipeline =
          ContainerTestHelper.createSingleNodePipeline(containerName);
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
          pipeline.getLeader().getContainerPort());

      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setRandomContainerPort(false)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();

      // This client talks to ozone container via datanode.
      XceiverClient client = new XceiverClient(pipeline, conf);

      runTestOzoneContainerViaDataNode(containerName, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void runTestOzoneContainerViaDataNode(
      String containerName, XceiverClientSpi client) throws Exception {
    ContainerProtos.ContainerCommandRequestProto
        request, writeChunkRequest, putKeyRequest,
        updateRequest1, updateRequest2;
    ContainerProtos.ContainerCommandResponseProto response,
        updateResponse1, updateResponse2;
    try {
      client.connect();

      // Create container
      createContainerForTesting(client, containerName);
      writeChunkRequest = writeChunkForContainer(client, containerName, 1024);

      // Read Chunk
      request = ContainerTestHelper.getReadChunkRequest(writeChunkRequest
          .getWriteChunk());

      response = client.sendCommand(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

      // Put Key
      putKeyRequest = ContainerTestHelper.getPutKeyRequest(writeChunkRequest
              .getWriteChunk());


      response = client.sendCommand(putKeyRequest);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert
          .assertTrue(putKeyRequest.getTraceID().equals(response.getTraceID()));

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

      //Update an existing container
      Map<String, String> containerUpdate = new HashMap<String, String>();
      containerUpdate.put("container_updated_key", "container_updated_value");
      updateRequest1 = ContainerTestHelper.getUpdateContainerRequest(
              containerName, containerUpdate);
      updateResponse1 = client.sendCommand(updateRequest1);
      Assert.assertNotNull(updateResponse1);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());

      //Update an non-existing container
      updateRequest2 = ContainerTestHelper.getUpdateContainerRequest(
              "non_exist_container", containerUpdate);
      updateResponse2 = client.sendCommand(updateRequest2);
      Assert.assertEquals(ContainerProtos.Result.CONTAINER_NOT_FOUND,
          updateResponse2.getResult());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testBothGetandPutSmallFile() throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClient client = null;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();

      client = createClientForTesting(conf);
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setRandomContainerPort(false)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      String containerName = client.getPipeline().getContainerName();

      runTestBothGetandPutSmallFile(containerName, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  static void runTestBothGetandPutSmallFile(
      String containerName, XceiverClientSpi client) throws Exception {
    try {
      client.connect();

      createContainerForTesting(client, containerName);

      String keyName = OzoneUtils.getRequestID();
      final ContainerProtos.ContainerCommandRequestProto smallFileRequest
          = ContainerTestHelper.getWriteSmallFileRequest(
          client.getPipeline(), containerName, keyName, 1024);
      ContainerProtos.ContainerCommandResponseProto response
          = client.sendCommand(smallFileRequest);
      Assert.assertNotNull(response);
      Assert.assertTrue(smallFileRequest.getTraceID()
          .equals(response.getTraceID()));

      final ContainerProtos.ContainerCommandRequestProto getSmallFileRequest
          = ContainerTestHelper.getReadSmallFileRequest(
          smallFileRequest.getPutSmallFile().getKey());
      response = client.sendCommand(getSmallFileRequest);
      Assert.assertArrayEquals(
          smallFileRequest.getPutSmallFile().getData().toByteArray(),
          response.getGetSmallFile().getData().getData().toByteArray());
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }



  @Test
  public void testCloseContainer() throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClient client = null;
    ContainerProtos.ContainerCommandResponseProto response;
    ContainerProtos.ContainerCommandRequestProto
        writeChunkRequest, putKeyRequest, request;
    try {

      OzoneConfiguration conf = newOzoneConfiguration();

      client = createClientForTesting(conf);
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setRandomContainerPort(false)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      client.connect();

      String containerName = client.getPipeline().getContainerName();
      createContainerForTesting(client, containerName);
      writeChunkRequest = writeChunkForContainer(client, containerName, 1024);


      putKeyRequest = ContainerTestHelper.getPutKeyRequest(writeChunkRequest
              .getWriteChunk());
      // Put key before closing.
      response = client.sendCommand(putKeyRequest);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());
      Assert.assertTrue(
          putKeyRequest.getTraceID().equals(response.getTraceID()));

      // Close the contianer.
      request = ContainerTestHelper.getCloseContainer(client.getPipeline());
      response = client.sendCommand(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));


      // Assert that none of the write  operations are working after close.

      // Write chunks should fail now.

      response = client.sendCommand(writeChunkRequest);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.CLOSED_CONTAINER_IO,
          response.getResult());
      Assert.assertTrue(
          writeChunkRequest.getTraceID().equals(response.getTraceID()));

      // Read chunk must work on a closed container.
      request = ContainerTestHelper.getReadChunkRequest(writeChunkRequest
          .getWriteChunk());
      response = client.sendCommand(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));


      // Put key will fail on a closed container.
      response = client.sendCommand(putKeyRequest);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.CLOSED_CONTAINER_IO,
          response.getResult());
      Assert
          .assertTrue(putKeyRequest.getTraceID().equals(response.getTraceID()));

      // Get key must work on the closed container.
      request = ContainerTestHelper.getKeyRequest(putKeyRequest.getPutKey());
      response = client.sendCommand(request);
      ContainerTestHelper.verifyGetKey(request, response);

      // Delete Key must fail on a closed container.
      request =
          ContainerTestHelper.getDeleteKeyRequest(putKeyRequest.getPutKey());
      response = client.sendCommand(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.CLOSED_CONTAINER_IO,
          response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
    } finally {
      if (client != null) {
        client.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClient client = null;
    ContainerProtos.ContainerCommandResponseProto response;
    ContainerProtos.ContainerCommandRequestProto request,
        writeChunkRequest, putKeyRequest;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();

      client = createClientForTesting(conf);
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setRandomContainerPort(false)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      client.connect();

      String containerName = client.getPipeline().getContainerName();
      createContainerForTesting(client, containerName);
      writeChunkRequest = writeChunkForContainer(client, containerName, 1024);

      putKeyRequest = ContainerTestHelper.getPutKeyRequest(writeChunkRequest
          .getWriteChunk());
      // Put key before deleting.
      response = client.sendCommand(putKeyRequest);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());
      Assert.assertTrue(
          putKeyRequest.getTraceID().equals(response.getTraceID()));

      // Container cannot be deleted forcibly because
      // the container is not closed.
      request = ContainerTestHelper.getDeleteContainer(
          client.getPipeline(), true);
      response = client.sendCommand(request);

      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.UNCLOSED_CONTAINER_IO,
          response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

      // Close the container.
      request = ContainerTestHelper.getCloseContainer(client.getPipeline());
      response = client.sendCommand(request);
      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

      // Container cannot be deleted because the container is not empty.
      request = ContainerTestHelper.getDeleteContainer(
          client.getPipeline(), false);
      response = client.sendCommand(request);

      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.ERROR_CONTAINER_NOT_EMPTY,
          response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

      // Container can be deleted forcibly because
      // it is closed and non-empty.
      request = ContainerTestHelper.getDeleteContainer(
          client.getPipeline(), true);
      response = client.sendCommand(request);

      Assert.assertNotNull(response);
      Assert.assertEquals(ContainerProtos.Result.SUCCESS,
          response.getResult());
      Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));

    } finally {
      if (client != null) {
        client.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  // Runs a set of commands as Async calls and verifies that calls indeed worked
  // as expected.
  static void runAsyncTests(
      String containerName, XceiverClientSpi client) throws Exception {
    try {
      client.connect();

      createContainerForTesting(client, containerName);
      final List<CompletableFuture> computeResults = new LinkedList<>();
      int requestCount = 1000;
      // Create a bunch of Async calls from this test.
      for(int x = 0; x <requestCount; x++) {
        String keyName = OzoneUtils.getRequestID();
        final ContainerProtos.ContainerCommandRequestProto smallFileRequest
            = ContainerTestHelper.getWriteSmallFileRequest(
            client.getPipeline(), containerName, keyName, 1024);

        CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
            response = client.sendCommandAsync(smallFileRequest);
        computeResults.add(response);
      }

      CompletableFuture<Void> combinedFuture =
          CompletableFuture.allOf(computeResults.toArray(
              new CompletableFuture[computeResults.size()]));
      // Wait for all futures to complete.
      combinedFuture.get();
      // Assert that all futures are indeed done.
      for (CompletableFuture future : computeResults) {
        Assert.assertTrue(future.isDone());
      }
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testXcieverClientAsync() throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClient client = null;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();

      client = createClientForTesting(conf);
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setRandomContainerPort(false)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      String containerName = client.getPipeline().getContainerName();
      runAsyncTests(containerName, client);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testInvalidRequest() throws Exception {
    MiniOzoneCluster cluster = null;
    XceiverClient client;
    ContainerProtos.ContainerCommandRequestProto request;
    try {
      OzoneConfiguration conf = newOzoneConfiguration();

      client = createClientForTesting(conf);
      cluster = new MiniOzoneClassicCluster.Builder(conf)
              .setRandomContainerPort(false)
              .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
      client.connect();

      // Send a request without traceId.
      request = ContainerTestHelper
          .getRequestWithoutTraceId(client.getPipeline());
      client.sendCommand(request);
      Assert.fail("IllegalArgumentException expected");
    } catch(IllegalArgumentException iae){
      GenericTestUtils.assertExceptionContains("Invalid trace ID", iae);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }


  private static XceiverClient createClientForTesting(OzoneConfiguration conf)
      throws Exception {
    String containerName = OzoneUtils.getRequestID();
    // Start ozone container Via Datanode create.
    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline(containerName);
    conf.setInt(OzoneConfigKeys.DFS_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());

    // This client talks to ozone container via datanode.
    return new XceiverClient(pipeline, conf);
  }

  private static void createContainerForTesting(XceiverClientSpi client,
      String containerName) throws Exception {
    // Create container
    ContainerProtos.ContainerCommandRequestProto request =
        ContainerTestHelper.getCreateContainerRequest(containerName,
            client.getPipeline());
    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(request);
    Assert.assertNotNull(response);
    Assert.assertTrue(request.getTraceID().equals(response.getTraceID()));
  }

  private static ContainerProtos.ContainerCommandRequestProto
      writeChunkForContainer(XceiverClientSpi client,
      String containerName, int dataLen) throws Exception {
    // Write Chunk
    final String keyName = OzoneUtils.getRequestID();
    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerTestHelper.getWriteChunkRequest(client.getPipeline(),
            containerName, keyName, dataLen);

    ContainerProtos.ContainerCommandResponseProto response =
        client.sendCommand(writeChunkRequest);
    Assert.assertNotNull(response);
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertTrue(response.getTraceID().equals(response.getTraceID()));
    return writeChunkRequest;
  }

  static void runRequestWithoutTraceId(
          String containerName, XceiverClientSpi client) throws Exception {
    try {
      client.connect();

      createContainerForTesting(client, containerName);

      String keyName = OzoneUtils.getRequestID();
      final ContainerProtos.ContainerCommandRequestProto smallFileRequest
              = ContainerTestHelper.getWriteSmallFileRequest(
              client.getPipeline(), containerName, keyName, 1024);

      ContainerProtos.ContainerCommandResponseProto response
              = client.sendCommand(smallFileRequest);
      Assert.assertNotNull(response);
      Assert.assertTrue(smallFileRequest.getTraceID()
              .equals(response.getTraceID()));


    } finally {
      if (client != null) {
        client.close();
      }
    }
  }

}
