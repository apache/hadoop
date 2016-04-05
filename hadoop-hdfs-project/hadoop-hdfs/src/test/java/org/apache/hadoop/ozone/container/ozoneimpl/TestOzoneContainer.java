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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.impl.ContainerManagerImpl;
import org.apache.hadoop.ozone.container.common.transport.client.XceiverClient;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class TestOzoneContainer {
  @Test
  public void testCreateOzoneContainer() throws Exception {
    String containerName = OzoneUtils.getRequestID();
    Configuration conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);

    // We don't start Ozone Container via data node, we will do it
    // independently in our test path.
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, false);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();


    Pipeline pipeline = ContainerTestHelper.createSingleNodePipeline
        (containerName);
    conf.setInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());
    OzoneContainer container = new OzoneContainer(conf, cluster.getDataNodes
        ().get(0).getFSDataset());
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
    Configuration conf = new OzoneConfiguration();
    URL p = conf.getClass().getResource("");
    String path = p.getPath().concat(
        TestOzoneContainer.class.getSimpleName());
    path += conf.getTrimmed(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT,
        OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.DFS_STORAGE_LOCAL_ROOT, path);

    // Start ozone container Via Datanode create.
    conf.setBoolean(OzoneConfigKeys.DFS_OBJECTSTORE_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.DFS_STORAGE_HANDLER_TYPE_KEY, "local");

    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline(containerName);
    conf.setInt(OzoneConfigKeys.DFS_OZONE_CONTAINER_IPC_PORT,
        pipeline.getLeader().getContainerPort());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();

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

}
