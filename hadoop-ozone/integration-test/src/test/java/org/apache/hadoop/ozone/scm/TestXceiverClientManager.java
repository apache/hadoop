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
package org.apache.hadoop.ozone.scm;

import com.google.common.cache.Cache;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.apache.hadoop.scm
    .ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_KEY;

/**
 * Test for XceiverClientManager caching and eviction.
 */
public class TestXceiverClientManager {
  private static OzoneConfiguration config;
  private static MiniOzoneCluster cluster;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String containerOwner = "OZONE";

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
    config = new OzoneConfiguration();
    cluster = new MiniOzoneClassicCluster.Builder(config)
        .numDataNodes(3)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient = cluster
        .createStorageContainerLocationClient();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanupWithLogger(null, cluster, storageContainerLocationClient);
  }

  @Test
  public void testCaching() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    XceiverClientManager clientManager = new XceiverClientManager(conf);

    String containerName1 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline1 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerName1, containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(pipeline1);
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertEquals(containerName1,
        client1.getPipeline().getContainerName());

    String containerName2 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerName2, containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(pipeline2);
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertEquals(containerName2,
        client2.getPipeline().getContainerName());

    XceiverClientSpi client3 = clientManager.acquireClient(pipeline1);
    Assert.assertEquals(2, client3.getRefcount());
    Assert.assertEquals(2, client1.getRefcount());
    Assert.assertEquals(containerName1,
        client3.getPipeline().getContainerName());
    Assert.assertEquals(client1, client3);
    clientManager.releaseClient(client1);
    clientManager.releaseClient(client2);
    clientManager.releaseClient(client3);
  }

  @Test
  public void testFreeByReference() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();

    String containerName1 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(), HdslProtos.ReplicationFactor.ONE,
            containerName1, containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(pipeline1);
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertEquals(containerName1,
        client1.getPipeline().getContainerName());

    String containerName2 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline2 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            HdslProtos.ReplicationFactor.ONE, containerName2, containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(pipeline2);
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertEquals(containerName2,
        client2.getPipeline().getContainerName());
    Assert.assertNotEquals(client1, client2);

    // least recent container (i.e containerName1) is evicted
    XceiverClientSpi nonExistent1 = cache.getIfPresent(containerName1);
    Assert.assertEquals(null, nonExistent1);
    // However container call should succeed because of refcount on the client.
    String traceID1 = "trace" + RandomStringUtils.randomNumeric(4);
    ContainerProtocolCalls.createContainer(client1,  traceID1);

    // After releasing the client, this connection should be closed
    // and any container operations should fail
    clientManager.releaseClient(client1);
    exception.expect(IOException.class);
    exception.expectMessage("This channel is not connected.");
    ContainerProtocolCalls.createContainer(client1,  traceID1);
    clientManager.releaseClient(client2);
  }

  @Test
  public void testFreeByEviction() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();

    String containerName1 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            clientManager.getFactor(), containerName1, containerOwner);
    XceiverClientSpi client1 = clientManager.acquireClient(pipeline1);
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertEquals(containerName1,
        client1.getPipeline().getContainerName());

    clientManager.releaseClient(client1);
    Assert.assertEquals(0, client1.getRefcount());

    String containerName2 = "container" + RandomStringUtils.randomNumeric(10);
    Pipeline pipeline2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerName2, containerOwner);
    XceiverClientSpi client2 = clientManager.acquireClient(pipeline2);
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertEquals(containerName2,
        client2.getPipeline().getContainerName());
    Assert.assertNotEquals(client1, client2);


    // now client 1 should be evicted
    XceiverClientSpi nonExistent = cache.getIfPresent(containerName1);
    Assert.assertEquals(null, nonExistent);

    // Any container operation should now fail
    String traceID2 = "trace" + RandomStringUtils.randomNumeric(4);
    exception.expect(IOException.class);
    exception.expectMessage("This channel is not connected.");
    ContainerProtocolCalls.createContainer(client1, traceID2);
    clientManager.releaseClient(client2);
  }
}
