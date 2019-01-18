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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.io.IOException;
import java.util.UUID;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.apache.hadoop.hdds.scm
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

  @Before
  public void init() throws Exception {
    config = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(config)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
  }

  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testCaching() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);

    XceiverClientManager clientManager = new XceiverClientManager(conf);

    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client1 = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, client1.getRefcount());

    ContainerWithPipeline container2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client2 = clientManager
        .acquireClient(container2.getPipeline());
    Assert.assertEquals(1, client2.getRefcount());

    XceiverClientSpi client3 = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(2, client3.getRefcount());
    Assert.assertEquals(2, client1.getRefcount());
    Assert.assertEquals(client1, client3);
    clientManager.releaseClient(client1, false);
    clientManager.releaseClient(client2, false);
    clientManager.releaseClient(client3, false);
  }

  @Test
  public void testFreeByReference() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();

    ContainerWithPipeline container1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(), HddsProtos.ReplicationFactor.ONE,
            containerOwner);
    XceiverClientSpi client1 = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertEquals(container1.getPipeline(),
        client1.getPipeline());

    ContainerWithPipeline container2 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            HddsProtos.ReplicationFactor.ONE, containerOwner);
    XceiverClientSpi client2 = clientManager
        .acquireClient(container2.getPipeline());
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertNotEquals(client1, client2);

    // least recent container (i.e containerName1) is evicted
    XceiverClientSpi nonExistent1 = cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType());
    Assert.assertEquals(null, nonExistent1);
    // However container call should succeed because of refcount on the client.
    String traceID1 = "trace" + RandomStringUtils.randomNumeric(4);
    ContainerProtocolCalls.createContainer(client1,
        container1.getContainerInfo().getContainerID(), traceID1, null);

    // After releasing the client, this connection should be closed
    // and any container operations should fail
    clientManager.releaseClient(client1, false);

    String expectedMessage = "This channel is not connected.";
    try {
      ContainerProtocolCalls.createContainer(client1,
          container1.getContainerInfo().getContainerID(), traceID1, null);
      Assert.fail("Create container should throw exception on closed"
          + "client");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass(), IOException.class);
      Assert.assertTrue(e.getMessage().contains(expectedMessage));
    }
    clientManager.releaseClient(client2, false);
  }

  @Test
  public void testFreeByEviction() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    String metaDir = GenericTestUtils.getTempPath(
        TestXceiverClientManager.class.getName() + UUID.randomUUID());
    conf.set(HDDS_METADATA_DIR_NAME, metaDir);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();

    ContainerWithPipeline container1 =
        storageContainerLocationClient.allocateContainer(
            clientManager.getType(),
            clientManager.getFactor(), containerOwner);
    XceiverClientSpi client1 = clientManager
        .acquireClient(container1.getPipeline());
    Assert.assertEquals(1, client1.getRefcount());

    clientManager.releaseClient(client1, false);
    Assert.assertEquals(0, client1.getRefcount());

    ContainerWithPipeline container2 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client2 = clientManager
        .acquireClient(container2.getPipeline());
    Assert.assertEquals(1, client2.getRefcount());
    Assert.assertNotEquals(client1, client2);

    // now client 1 should be evicted
    XceiverClientSpi nonExistent = cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType());
    Assert.assertEquals(null, nonExistent);

    // Any container operation should now fail
    String traceID2 = "trace" + RandomStringUtils.randomNumeric(4);
    String expectedMessage = "This channel is not connected.";
    try {
      ContainerProtocolCalls.createContainer(client1,
          container1.getContainerInfo().getContainerID(), traceID2, null);
      Assert.fail("Create container should throw exception on closed"
          + "client");
    } catch (Exception e) {
      Assert.assertEquals(e.getClass(), IOException.class);
      Assert.assertTrue(e.getMessage().contains(expectedMessage));
    }
    clientManager.releaseClient(client2, false);
  }

  @Test
  public void testFreeByRetryFailure() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(SCM_CONTAINER_CLIENT_MAX_SIZE_KEY, 1);
    XceiverClientManager clientManager = new XceiverClientManager(conf);
    Cache<String, XceiverClientSpi> cache =
        clientManager.getClientCache();

    // client is added in cache
    ContainerWithPipeline container1 = storageContainerLocationClient
        .allocateContainer(clientManager.getType(), clientManager.getFactor(),
            containerOwner);
    XceiverClientSpi client1 =
        clientManager.acquireClient(container1.getPipeline());
    clientManager.acquireClient(container1.getPipeline());
    Assert.assertEquals(2, client1.getRefcount());

    // client should be invalidated in the cache
    clientManager.releaseClient(client1, true);
    Assert.assertEquals(1, client1.getRefcount());
    Assert.assertNull(cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType()));

    // new client should be added in cache
    XceiverClientSpi client2 =
        clientManager.acquireClient(container1.getPipeline());
    Assert.assertNotEquals(client1, client2);
    Assert.assertEquals(1, client2.getRefcount());

    // on releasing the old client the entry in cache should not be invalidated
    clientManager.releaseClient(client1, true);
    Assert.assertEquals(0, client1.getRefcount());
    Assert.assertNotNull(cache.getIfPresent(
        container1.getContainerInfo().getPipelineID().getId().toString()
            + container1.getContainerInfo().getReplicationType()));
  }
}
