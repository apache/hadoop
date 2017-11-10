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

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.ozone.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.XceiverClientManager;
import org.apache.hadoop.scm.XceiverClientSpi;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.scm.storage.ContainerProtocolCalls;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

/**
 * Test Container calls.
 */
public class TestContainerSmallFile {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConfig;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;

  @BeforeClass
  public static void init() throws Exception {
    long datanodeCapacities = 3 * OzoneConsts.TB;
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    cluster = new MiniOzoneClassicCluster.Builder(ozoneConfig).numDataNodes(1)
        .storageCapacities(new long[] {datanodeCapacities, datanodeCapacities})
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageContainerLocationClient = cluster
        .createStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(ozoneConfig);
    cluster.waitForHeartbeatProcessed();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testAllocateWrite() throws Exception {
    String traceID = UUID.randomUUID().toString();
    String containerName = "container0";
    Pipeline pipeline =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, containerName);
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    ContainerProtocolCalls.createContainer(client, traceID);

    ContainerProtocolCalls.writeSmallFile(client, containerName,
        "key", "data123".getBytes(), traceID);
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, containerName, "key",
            traceID);
    String readData = response.getData().getData().toStringUtf8();
    Assert.assertEquals("data123", readData);
    xceiverClientManager.releaseClient(client);
  }

  @Test
  public void testInvalidKeyRead() throws Exception {
    String traceID = UUID.randomUUID().toString();
    String containerName = "container1";
    Pipeline pipeline =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, containerName);
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    ContainerProtocolCalls.createContainer(client, traceID);

    thrown.expect(StorageContainerException.class);
    thrown.expectMessage("Unable to find the key");

    // Try to read a Key Container Name
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, containerName, "key",
            traceID);
    xceiverClientManager.releaseClient(client);
  }

  @Test
  public void testInvalidContainerRead() throws Exception {
    String traceID = UUID.randomUUID().toString();
    String invalidName = "invalidName";
    String containerName = "container2";
    Pipeline pipeline =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            OzoneProtos.ReplicationFactor.ONE, containerName);
    XceiverClientSpi client = xceiverClientManager.acquireClient(pipeline);
    ContainerProtocolCalls.createContainer(client, traceID);
    ContainerProtocolCalls.writeSmallFile(client, containerName,
        "key", "data123".getBytes(), traceID);


    thrown.expect(StorageContainerException.class);
    thrown.expectMessage("Unable to find the container");

    // Try to read a invalid key
    ContainerProtos.GetSmallFileResponseProto response =
        ContainerProtocolCalls.readSmallFile(client, invalidName, "key",
            traceID);
    xceiverClientManager.releaseClient(client);
  }
}


