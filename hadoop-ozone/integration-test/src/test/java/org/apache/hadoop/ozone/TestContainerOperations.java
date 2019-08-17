/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.ContainerPlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.client.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This class tests container operations (TODO currently only supports create)
 * from cblock clients.
 */
public class TestContainerOperations {

  private static ScmClient storageClient;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;

  @BeforeClass
  public static void setup() throws Exception {
    int containerSizeGB = 5;
    ContainerOperationClient.setContainerSizeB(
        containerSizeGB * OzoneConsts.GB);
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, ContainerPlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(1).build();
    StorageContainerLocationProtocolClientSideTranslatorPB client =
        cluster.getStorageContainerLocationClient();
    RPC.setProtocolEngine(ozoneConf, StorageContainerLocationProtocolPB.class,
        ProtobufRpcEngine.class);
    storageClient = new ContainerOperationClient(
        client, new XceiverClientManager(ozoneConf));
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if(cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * A simple test to create a container with {@link ContainerOperationClient}.
   * @throws Exception
   */
  @Test
  public void testCreate() throws Exception {
    ContainerWithPipeline container = storageClient.createContainer(HddsProtos
        .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
        .ONE, "OZONE");
    assertEquals(container.getContainerInfo().getContainerID(), storageClient
        .getContainer(container.getContainerInfo().getContainerID())
        .getContainerID());
  }

}
