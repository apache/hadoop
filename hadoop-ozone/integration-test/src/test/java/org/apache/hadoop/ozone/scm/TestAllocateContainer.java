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

import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test allocate container calls.
 */
public class TestAllocateContainer {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static XceiverClientManager xceiverClientManager;
  private static String containerOwner = "OZONE";
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    xceiverClientManager = new XceiverClientManager(conf);
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if(cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testAllocate() throws Exception {
    ContainerWithPipeline container =
        storageContainerLocationClient.allocateContainer(
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            containerOwner);
    Assert.assertNotNull(container);
    Assert.assertNotNull(container.getPipeline().getFirstNode());

  }

  @Test
  public void testAllocateNull() throws Exception {
    thrown.expect(NullPointerException.class);
    storageContainerLocationClient.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), null);
  }
}
