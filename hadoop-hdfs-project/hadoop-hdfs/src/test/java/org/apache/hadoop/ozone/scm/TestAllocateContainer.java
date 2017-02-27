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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.protocolPB
    .StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Test allocate container calls.
 */
public class TestAllocateContainer {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void init() throws IOException {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(1)
        .setHandlerType("distributed").build();
  }

  @AfterClass
  public static void shutdown() throws InterruptedException {
    if(cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanup(null, storageContainerLocationClient, cluster);
  }

  @Test
  public void testAllocate() throws Exception {
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    Pipeline pipeline = storageContainerLocationClient.allocateContainer(
        "container0");
    Assert.assertNotNull(pipeline);
    Assert.assertNotNull(pipeline.getLeader());

  }

  @Test
  public void testAllocateNull() throws Exception {
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    thrown.expect(NullPointerException.class);
    storageContainerLocationClient.allocateContainer(null);
  }

  @Test
  public void testAllocateDuplicate() throws Exception {
    String containerName = RandomStringUtils.randomAlphanumeric(10);
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    thrown.expect(IOException.class);
    thrown.expectMessage("Specified container already exists");
    storageContainerLocationClient.allocateContainer(containerName);
    storageContainerLocationClient.allocateContainer(containerName);

  }
}
