/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;

import org.apache.commons.lang.RandomStringUtils;

import org.apache.hadoop.hdsl.conf.OzoneConfiguration;
import org.apache.hadoop.hdsl.protocol.proto.HdslProtos;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneTestHelper;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.scm.StorageContainerManager;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.scm.container.common.helpers.StorageContainerException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class tests container report with DN container state info.
 */
public class TestContainerReportWithKeys {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestContainerReportWithKeys.class);
  private static MiniOzoneClassicCluster cluster = null;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    scm = cluster.getStorageContainerManager();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerReportKeyWrite() throws Exception {
    final String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    final String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    final String keyName = "key" + RandomStringUtils.randomNumeric(5);
    final int keySize = 100;

    OzoneClient client = OzoneClientFactory.getClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    OzoneOutputStream key = objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, keySize, ReplicationType.STAND_ALONE,
            ReplicationFactor.ONE);
    String dataString = RandomStringUtils.randomAlphabetic(keySize);
    key.write(dataString.getBytes());
    key.close();

    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setType(HdslProtos.ReplicationType.STAND_ALONE)
        .setFactor(HdslProtos.ReplicationFactor.ONE).setDataSize(keySize)
        .build();


    KsmKeyLocationInfo keyInfo =
        cluster.getKeySpaceManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    ContainerData cd = getContainerData(cluster, keyInfo.getContainerName());

    LOG.info("DN Container Data:  keyCount: {} used: {} ",
        cd.getKeyCount(), cd.getBytesUsed());

    ContainerInfo cinfo = scm.getContainerInfo(keyInfo.getContainerName());

    LOG.info("SCM Container Info keyCount: {} usedBytes: {}",
        cinfo.getNumberOfKeys(), cinfo.getUsedBytes());
  }


  private static ContainerData getContainerData(MiniOzoneClassicCluster clus,
      String containerName) {
    ContainerData containerData = null;
    try {
      ContainerManager containerManager = MiniOzoneTestHelper
          .getOzoneContainerManager(clus.getDataNodes().get(0));
      containerData = containerManager.readContainer(containerName);
    } catch (StorageContainerException e) {
      throw new AssertionError(e);
    }
    return containerData;
  }
}