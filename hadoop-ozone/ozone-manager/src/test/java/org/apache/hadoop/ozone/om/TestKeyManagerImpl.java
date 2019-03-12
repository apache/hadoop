/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.MockNodeManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.ozone.OzoneConfigKeys.*;

/**
 * Test class for @{@link KeyManagerImpl}.
 */
public class TestKeyManagerImpl {

  private static KeyManagerImpl keyManager;
  private static VolumeManagerImpl volumeManager;
  private static BucketManagerImpl bucketManager;
  private static StorageContainerManager scm;
  private static ScmBlockLocationProtocol mockScmBlockLocationProtocol;
  private static OzoneConfiguration conf;
  private static OMMetadataManager metadataManager;
  private static File dir;
  private static long scmBlockSize;
  private static final String KEY_NAME = "key1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String VOLUME_NAME = "vol1";

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new OzoneConfiguration();
    dir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    mockScmBlockLocationProtocol = Mockito.mock(ScmBlockLocationProtocol.class);
    metadataManager = new OmMetadataManagerImpl(conf);
    volumeManager = new VolumeManagerImpl(metadataManager, conf);
    bucketManager = new BucketManagerImpl(metadataManager);
    NodeManager nodeManager = new MockNodeManager(true, 10);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setScmNodeManager(nodeManager);
    scm = TestUtils.getScm(conf, configurator);
    scm.start();
    scm.exitChillMode();
    scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    conf.setLong(OZONE_KEY_PREALLOCATION_BLOCKS_MAX, 10);

    keyManager =
        new KeyManagerImpl(scm.getBlockProtocolServer(), metadataManager, conf,
            "om1", null);
    Mockito.when(mockScmBlockLocationProtocol
        .allocateBlock(Mockito.anyLong(), Mockito.anyInt(),
            Mockito.any(ReplicationType.class),
            Mockito.any(ReplicationFactor.class), Mockito.anyString(),
            Mockito.any(ExcludeList.class))).thenThrow(
        new SCMException("ChillModePrecheck failed for allocateBlock",
            ResultCodes.CHILL_MODE_EXCEPTION));
    createVolume(VOLUME_NAME);
    createBucket(VOLUME_NAME, BUCKET_NAME);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    scm.stop();
    scm.join();
    metadataManager.stop();
    keyManager.stop();
    FileUtils.deleteDirectory(dir);
  }

  private static void createBucket(String volumeName, String bucketName)
      throws IOException {
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .build();
    bucketManager.createBucket(bucketInfo);
  }

  private static void createVolume(String volumeName) throws IOException {
    OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("bilbo")
        .setOwnerName("bilbo")
        .build();
    volumeManager.createVolume(volumeArgs);
  }

  @Test
  public void allocateBlockFailureInChillMode() throws Exception {
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setKeyName(KEY_NAME)
        .setBucketName(BUCKET_NAME)
        .setFactor(ReplicationFactor.ONE)
        .setType(ReplicationType.STAND_ALONE)
        .setVolumeName(VOLUME_NAME)
        .build();
    OpenKeySession keySession = keyManager1.openKey(keyArgs);
    LambdaTestUtils.intercept(OMException.class,
        "ChillModePrecheck failed for allocateBlock", () -> {
          keyManager1
              .allocateBlock(keyArgs, keySession.getId(), new ExcludeList());
        });
  }

  @Test
  public void openKeyFailureInChillMode() throws Exception {
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setKeyName(KEY_NAME)
        .setBucketName(BUCKET_NAME)
        .setFactor(ReplicationFactor.ONE)
        .setDataSize(1000)
        .setType(ReplicationType.STAND_ALONE)
        .setVolumeName(VOLUME_NAME).build();
    LambdaTestUtils.intercept(OMException.class,
        "ChillModePrecheck failed for allocateBlock", () -> {
          keyManager1.openKey(keyArgs);
        });
  }

  @Test
  public void openKeyWithMultipleBlocks() throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setKeyName(UUID.randomUUID().toString())
        .setBucketName(BUCKET_NAME)
        .setFactor(ReplicationFactor.ONE)
        .setDataSize(scmBlockSize * 10)
        .setType(ReplicationType.STAND_ALONE)
        .setVolumeName(VOLUME_NAME)
        .build();
    OpenKeySession keySession = keyManager.openKey(keyArgs);
    OmKeyInfo keyInfo = keySession.getKeyInfo();
    Assert.assertEquals(10,
        keyInfo.getLatestVersionLocations().getLocationList().size());
  }
}