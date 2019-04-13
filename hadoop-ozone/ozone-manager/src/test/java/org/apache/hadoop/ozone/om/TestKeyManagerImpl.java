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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
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
    scm.exitSafeMode();
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
        new SCMException("SafeModePrecheck failed for allocateBlock",
            ResultCodes.SAFE_MODE_EXCEPTION));
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
  public void allocateBlockFailureInSafeMode() throws Exception {
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .build();
    OpenKeySession keySession = keyManager1.openKey(keyArgs);
    LambdaTestUtils.intercept(OMException.class,
        "SafeModePrecheck failed for allocateBlock", () -> {
          keyManager1
              .allocateBlock(keyArgs, keySession.getId(), new ExcludeList());
        });
  }

  @Test
  public void openKeyFailureInSafeMode() throws Exception {
    KeyManager keyManager1 = new KeyManagerImpl(mockScmBlockLocationProtocol,
        metadataManager, conf, "om1", null);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(KEY_NAME)
        .setDataSize(1000)
        .build();
    LambdaTestUtils.intercept(OMException.class,
        "SafeModePrecheck failed for allocateBlock", () -> {
          keyManager1.openKey(keyArgs);
        });
  }

  @Test
  public void openKeyWithMultipleBlocks() throws IOException {
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(UUID.randomUUID().toString())
        .setDataSize(scmBlockSize * 10)
        .build();
    OpenKeySession keySession = keyManager.openKey(keyArgs);
    OmKeyInfo keyInfo = keySession.getKeyInfo();
    Assert.assertEquals(10,
        keyInfo.getLatestVersionLocations().getLocationList().size());
  }

  @Test
  public void testCreateDirectory() throws IOException {
    // Create directory where the parent directory does not exist
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    keyManager.createDirectory(keyArgs);
    Path path = Paths.get(keyName);
    while (path != null) {
      // verify parent directories are created
      Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());
      path = path.getParent();
    }

    // make sure create directory fails where parent is a file
    keyName = RandomStringUtils.randomAlphabetic(5);
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = keyManager.openKey(keyArgs);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    try {
      keyManager.createDirectory(keyArgs);
      Assert.fail("Creation should fail for directory.");
    } catch (OMException e) {
      Assert.assertEquals(e.getResult(),
          OMException.ResultCodes.FILE_ALREADY_EXISTS);
    }

    // create directory for root directory
    keyName = "";
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    keyManager.createDirectory(keyArgs);
    Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());

    // create directory where parent is root
    keyName = RandomStringUtils.randomAlphabetic(5);
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    keyManager.createDirectory(keyArgs);
    Assert.assertTrue(keyManager.getFileStatus(keyArgs).isDirectory());
  }

  @Test
  public void testOpenFile() throws IOException {
    // create key
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());

    // try to open created key with overWrite flag set to false
    try {
      keyManager.createFile(keyArgs, false, false);
      Assert.fail("Open key should fail for non overwrite create");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.FILE_ALREADY_EXISTS) {
        throw ex;
      }
    }

    // create file should pass with overwrite flag set to true
    keyManager.createFile(keyArgs, true, false);

    // try to create a file where parent directories do not exist and
    // recursive flag is set to false
    keyName = RandomStringUtils.randomAlphabetic(5);
    for (int i =0; i< 5; i++) {
      keyName += "/" + RandomStringUtils.randomAlphabetic(5);
    }
    keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();
    try {
      keyManager.createFile(keyArgs, false, false);
      Assert.fail("Open file should fail for non recursive write");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.DIRECTORY_NOT_FOUND) {
        throw ex;
      }
    }

    // file create should pass when recursive flag is set to true
    keySession = keyManager.createFile(keyArgs, false, true);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    Assert.assertTrue(keyManager
        .getFileStatus(keyArgs).isFile());

    // try creating a file over a directory
    keyArgs = createBuilder()
        .setKeyName("")
        .build();
    try {
      keyManager.createFile(keyArgs, true, true);
      Assert.fail("Open file should fail for non recursive write");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.NOT_A_FILE) {
        throw ex;
      }
    }
  }

  @Test
  public void testLookupFile() throws IOException {
    String keyName = RandomStringUtils.randomAlphabetic(5);
    OmKeyArgs keyArgs = createBuilder()
        .setKeyName(keyName)
        .build();

    // lookup for a non-existent file
    try {
      keyManager.lookupFile(keyArgs);
      Assert.fail("Lookup file should fail for non existent file");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.FILE_NOT_FOUND) {
        throw ex;
      }
    }

    // create a file
    OpenKeySession keySession = keyManager.createFile(keyArgs, false, false);
    keyArgs.setLocationInfoList(
        keySession.getKeyInfo().getLatestVersionLocations().getLocationList());
    keyManager.commitKey(keyArgs, keySession.getId());
    Assert.assertEquals(keyManager.lookupFile(keyArgs).getKeyName(), keyName);

    // lookup for created file
    keyArgs = createBuilder()
        .setKeyName("")
        .build();
    try {
      keyManager.lookupFile(keyArgs);
      Assert.fail("Lookup file should fail for a directory");
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.NOT_A_FILE) {
        throw ex;
      }
    }
  }

  private OmKeyArgs.Builder createBuilder() {
    return new OmKeyArgs.Builder()
        .setBucketName(BUCKET_NAME)
        .setFactor(ReplicationFactor.ONE)
        .setDataSize(0)
        .setType(ReplicationType.STAND_ALONE)
        .setVolumeName(VOLUME_NAME);
  }
}