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
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.ksm.helpers.OpenKeySession;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the versioning of blocks from KSM side.
 */
public class TestKsmBlockVersioning {
  private static MiniOzoneCluster cluster = null;
  private static UserArgs userArgs;
  private static OzoneConfiguration conf;
  private static KeySpaceManager keySpaceManager;
  private static StorageHandler storageHandler;

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
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    keySpaceManager = cluster.getKeySpaceManager();
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
  public void testAllocateCommit() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(1000)
        .build();

    // 1st update, version 0
    OpenKeySession openKey = keySpaceManager.openKey(keyArgs);
    keySpaceManager.commitKey(keyArgs, openKey.getId());

    KsmKeyInfo keyInfo = keySpaceManager.lookupKey(keyArgs);
    KsmKeyLocationInfoGroup highestVersion =
        checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(0, highestVersion.getVersion());
    assertEquals(1, highestVersion.getLocationList().size());

    // 2nd update, version 1
    openKey = keySpaceManager.openKey(keyArgs);
    //KsmKeyLocationInfo locationInfo =
    //    keySpaceManager.allocateBlock(keyArgs, openKey.getId());
    keySpaceManager.commitKey(keyArgs, openKey.getId());

    keyInfo = keySpaceManager.lookupKey(keyArgs);
    highestVersion = checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(1, highestVersion.getVersion());
    assertEquals(2, highestVersion.getLocationList().size());

    // 3rd update, version 2
    openKey = keySpaceManager.openKey(keyArgs);
    // this block will be appended to the latest version of version 2.
    keySpaceManager.allocateBlock(keyArgs, openKey.getId());
    keySpaceManager.commitKey(keyArgs, openKey.getId());

    keyInfo = keySpaceManager.lookupKey(keyArgs);
    highestVersion = checkVersions(keyInfo.getKeyLocationVersions());
    assertEquals(2, highestVersion.getVersion());
    assertEquals(4, highestVersion.getLocationList().size());
  }

  private KsmKeyLocationInfoGroup checkVersions(
      List<KsmKeyLocationInfoGroup> versions) {
    KsmKeyLocationInfoGroup currentVersion = null;
    for (KsmKeyLocationInfoGroup version : versions) {
      if (currentVersion != null) {
        assertEquals(currentVersion.getVersion() + 1, version.getVersion());
        for (KsmKeyLocationInfo info : currentVersion.getLocationList()) {
          boolean found = false;
          // all the blocks from the previous version must present in the next
          // version
          for (KsmKeyLocationInfo info2 : version.getLocationList()) {
            if (info.getBlockID().equals(info2.getBlockID())) {
              found = true;
              break;
            }
          }
          assertTrue(found);
        }
      }
      currentVersion = version;
    }
    return currentVersion;
  }

  @Test
  public void testReadLatestVersion() throws Exception {

    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    KsmKeyArgs ksmKeyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDataSize(1000)
        .build();

    String dataString = RandomStringUtils.randomAlphabetic(100);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    // this write will create 1st version with one block
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    byte[] data = new byte[dataString.length()];
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    KsmKeyInfo keyInfo = keySpaceManager.lookupKey(ksmKeyArgs);
    assertEquals(dataString, DFSUtil.bytes2String(data));
    assertEquals(0, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(1,
        keyInfo.getLatestVersionLocations().getLocationList().size());

    // this write will create 2nd version, 2nd version will contain block from
    // version 1, and add a new block
    dataString = RandomStringUtils.randomAlphabetic(10);
    data = new byte[dataString.length()];
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    keyInfo = keySpaceManager.lookupKey(ksmKeyArgs);
    assertEquals(dataString, DFSUtil.bytes2String(data));
    assertEquals(1, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(2,
        keyInfo.getLatestVersionLocations().getLocationList().size());

    dataString = RandomStringUtils.randomAlphabetic(200);
    data = new byte[dataString.length()];
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    keyInfo = keySpaceManager.lookupKey(ksmKeyArgs);
    assertEquals(dataString, DFSUtil.bytes2String(data));
    assertEquals(2, keyInfo.getLatestVersionLocations().getVersion());
    assertEquals(3,
        keyInfo.getLatestVersionLocations().getLocationList().size());
  }
}