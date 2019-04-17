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
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

/**
 * Test key write/read where a key can span multiple containers.
 */
public class TestMultipleContainerReadWrite {
  private static MiniOzoneCluster cluster = null;
  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
  private static OzoneConfiguration conf;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE, 1,
        StorageUnit.MB);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 5);
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
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
  public void testWriteRead() throws Exception {
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

    String dataString = RandomStringUtils.randomAscii(3 * (int)OzoneConsts.MB);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(3 * (int)OzoneConsts.MB);

    try (OutputStream outputStream = storageHandler.newKeyWriter(keyArgs)) {
      outputStream.write(dataString.getBytes());
    }

    byte[] data = new byte[dataString.length()];
    try (InputStream inputStream = storageHandler.newKeyReader(keyArgs)) {
      inputStream.read(data, 0, data.length);
    }
    assertEquals(dataString, new String(data));
    // checking whether container meta data has the chunk file persisted.
    MetricsRecordBuilder containerMetrics = getMetrics(
        "StorageContainerMetrics");
    assertCounter("numWriteChunk", 3L, containerMetrics);
    assertCounter("numReadChunk", 3L, containerMetrics);
  }

  // Disable this test, because this tests assumes writing beyond a specific
  // size is not allowed. Which is not true for now. Keeping this test in case
  // we add this restrict in the future.
  @Ignore
  @Test
  public void testErrorWrite() throws Exception {
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

    String dataString1 = RandomStringUtils.randomAscii(100);
    String dataString2 = RandomStringUtils.randomAscii(500);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(500);

    try (OutputStream outputStream = storageHandler.newKeyWriter(keyArgs)) {
      // first write will write succeed
      outputStream.write(dataString1.getBytes());
      // second write
      exception.expect(IOException.class);
      exception.expectMessage(
          "Can not write 500 bytes with only 400 byte space");
      outputStream.write(dataString2.getBytes());
    }
  }

  @Test
  public void testPartialRead() throws Exception {
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

    String dataString = RandomStringUtils.randomAscii(500);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(500);

    try (OutputStream outputStream = storageHandler.newKeyWriter(keyArgs)) {
      outputStream.write(dataString.getBytes());
    }

    byte[] data = new byte[600];
    try (InputStream inputStream = storageHandler.newKeyReader(keyArgs)) {
      int readLen = inputStream.read(data, 0, 340);
      assertEquals(340, readLen);
      assertEquals(dataString.substring(0, 340),
          new String(data).substring(0, 340));

      readLen = inputStream.read(data, 340, 260);
      assertEquals(160, readLen);
      assertEquals(dataString, new String(data).substring(0, 500));

      readLen = inputStream.read(data, 500, 1);
      assertEquals(-1, readLen);
    }
  }
}
