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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;


import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Random;

/**
 * Test Key Space Manager operation in distributed handler scenario.
 */
public class TestKeySpaceManager {
  private static MiniOzoneCluster cluster = null;
  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
  private static KSMMetrics ksmMetrics;

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
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    cluster = new MiniOzoneCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    ksmMetrics = cluster.getKeySpaceManager().getMetrics();
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

  // Create a volume and test its attribute after creating them
  @Test(timeout = 60000)
  public void testCreateVolume() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    VolumeArgs getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    VolumeInfo retVolumeinfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertTrue(retVolumeinfo.getVolumeName().equals(volumeName));
    Assert.assertTrue(retVolumeinfo.getOwner().getName().equals(userName));
    Assert.assertEquals(0, ksmMetrics.getNumVolumeCreateFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test(timeout = 60000)
  public void testChangeVolumeOwner() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    String newUserName = "user" + RandomStringUtils.randomNumeric(5);
    createVolumeArgs.setUserName(newUserName);
    storageHandler.setVolumeOwner(createVolumeArgs);

    VolumeArgs getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    VolumeInfo retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);

    Assert.assertTrue(retVolumeInfo.getVolumeName().equals(volumeName));
    Assert.assertFalse(retVolumeInfo.getOwner().getName().equals(userName));
    Assert.assertTrue(retVolumeInfo.getOwner().getName().equals(newUserName));
    Assert.assertEquals(0, ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(0, ksmMetrics.getNumVolumeInfoFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test(timeout = 60000)
  public void testChangeVolumeQuota() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    Random rand = new Random();

    // Create a new volume with a quota
    OzoneQuota createQuota =
        new OzoneQuota(rand.nextInt(100), OzoneQuota.Units.GB);
    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    createVolumeArgs.setQuota(createQuota);
    storageHandler.createVolume(createVolumeArgs);

    VolumeArgs getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    VolumeInfo retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertEquals(retVolumeInfo.getQuota().sizeInBytes(),
                                              createQuota.sizeInBytes());

    // Set a new quota and test it
    OzoneQuota setQuota =
        new OzoneQuota(rand.nextInt(100), OzoneQuota.Units.GB);
    createVolumeArgs.setQuota(setQuota);
    storageHandler.setVolumeQuota(createVolumeArgs, false);
    getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertEquals(retVolumeInfo.getQuota().sizeInBytes(),
                                                setQuota.sizeInBytes());

    // Remove the quota and test it again
    storageHandler.setVolumeQuota(createVolumeArgs, true);
    getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertEquals(retVolumeInfo.getQuota().sizeInBytes(),
        OzoneConsts.MAX_QUOTA_IN_BYTES);
    Assert.assertEquals(0, ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(0, ksmMetrics.getNumVolumeInfoFails());
  }

  @Test(timeout = 60000)
  public void testCreateBucket() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs volumeArgs = new VolumeArgs(volumeName, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);

    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    BucketArgs getBucketArgs = new BucketArgs(volumeName, bucketName,
        userArgs);
    BucketInfo bucketInfo = storageHandler.getBucketInfo(getBucketArgs);
    Assert.assertTrue(bucketInfo.getVolumeName().equals(volumeName));
    Assert.assertTrue(bucketInfo.getBucketName().equals(bucketName));
    Assert.assertEquals(0, ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(0, ksmMetrics.getNumBucketCreateFails());
    Assert.assertEquals(0, ksmMetrics.getNumBucketInfoFails());
  }

  @Test
  public void testGetKeyWriter() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    Assert.assertEquals(0, ksmMetrics.getNumKeyBlockAllocates());

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    String dataString = RandomStringUtils.randomAscii(100);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(4096);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    Assert.assertEquals(1, ksmMetrics.getNumKeyBlockAllocates());
  }
}
