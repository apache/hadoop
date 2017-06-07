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
import org.apache.hadoop.hdfs.DFSUtil;
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
import org.apache.hadoop.ozone.web.request.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

/**
 * Test Key Space Manager operation in distributed handler scenario.
 */
public class TestKeySpaceManager {
  private static MiniOzoneCluster cluster = null;
  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
  private static KSMMetrics ksmMetrics;

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
    long volumeCreateFailCount = ksmMetrics.getNumVolumeCreateFails();
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
    Assert.assertEquals(volumeCreateFailCount,
        ksmMetrics.getNumVolumeCreateFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test(timeout = 60000)
  public void testChangeVolumeOwner() throws IOException, OzoneException {
    long volumeCreateFailCount = ksmMetrics.getNumVolumeCreateFails();
    long volumeInfoFailCount = ksmMetrics.getNumVolumeInfoFails();
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
    Assert.assertEquals(volumeCreateFailCount,
        ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(volumeInfoFailCount,
        ksmMetrics.getNumVolumeInfoFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test(timeout = 60000)
  public void testChangeVolumeQuota() throws IOException, OzoneException {
    long numVolumeCreateFail = ksmMetrics.getNumVolumeCreateFails();
    long numVolumeInfoFail = ksmMetrics.getNumVolumeInfoFails();
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
    Assert.assertEquals(numVolumeCreateFail,
        ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(numVolumeInfoFail,
        ksmMetrics.getNumVolumeInfoFails());
  }

  // Create a volume and then delete it and then check for deletion
  @Test(timeout = 60000)
  public void testDeleteVolume() throws IOException, OzoneException {
    long volumeCreateFailCount = ksmMetrics.getNumVolumeCreateFails();
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String volumeName1 = volumeName + "_A";
    String volumeName2 = volumeName + "_AA";
    VolumeArgs volumeArgs = null;
    VolumeInfo volumeInfo = null;

    // Create 2 empty volumes with same prefix.
    volumeArgs = new VolumeArgs(volumeName1, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);

    volumeArgs = new VolumeArgs(volumeName2, userArgs);
    volumeArgs.setUserName(userName);
    volumeArgs.setAdminName(adminName);
    storageHandler.createVolume(volumeArgs);

    volumeArgs  = new VolumeArgs(volumeName1, userArgs);
    volumeInfo = storageHandler.getVolumeInfo(volumeArgs);
    Assert.assertTrue(volumeInfo.getVolumeName().equals(volumeName1));
    Assert.assertTrue(volumeInfo.getOwner().getName().equals(userName));
    Assert.assertEquals(volumeCreateFailCount,
        ksmMetrics.getNumVolumeCreateFails());

    // Volume with _A should be able to delete as it is empty.
    storageHandler.deleteVolume(volumeArgs);

    // Make sure volume with _AA suffix still exists.
    volumeArgs = new VolumeArgs(volumeName2, userArgs);
    volumeInfo = storageHandler.getVolumeInfo(volumeArgs);
    Assert.assertTrue(volumeInfo.getVolumeName().equals(volumeName2));

    // Make sure volume with _A suffix is successfully deleted.
    exception.expect(IOException.class);
    exception.expectMessage("Info Volume failed, error:VOLUME_NOT_FOUND");
    volumeArgs = new VolumeArgs(volumeName1, userArgs);
    storageHandler.getVolumeInfo(volumeArgs);
  }

  // Create a volume and a bucket inside the volume,
  // then delete it and then check for deletion failure
  @Test(timeout = 60000)
  public void testFailedDeleteVolume() throws IOException, OzoneException {
    long numVolumeCreateFails = ksmMetrics.getNumVolumeCreateFails();
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    VolumeArgs getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    VolumeInfo retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertTrue(retVolumeInfo.getVolumeName().equals(volumeName));
    Assert.assertTrue(retVolumeInfo.getOwner().getName().equals(userName));
    Assert.assertEquals(numVolumeCreateFails,
        ksmMetrics.getNumVolumeCreateFails());

    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    try {
      storageHandler.deleteVolume(createVolumeArgs);
    } catch (IOException ex) {
      Assert.assertEquals(ex.getMessage(),
          "Delete Volume failed, error:VOLUME_NOT_EMPTY");
    }
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertTrue(retVolumeInfo.getVolumeName().equals(volumeName));
    Assert.assertTrue(retVolumeInfo.getOwner().getName().equals(userName));
  }

  // Create a volume and test Volume access for a different user
  @Test(timeout = 60000)
  public void testAccessVolume() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String[] groupName =
        {"group" + RandomStringUtils.randomNumeric(5)};

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    createVolumeArgs.setGroups(groupName);
    storageHandler.createVolume(createVolumeArgs);

    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, userName,
        OzoneAcl.OzoneACLRights.READ_WRITE);
    Assert.assertTrue(storageHandler.checkVolumeAccess(volumeName, userAcl));
    OzoneAcl group = new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, groupName[0],
        OzoneAcl.OzoneACLRights.READ);
    Assert.assertTrue(storageHandler.checkVolumeAccess(volumeName, group));

    // Create a different user and access should fail
    String falseUserName = "user" + RandomStringUtils.randomNumeric(5);
    OzoneAcl falseUserAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.USER, falseUserName,
            OzoneAcl.OzoneACLRights.READ_WRITE);
    Assert.assertFalse(storageHandler
        .checkVolumeAccess(volumeName, falseUserAcl));
    // Checking access with user name and Group Type should fail
    OzoneAcl falseGroupAcl = new OzoneAcl(OzoneAcl.OzoneACLType.GROUP, userName,
        OzoneAcl.OzoneACLRights.READ_WRITE);
    Assert.assertFalse(storageHandler
        .checkVolumeAccess(volumeName, falseGroupAcl));

    // Access for acl type world should also fail
    OzoneAcl worldAcl =
        new OzoneAcl(OzoneAcl.OzoneACLType.WORLD, "",
            OzoneAcl.OzoneACLRights.READ);
    Assert.assertFalse(storageHandler.checkVolumeAccess(volumeName, worldAcl));

    Assert.assertEquals(0, ksmMetrics.getNumVolumeCheckAccessFails());
    Assert.assertEquals(0, ksmMetrics.getNumVolumeCreateFails());
  }

  @Test(timeout = 60000)
  public void testCreateBucket() throws IOException, OzoneException {
    long numVolumeCreateFail = ksmMetrics.getNumVolumeCreateFails();
    long numBucketCreateFail = ksmMetrics.getNumBucketCreateFails();
    long numBucketInfoFail = ksmMetrics.getNumBucketInfoFails();
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
    Assert.assertEquals(numVolumeCreateFail,
        ksmMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(numBucketCreateFail,
        ksmMetrics.getNumBucketCreateFails());
    Assert.assertEquals(numBucketInfoFail,
        ksmMetrics.getNumBucketInfoFails());
  }

  @Test(timeout = 60000)
  public void testDeleteBucket() throws IOException, OzoneException {
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
    storageHandler.deleteBucket(bucketArgs);
    exception.expect(IOException.class);
    exception.expectMessage("Info Bucket failed, error: BUCKET_NOT_FOUND");
    storageHandler.getBucketInfo(getBucketArgs);
  }

  @Test(timeout = 60000)
  public void testDeleteNonExistingBucket() throws IOException, OzoneException {
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
    BucketArgs newBucketArgs = new BucketArgs(
        volumeName, bucketName + "_invalid", userArgs);
    exception.expect(IOException.class);
    exception.expectMessage("Delete Bucket failed, error:BUCKET_NOT_FOUND");
    storageHandler.deleteBucket(newBucketArgs);
  }


  @Test(timeout = 60000)
  public void testDeleteNonEmptyBucket() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
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
    String dataString = RandomStringUtils.randomAscii(100);
    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    exception.expect(IOException.class);
    exception.expectMessage("Delete Bucket failed, error:BUCKET_NOT_EMPTY");
    storageHandler.deleteBucket(bucketArgs);
  }

  /**
   * Basic test of both putKey and getKey from KSM, as one can not be tested
   * without the other.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testGetKeyWriterReader() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyAllocates = ksmMetrics.getNumKeyAllocates();
    long numKeyLookups = ksmMetrics.getNumKeyLookups();

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
    keyArgs.setSize(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    Assert.assertEquals(1 + numKeyAllocates, ksmMetrics.getNumKeyAllocates());

    byte[] data = new byte[dataString.length()];
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    Assert.assertEquals(dataString, DFSUtil.bytes2String(data));
    Assert.assertEquals(1 + numKeyLookups, ksmMetrics.getNumKeyLookups());
  }

  /**
   * Test write the same key twice, the second write should fail, as currently
   * key overwrite is not supported.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testKeyOverwrite() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyAllocateFails = ksmMetrics.getNumKeyAllocateFails();

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    keyArgs.setSize(100);
    String dataString = RandomStringUtils.randomAscii(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }

    // We allow the key overwrite to be successful. Please note : Till HDFS-11922
    // is fixed this causes a data block leak on the data node side. That is
    // this overwrite only overwrites the keys on KSM. We need to garbage
    // collect those blocks from datanode.
    KeyArgs keyArgs2 = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    storageHandler.newKeyWriter(keyArgs2);
    Assert.assertEquals(numKeyAllocateFails,
        ksmMetrics.getNumKeyAllocateFails());
  }

  /**
   * Test get a non-exiting key.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testGetNonExistKey() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyLookupFails = ksmMetrics.getNumKeyLookupFails();

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    KeyArgs keyArgs = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    // try to get the key, should fail as it hasn't been created
    exception.expect(IOException.class);
    exception.expectMessage("KEY_NOT_FOUND");
    storageHandler.newKeyReader(keyArgs);
    Assert.assertEquals(1 + numKeyLookupFails,
        ksmMetrics.getNumKeyLookupFails());
  }

  /**
   * Test delete keys for ksm.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testDeleteKey() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyDeletes = ksmMetrics.getNumKeyDeletes();
    long numKeyDeleteFails = ksmMetrics.getNumKeyDeletesFails();

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    storageHandler.createBucket(bucketArgs);

    KeyArgs keyArgs = new KeyArgs(keyName, bucketArgs);
    keyArgs.setSize(100);
    String dataString = RandomStringUtils.randomAscii(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }

    storageHandler.deleteKey(keyArgs);
    Assert.assertEquals(1 + numKeyDeletes, ksmMetrics.getNumKeyDeletes());

    // Check the block key in SCM, make sure it's deleted.
    Set<String> keys = new HashSet<>();
    keys.add(keyArgs.getResourceName());
    exception.expect(IOException.class);
    exception.expectMessage("Specified block key does not exist");
    cluster.getStorageContainerManager().getBlockLocations(keys);

    // Delete the key again to test deleting non-existing key.
    exception.expect(IOException.class);
    exception.expectMessage("KEY_NOT_FOUND");
    storageHandler.deleteKey(keyArgs);
    Assert.assertEquals(1 + numKeyDeleteFails,
        ksmMetrics.getNumKeyDeletesFails());
  }

}
