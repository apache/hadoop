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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.scm.SCMStorage;
import org.apache.hadoop.ozone.ksm.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.scm.ScmInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConsts.DELETING_KEY_PREFIX;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KSM_ADDRESS_KEY;
import static org.apache.hadoop.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;

/**
 * Test Key Space Manager operation in distributed handler scenario.
 */
public class TestKeySpaceManager {
  private static MiniOzoneCluster cluster = null;
  private static StorageHandler storageHandler;
  private static UserArgs userArgs;
  private static KSMMetrics ksmMetrics;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String ksmId;

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
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    ksmId = UUID.randomUUID().toString();
    conf.set(OzoneConfigKeys.OZONE_HANDLER_TYPE_KEY,
        OzoneConsts.OZONE_HANDLER_DISTRIBUTED);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    cluster = new MiniOzoneClassicCluster.Builder(conf)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setKsmId(ksmId)
        .build();
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
    Assert.assertEquals(createQuota.sizeInBytes(),
        retVolumeInfo.getQuota().sizeInBytes());

    // Set a new quota and test it
    OzoneQuota setQuota =
        new OzoneQuota(rand.nextInt(100), OzoneQuota.Units.GB);
    createVolumeArgs.setQuota(setQuota);
    storageHandler.setVolumeQuota(createVolumeArgs, false);
    getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertEquals(setQuota.sizeInBytes(),
        retVolumeInfo.getQuota().sizeInBytes());

    // Remove the quota and test it again
    storageHandler.setVolumeQuota(createVolumeArgs, true);
    getVolumeArgs = new VolumeArgs(volumeName, userArgs);
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertEquals(OzoneConsts.MAX_QUOTA_IN_BYTES,
        retVolumeInfo.getQuota().sizeInBytes());
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
      Assert.fail("Expecting deletion should fail "
          + "because volume is not empty");
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

    // We allow the key overwrite to be successful. Please note : Till
    // HDFS-11922 is fixed this causes a data block leak on the data node side.
    // That is this overwrite only overwrites the keys on KSM. We need to
    // garbage collect those blocks from datanode.
    KeyArgs keyArgs2 = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    storageHandler.newKeyWriter(keyArgs2);
    Assert
        .assertEquals(numKeyAllocateFails, ksmMetrics.getNumKeyAllocateFails());
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

    // Make sure the deleted key has been renamed.
    MetadataStore store = cluster.getKeySpaceManager().
        getMetadataManager().getStore();
    List<Map.Entry<byte[], byte[]>> list = store.getRangeKVs(null, 10,
        new MetadataKeyFilters.KeyPrefixFilter(DELETING_KEY_PREFIX));
    Assert.assertEquals(1, list.size());

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

  @Test(timeout = 60000)
  public void testListBuckets() throws IOException, OzoneException {
    ListBuckets result = null;
    ListArgs listBucketArgs = null;

    // Create volume - volA.
    final String volAname = "volA";
    VolumeArgs volAArgs = new VolumeArgs(volAname, userArgs);
    volAArgs.setUserName("userA");
    volAArgs.setAdminName("adminA");
    storageHandler.createVolume(volAArgs);

    // Create 20 buckets in volA for tests.
    for (int i=0; i<10; i++) {
      // Create "/volA/aBucket_0" to "/volA/aBucket_9" buckets in volA volume.
      BucketArgs aBuckets = new BucketArgs(volAname,
          "aBucket_" + i, userArgs);
      if(i % 3 == 0) {
        aBuckets.setStorageType(StorageType.ARCHIVE);
      } else {
        aBuckets.setStorageType(StorageType.DISK);
      }
      storageHandler.createBucket(aBuckets);

      // Create "/volA/bBucket_0" to "/volA/bBucket_9" buckets in volA volume.
      BucketArgs bBuckets = new BucketArgs(volAname,
          "bBucket_" + i, userArgs);
      if(i % 3 == 0) {
        bBuckets.setStorageType(StorageType.RAM_DISK);
      } else {
        bBuckets.setStorageType(StorageType.SSD);
      }
      storageHandler.createBucket(bBuckets);
    }

    VolumeArgs volArgs = new VolumeArgs(volAname, userArgs);

    // List all buckets in volA.
    listBucketArgs = new ListArgs(volArgs, null, 100, null);
    result = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(20, result.getBuckets().size());
    List<BucketInfo> archiveBuckets = result.getBuckets().stream()
        .filter(item -> item.getStorageType() == StorageType.ARCHIVE)
        .collect(Collectors.toList());
    Assert.assertEquals(4, archiveBuckets.size());

    // List buckets with prefix "aBucket".
    listBucketArgs = new ListArgs(volArgs, "aBucket", 100, null);
    result = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(10, result.getBuckets().size());
    Assert.assertTrue(result.getBuckets().stream()
        .allMatch(entry -> entry.getBucketName().startsWith("aBucket")));

    // List a certain number of buckets.
    listBucketArgs = new ListArgs(volArgs, null, 3, null);
    result = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(3, result.getBuckets().size());
    Assert.assertEquals("aBucket_0",
        result.getBuckets().get(0).getBucketName());
    Assert.assertEquals("aBucket_1",
        result.getBuckets().get(1).getBucketName());
    Assert.assertEquals("aBucket_2",
        result.getBuckets().get(2).getBucketName());

    // List a certain number of buckets from the startKey.
    listBucketArgs = new ListArgs(volArgs, null, 2, "bBucket_3");
    result = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(2, result.getBuckets().size());
    Assert.assertEquals("bBucket_4",
        result.getBuckets().get(0).getBucketName());
    Assert.assertEquals("bBucket_5",
        result.getBuckets().get(1).getBucketName());

    // Provide an invalid bucket name as start key.
    listBucketArgs = new ListArgs(volArgs, null, 100, "unknown_bucket_name");
    ListBuckets buckets = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(buckets.getBuckets().size(), 0);

    // Use all arguments.
    listBucketArgs = new ListArgs(volArgs, "b", 5, "bBucket_7");
    result = storageHandler.listBuckets(listBucketArgs);
    Assert.assertEquals(2, result.getBuckets().size());
    Assert.assertEquals("bBucket_8",
        result.getBuckets().get(0).getBucketName());
    Assert.assertEquals("bBucket_9",
        result.getBuckets().get(1).getBucketName());

    // Provide an invalid maxKeys argument.
    try {
      listBucketArgs = new ListArgs(volArgs, null, -1, null);
      storageHandler.listBuckets(listBucketArgs);
      Assert.fail("Expecting an error when the given"
          + " maxKeys argument is invalid.");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage()
          .contains(String.format("the value must be in range (0, %d]",
              OzoneConsts.MAX_LISTBUCKETS_SIZE)));
    }

    // Provide an invalid volume name.
    VolumeArgs invalidVolArgs = new VolumeArgs("invalid_name", userArgs);
    try {
      listBucketArgs = new ListArgs(invalidVolArgs, null, 100, null);
      storageHandler.listBuckets(listBucketArgs);
      Assert.fail("Expecting an error when the given volume name is invalid.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
      Assert.assertTrue(e.getMessage()
          .contains(Status.VOLUME_NOT_FOUND.name()));
    }
  }

  /**
   * Test list keys.
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testListKeys() throws IOException, OzoneException {
    ListKeys result = null;
    ListArgs listKeyArgs = null;

    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    // Write 20 keys in bucket.
    int numKeys = 20;
    String keyName = "Key";
    KeyArgs keyArgs = null;
    for (int i = 0; i < numKeys; i++) {
      if (i % 2 == 0) {
        // Create /volume/bucket/aKey[0,2,4,...,18] in bucket.
        keyArgs = new KeyArgs("a" + keyName + i, bucketArgs);
      } else {
        // Create /volume/bucket/bKey[1,3,5,...,19] in bucket.
        keyArgs = new KeyArgs("b" + keyName + i, bucketArgs);
      }
      keyArgs.setSize(4096);

      // Just for testing list keys call, so no need to write real data.
      OutputStream stream = storageHandler.newKeyWriter(keyArgs);
      stream.close();
    }

    // List all keys in bucket.
    bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    listKeyArgs = new ListArgs(bucketArgs, null, 100, null);
    result = storageHandler.listKeys(listKeyArgs);
    Assert.assertEquals(numKeys, result.getKeyList().size());
    List<KeyInfo> allKeys = result.getKeyList().stream()
        .filter(item -> item.getSize() == 4096)
        .collect(Collectors.toList());

    // List keys with prefix "aKey".
    listKeyArgs = new ListArgs(bucketArgs, "aKey", 100, null);
    result = storageHandler.listKeys(listKeyArgs);
    Assert.assertEquals(numKeys / 2, result.getKeyList().size());
    Assert.assertTrue(result.getKeyList().stream()
        .allMatch(entry -> entry.getKeyName().startsWith("aKey")));

    // List a certain number of keys.
    listKeyArgs = new ListArgs(bucketArgs, null, 3, null);
    result = storageHandler.listKeys(listKeyArgs);
    Assert.assertEquals(3, result.getKeyList().size());
    Assert.assertEquals("aKey0",
        result.getKeyList().get(0).getKeyName());
    Assert.assertEquals("aKey10",
        result.getKeyList().get(1).getKeyName());
    Assert.assertEquals("aKey12",
        result.getKeyList().get(2).getKeyName());

    // List a certain number of keys from the startKey.
    listKeyArgs = new ListArgs(bucketArgs, null, 2, "bKey1");
    result = storageHandler.listKeys(listKeyArgs);
    Assert.assertEquals(2, result.getKeyList().size());
    Assert.assertEquals("bKey11",
        result.getKeyList().get(0).getKeyName());
    Assert.assertEquals("bKey13",
        result.getKeyList().get(1).getKeyName());

    // Provide an invalid key name as start key.
    listKeyArgs = new ListArgs(bucketArgs, null, 100, "invalid_start_key");
    ListKeys keys = storageHandler.listKeys(listKeyArgs);
    Assert.assertEquals(keys.getKeyList().size(), 0);

    // Provide an invalid maxKeys argument.
    try {
      listKeyArgs = new ListArgs(bucketArgs, null, -1, null);
      storageHandler.listBuckets(listKeyArgs);
      Assert.fail("Expecting an error when the given"
          + " maxKeys argument is invalid.");
    } catch (Exception e) {
      GenericTestUtils.assertExceptionContains(
          String.format("the value must be in range (0, %d]",
              OzoneConsts.MAX_LISTKEYS_SIZE), e);
    }

    // Provide an invalid bucket name.
    bucketArgs = new BucketArgs("invalid_bucket", createVolumeArgs);
    try {
      listKeyArgs = new ListArgs(bucketArgs, null, numKeys, null);
      storageHandler.listKeys(listKeyArgs);
      Assert.fail(
          "Expecting an error when the given bucket name is invalid.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          Status.BUCKET_NOT_FOUND.name(), e);
    }
  }

  @Test
  public void testListVolumes() throws IOException, OzoneException {

    String user0 = "testListVolumes-user-0";
    String user1 = "testListVolumes-user-1";
    String adminUser = "testListVolumes-admin";
    ListArgs listVolumeArgs;
    ListVolumes volumes;

    // Create 10 volumes by user0 and user1
    String[] user0vols = new String[10];
    String[] user1vols = new String[10];
    for (int i =0; i<10; i++) {
      VolumeArgs createVolumeArgs;
      String user0VolName = "Vol-" + user0 + "-" + i;
      user0vols[i] = user0VolName;
      createVolumeArgs = new VolumeArgs(user0VolName, userArgs);
      createVolumeArgs.setUserName(user0);
      createVolumeArgs.setAdminName(adminUser);
      createVolumeArgs.setQuota(new OzoneQuota(i, OzoneQuota.Units.GB));
      storageHandler.createVolume(createVolumeArgs);

      String user1VolName = "Vol-" + user1 + "-" + i;
      user1vols[i] = user1VolName;
      createVolumeArgs = new VolumeArgs(user1VolName, userArgs);
      createVolumeArgs.setUserName(user1);
      createVolumeArgs.setAdminName(adminUser);
      createVolumeArgs.setQuota(new OzoneQuota(i, OzoneQuota.Units.GB));
      storageHandler.createVolume(createVolumeArgs);
    }

    // Test list all volumes
    UserArgs userArgs0 = new UserArgs(user0, OzoneUtils.getRequestID(),
        null, null, null, null);
    listVolumeArgs = new ListArgs(userArgs0, "Vol-testListVolumes", 100, null);
    listVolumeArgs.setRootScan(true);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(20, volumes.getVolumes().size());

    // Test list all volumes belongs to an user
    listVolumeArgs = new ListArgs(userArgs0, null, 100, null);
    listVolumeArgs.setRootScan(false);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(10, volumes.getVolumes().size());

    // Test prefix
    listVolumeArgs = new ListArgs(userArgs0,
        "Vol-" + user0 + "-3", 100, null);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(1, volumes.getVolumes().size());
    Assert.assertEquals(user0vols[3],
        volumes.getVolumes().get(0).getVolumeName());
    Assert.assertEquals(user0,
        volumes.getVolumes().get(0).getOwner().getName());

    // Test list volumes by user
    UserArgs userArgs1 = new UserArgs(user1, OzoneUtils.getRequestID(),
        null, null, null, null);
    listVolumeArgs = new ListArgs(userArgs1, null, 100, null);
    listVolumeArgs.setRootScan(false);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(10, volumes.getVolumes().size());
    Assert.assertEquals(user1,
        volumes.getVolumes().get(3).getOwner().getName());

    // Make sure all available fields are returned
    final String user0vol4 = "Vol-" + user0 + "-4";
    final String user0vol5 = "Vol-" + user0 + "-5";
    listVolumeArgs = new ListArgs(userArgs0, null, 1, user0vol4);
    listVolumeArgs.setRootScan(false);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(1, volumes.getVolumes().size());
    Assert.assertEquals(user0,
        volumes.getVolumes().get(0).getOwner().getName());
    Assert.assertEquals(user0vol5,
        volumes.getVolumes().get(0).getVolumeName());
    Assert.assertEquals(5,
        volumes.getVolumes().get(0).getQuota().getSize());
    Assert.assertEquals(OzoneQuota.Units.GB,
        volumes.getVolumes().get(0).getQuota().getUnit());

    // User doesn't have volumes
    UserArgs userArgsX = new UserArgs("unknwonUser", OzoneUtils.getRequestID(),
        null, null, null, null);
    listVolumeArgs = new ListArgs(userArgsX, null, 100, null);
    listVolumeArgs.setRootScan(false);
    volumes = storageHandler.listVolumes(listVolumeArgs);
    Assert.assertEquals(0, volumes.getVolumes().size());
  }

  /**
   * Test get key information.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testGetKeyInfo() throws IOException,
      OzoneException, ParseException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    long currentTime = Time.now();

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    String keyName = "testKey";
    KeyArgs keyArgs = new KeyArgs(keyName, bucketArgs);
    keyArgs.setSize(4096);


    OutputStream stream = storageHandler.newKeyWriter(keyArgs);
    stream.close();

    KeyInfo keyInfo = storageHandler.getKeyInfo(keyArgs);
    // Compare the time in second unit since the date string reparsed to
    // millisecond will lose precision.
    Assert.assertTrue((OzoneUtils.formatDate(keyInfo.getCreatedOn())
        / 1000) >= (currentTime / 1000));
    Assert.assertTrue((OzoneUtils.formatDate(keyInfo.getModifiedOn())
        / 1000) >= (currentTime / 1000));
    Assert.assertEquals(keyName, keyInfo.getKeyName());
    // with out data written, the size would be 0
    Assert.assertEquals(0, keyInfo.getSize());
  }

  /**
   * Test that the write can proceed without having to set the right size.
   *
   * @throws IOException
   */
  @Test
  public void testWriteSize() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

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
    // write a key without specifying size at all
    String keyName = "testKey";
    KeyArgs keyArgs = new KeyArgs(keyName, bucketArgs);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }
    byte[] data = new byte[dataString.length()];
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    Assert.assertEquals(dataString, DFSUtil.bytes2String(data));

    // write a key with a size, but write above it.
    String keyName1 = "testKey1";
    KeyArgs keyArgs1 = new KeyArgs(keyName1, bucketArgs);
    keyArgs1.setSize(30);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs1)) {
      stream.write(dataString.getBytes());
    }
    byte[] data1 = new byte[dataString.length()];
    try (InputStream in = storageHandler.newKeyReader(keyArgs1)) {
      in.read(data1);
    }
    Assert.assertEquals(dataString, DFSUtil.bytes2String(data1));
  }

  /**
   * Tests the RPC call for getting scmId and clusterId from SCM.
   * @throws IOException
   */
  @Test
  public void testGetScmInfo() throws IOException {
    ScmInfo info = cluster.getKeySpaceManager().getScmInfo();
    Assert.assertEquals(clusterId, info.getClusterId());
    Assert.assertEquals(scmId, info.getScmId());
  }


  @Test
  public void testExpiredOpenKey() throws Exception {
    BackgroundService openKeyCleanUpService = ((KeyManagerImpl)cluster
        .getKeySpaceManager().getKeyManager()).getOpenKeyCleanupService();

    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    bucketArgs.setAddAcls(new LinkedList<>());
    bucketArgs.setRemoveAcls(new LinkedList<>());
    bucketArgs.setStorageType(StorageType.DISK);
    storageHandler.createBucket(bucketArgs);

    // open some keys.

    KeyArgs keyArgs1 = new KeyArgs("testKey1", bucketArgs);
    KeyArgs keyArgs2 = new KeyArgs("testKey2", bucketArgs);
    KeyArgs keyArgs3 = new KeyArgs("testKey3", bucketArgs);
    KeyArgs keyArgs4 = new KeyArgs("testKey4", bucketArgs);
    List<BlockGroup> openKeys;
    storageHandler.newKeyWriter(keyArgs1);
    storageHandler.newKeyWriter(keyArgs2);
    storageHandler.newKeyWriter(keyArgs3);
    storageHandler.newKeyWriter(keyArgs4);

    Set<String> expected = Stream.of(
        "testKey1", "testKey2", "testKey3", "testKey4")
        .collect(Collectors.toSet());

    // Now all k1-k4 should be in open state, so ExpiredOpenKeys should not
    // contain these values.
    openKeys = cluster.getKeySpaceManager()
        .getMetadataManager().getExpiredOpenKeys();

    for (BlockGroup bg : openKeys) {
      String[] subs = bg.getGroupID().split("/");
      String keyName = subs[subs.length - 1];
      Assert.assertFalse(expected.contains(keyName));
    }

    Thread.sleep(2000);
    // Now all k1-k4 should be in ExpiredOpenKeys
    openKeys = cluster.getKeySpaceManager()
        .getMetadataManager().getExpiredOpenKeys();
    for (BlockGroup bg : openKeys) {
      String[] subs = bg.getGroupID().split("/");
      String keyName = subs[subs.length - 1];
      if (expected.contains(keyName)) {
        expected.remove(keyName);
      }
    }
    Assert.assertEquals(0, expected.size());

    KeyArgs keyArgs5 = new KeyArgs("testKey5", bucketArgs);
    storageHandler.newKeyWriter(keyArgs5);

    openKeyCleanUpService.triggerBackgroundTaskForTesting();
    Thread.sleep(2000);
    // now all k1-k4 should have been removed by the clean-up task, only k5
    // should be present in ExpiredOpenKeys.
    openKeys =
        cluster.getKeySpaceManager().getMetadataManager().getExpiredOpenKeys();
    System.out.println(openKeys);
    boolean key5found = false;
    Set<String> removed = Stream.of(
        "testKey1", "testKey2", "testKey3", "testKey4")
        .collect(Collectors.toSet());
    for (BlockGroup bg : openKeys) {
      String[] subs = bg.getGroupID().split("/");
      String keyName = subs[subs.length - 1];
      Assert.assertFalse(removed.contains(keyName));
      if (keyName.equals("testKey5")) {
        key5found = true;
      }
    }
    Assert.assertTrue(key5found);
  }

  /**
   * Tests the KSM Initialization.
   * @throws IOException
   */
  @Test
  public void testKSMInitialization() throws IOException {
    // Read the version file info from KSM version file
    KSMStorage ksmStorage = cluster.getKeySpaceManager().getKsmStorage();
    SCMStorage scmStorage = new SCMStorage(conf);
    // asserts whether cluster Id and SCM ID are properly set in SCM Version
    // file.
    Assert.assertEquals(clusterId, scmStorage.getClusterID());
    Assert.assertEquals(scmId, scmStorage.getScmId());
    // asserts whether KSM Id is properly set in KSM Version file.
    Assert.assertEquals(ksmId, ksmStorage.getKsmId());
    // asserts whether the SCM info is correct in KSM Version file.
    Assert.assertEquals(clusterId, ksmStorage.getClusterID());
    Assert.assertEquals(scmId, ksmStorage.getScmId());
  }

  /**
   * Tests the KSM Initialization Failure.
   * @throws IOException
   */
  @Test
  public void testKSMInitializationFailure() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    Path metaDirPath = Paths.get(path, "ksm-meta");
    config.set(OzoneConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    config.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    config.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    config.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        conf.get(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY));
    exception.expect(KSMException.class);
    exception.expectMessage("KSM not initialized.");
    KeySpaceManager.createKSM(null, config);
    KSMStorage ksmStore = new KSMStorage(config);
    ksmStore.setClusterId("testClusterId");
    ksmStore.setScmId("testScmId");
    // writes the version file properties
    ksmStore.initialize();
    exception.expect(KSMException.class);
    exception.expectMessage("SCM version info mismatch.");
    KeySpaceManager.createKSM(null, conf);
  }

  @Test
  public void testGetServiceList() throws IOException {
    long numGetServiceListCalls = ksmMetrics.getNumGetServiceLists();
    List<ServiceInfo> services = cluster.getKeySpaceManager().getServiceList();

    Assert.assertEquals(numGetServiceListCalls + 1,
        ksmMetrics.getNumGetServiceLists());

    ServiceInfo ksmInfo = services.stream().filter(
        a -> a.getNodeType().equals(OzoneProtos.NodeType.KSM))
        .collect(Collectors.toList()).get(0);
    InetSocketAddress ksmAddress = new InetSocketAddress(ksmInfo.getHostname(),
        ksmInfo.getPort(ServicePort.Type.RPC));
    Assert.assertEquals(NetUtils.createSocketAddr(
        conf.get(OZONE_KSM_ADDRESS_KEY)), ksmAddress);

    ServiceInfo scmInfo = services.stream().filter(
        a -> a.getNodeType().equals(OzoneProtos.NodeType.SCM))
        .collect(Collectors.toList()).get(0);
    InetSocketAddress scmAddress = new InetSocketAddress(scmInfo.getHostname(),
        scmInfo.getPort(ServicePort.Type.RPC));
    Assert.assertEquals(NetUtils.createSocketAddr(
        conf.get(OZONE_SCM_CLIENT_ADDRESS_KEY)), scmAddress);
  }
}
