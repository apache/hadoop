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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.datanode.ObjectStoreHandler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.util.OzoneVersionInfo;
import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.KeyArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.handlers.UserArgs;
import org.apache.hadoop.ozone.web.handlers.VolumeArgs;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.apache.hadoop.ozone.web.request.OzoneQuota;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.apache.hadoop.ozone.web.response.KeyInfo;
import org.apache.hadoop.ozone.web.response.ListBuckets;
import org.apache.hadoop.ozone.web.response.ListKeys;
import org.apache.hadoop.ozone.web.response.ListVolumes;
import org.apache.hadoop.ozone.web.response.VolumeInfo;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.apache.hadoop.utils.db.TableIterator;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ACL_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Test Ozone Manager operation in distributed handler scenario.
 */
public class TestOzoneManager {
  private MiniOzoneCluster cluster = null;
  private StorageHandler storageHandler;
  private UserArgs userArgs;
  private OMMetrics omMetrics;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omId;

  @Rule
  public Timeout timeout = new Timeout(60000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    conf.setBoolean(OZONE_ACL_ENABLED, true);
    conf.setInt(OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS, 2);
    cluster =  MiniOzoneCluster.newBuilder(conf)
        .setClusterId(clusterId)
        .setScmId(scmId)
        .setOmId(omId)
        .build();
    cluster.waitForClusterToBeReady();
    storageHandler = new ObjectStoreHandler(conf).getStorageHandler();
    userArgs = new UserArgs(null, OzoneUtils.getRequestID(),
        null, null, null, null);
    omMetrics = cluster.getOzoneManager().getMetrics();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // Create a volume and test its attribute after creating them
  @Test
  public void testCreateVolume() throws IOException, OzoneException {
    long volumeCreateFailCount = omMetrics.getNumVolumeCreateFails();
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
        omMetrics.getNumVolumeCreateFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test
  public void testChangeVolumeOwner() throws IOException, OzoneException {
    long volumeCreateFailCount = omMetrics.getNumVolumeCreateFails();
    long volumeInfoFailCount = omMetrics.getNumVolumeInfoFails();
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
        omMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(volumeInfoFailCount,
        omMetrics.getNumVolumeInfoFails());
  }

  // Create a volume and modify the volume owner and then test its attributes
  @Test
  public void testChangeVolumeQuota() throws IOException, OzoneException {
    long numVolumeCreateFail = omMetrics.getNumVolumeCreateFails();
    long numVolumeInfoFail = omMetrics.getNumVolumeInfoFails();
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
        omMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(numVolumeInfoFail,
        omMetrics.getNumVolumeInfoFails());
  }

  // Create a volume and then delete it and then check for deletion
  @Test
  public void testDeleteVolume() throws IOException, OzoneException {
    long volumeCreateFailCount = omMetrics.getNumVolumeCreateFails();
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
        omMetrics.getNumVolumeCreateFails());

    // Volume with _A should be able to delete as it is empty.
    storageHandler.deleteVolume(volumeArgs);

    // Make sure volume with _AA suffix still exists.
    volumeArgs = new VolumeArgs(volumeName2, userArgs);
    volumeInfo = storageHandler.getVolumeInfo(volumeArgs);
    Assert.assertTrue(volumeInfo.getVolumeName().equals(volumeName2));

    // Make sure volume with _A suffix is successfully deleted.
    try {
      volumeArgs = new VolumeArgs(volumeName1, userArgs);
      storageHandler.getVolumeInfo(volumeArgs);
      Assert.fail("Volume is not deleted");
    } catch (OMException ex) {
      Assert.assertEquals(ResultCodes.VOLUME_NOT_FOUND, ex.getResult());
    }
    //delete the _AA volume, too
    storageHandler.deleteVolume(new VolumeArgs(volumeName2, userArgs));

    //Make sure there is no volume information for the specific user
    OMMetadataManager metadataManager =
        cluster.getOzoneManager().getMetadataManager();

    String userKey = metadataManager.getUserKey(userName);
    VolumeList volumes = metadataManager.getUserTable().get(userKey);

    //that was the last volume of the user, shouldn't be any record here
    Assert.assertNull(volumes);


  }

  // Create a volume and a bucket inside the volume,
  // then delete it and then check for deletion failure
  @Test
  public void testFailedDeleteVolume() throws IOException, OzoneException {
    long numVolumeCreateFails = omMetrics.getNumVolumeCreateFails();
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
        omMetrics.getNumVolumeCreateFails());

    BucketArgs bucketArgs = new BucketArgs(volumeName, bucketName, userArgs);
    storageHandler.createBucket(bucketArgs);

    try {
      storageHandler.deleteVolume(createVolumeArgs);
      Assert.fail("Expecting deletion should fail "
          + "because volume is not empty");
    } catch (OMException ex) {
      Assert.assertEquals(ResultCodes.VOLUME_NOT_EMPTY, ex.getResult());
    }
    retVolumeInfo = storageHandler.getVolumeInfo(getVolumeArgs);
    Assert.assertTrue(retVolumeInfo.getVolumeName().equals(volumeName));
    Assert.assertTrue(retVolumeInfo.getOwner().getName().equals(userName));
  }

  // Create a volume and test Volume access for a different user
  @Test
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

    Assert.assertEquals(0, omMetrics.getNumVolumeCheckAccessFails());
    Assert.assertEquals(0, omMetrics.getNumVolumeCreateFails());
  }

  @Test
  public void testCreateBucket() throws IOException, OzoneException {
    long numVolumeCreateFail = omMetrics.getNumVolumeCreateFails();
    long numBucketCreateFail = omMetrics.getNumBucketCreateFails();
    long numBucketInfoFail = omMetrics.getNumBucketInfoFails();
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
        omMetrics.getNumVolumeCreateFails());
    Assert.assertEquals(numBucketCreateFail,
        omMetrics.getNumBucketCreateFails());
    Assert.assertEquals(numBucketInfoFail,
        omMetrics.getNumBucketInfoFails());
  }

  @Test
  public void testDeleteBucket() throws Exception {
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

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> storageHandler.getBucketInfo(getBucketArgs));
  }

  @Test
  public void testDeleteNonExistingBucket() throws Exception {
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
    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> storageHandler.deleteBucket(newBucketArgs));
  }


  @Test
  public void testDeleteNonEmptyBucket() throws Exception {
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
    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_EMPTY,
        () -> storageHandler.deleteBucket(bucketArgs));

  }

  /**
   * Basic test of both putKey and getKey from OM, as one can not be tested
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
    long numKeyAllocates = omMetrics.getNumKeyAllocates();
    long numKeyLookups = omMetrics.getNumKeyLookups();

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
    Assert.assertEquals(1 + numKeyAllocates, omMetrics.getNumKeyAllocates());

    byte[] data = new byte[dataString.length()];
    try (InputStream in = storageHandler.newKeyReader(keyArgs)) {
      in.read(data);
    }
    Assert.assertEquals(dataString, DFSUtil.bytes2String(data));
    Assert.assertEquals(1 + numKeyLookups, omMetrics.getNumKeyLookups());
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
    long numKeyAllocateFails = omMetrics.getNumKeyAllocateFails();

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
    // That is this overwrite only overwrites the keys on OM. We need to
    // garbage collect those blocks from datanode.
    KeyArgs keyArgs2 = new KeyArgs(volumeName, bucketName, keyName, userArgs);
    storageHandler.newKeyWriter(keyArgs2);
    Assert
        .assertEquals(numKeyAllocateFails, omMetrics.getNumKeyAllocateFails());
  }

  /**
   * Test get a non-exiting key.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testGetNonExistKey() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyLookupFails = omMetrics.getNumKeyLookupFails();

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
    OzoneTestUtils.expectOmException(KEY_NOT_FOUND,
        () -> storageHandler.newKeyReader(keyArgs));
    Assert.assertEquals(1 + numKeyLookupFails,
        omMetrics.getNumKeyLookupFails());
  }

  /**
   * Test delete keys for om.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testDeleteKey() throws Exception {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyDeletes = omMetrics.getNumKeyDeletes();
    long numKeyDeleteFails = omMetrics.getNumKeyDeletesFails();

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
    Assert.assertEquals(1 + numKeyDeletes, omMetrics.getNumKeyDeletes());

    // Make sure the deleted key has been moved to the deleted table.
    OMMetadataManager manager = cluster.getOzoneManager().
        getMetadataManager();
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>  iter =
            manager.getDeletedTable().iterator()) {
      iter.seekToFirst();
      Table.KeyValue kv = iter.next();
      Assert.assertNotNull(kv);
    }

    // Delete the key again to test deleting non-existing key.
    OzoneTestUtils.expectOmException(KEY_NOT_FOUND,
        () -> storageHandler.deleteKey(keyArgs));
    Assert.assertEquals(1 + numKeyDeleteFails,
        omMetrics.getNumKeyDeletesFails());
  }

  /**
   * Test rename key for om.
   *
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testRenameKey() throws IOException, OzoneException {
    String userName = "user" + RandomStringUtils.randomNumeric(5);
    String adminName = "admin" + RandomStringUtils.randomNumeric(5);
    String volumeName = "volume" + RandomStringUtils.randomNumeric(5);
    String bucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    String keyName = "key" + RandomStringUtils.randomNumeric(5);
    long numKeyRenames = omMetrics.getNumKeyRenames();
    long numKeyRenameFails = omMetrics.getNumKeyRenameFails();
    int testRenameFails = 0;
    int testRenames = 0;
    OMException omException = null;

    VolumeArgs createVolumeArgs = new VolumeArgs(volumeName, userArgs);
    createVolumeArgs.setUserName(userName);
    createVolumeArgs.setAdminName(adminName);
    storageHandler.createVolume(createVolumeArgs);

    BucketArgs bucketArgs = new BucketArgs(bucketName, createVolumeArgs);
    storageHandler.createBucket(bucketArgs);

    KeyArgs keyArgs = new KeyArgs(keyName, bucketArgs);
    keyArgs.setSize(100);
    String toKeyName = "key" + RandomStringUtils.randomNumeric(5);

    // Rename from non-existent key should fail
    try {
      testRenames++;
      storageHandler.renameKey(keyArgs, toKeyName);
    } catch (OMException e) {
      testRenameFails++;
      omException = e;
    }
    Assert.assertEquals(KEY_NOT_FOUND, omException.getResult());

    // Write the contents of the key to be renamed
    String dataString = RandomStringUtils.randomAscii(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
    }

    // Rename the key
    toKeyName = "key" + RandomStringUtils.randomNumeric(5);
    testRenames++;
    storageHandler.renameKey(keyArgs, toKeyName);
    Assert.assertEquals(numKeyRenames + testRenames,
        omMetrics.getNumKeyRenames());
    Assert.assertEquals(numKeyRenameFails + testRenameFails,
        omMetrics.getNumKeyRenameFails());

    // Try to get the key, should fail as it has been renamed
    try {
      storageHandler.newKeyReader(keyArgs);
    } catch (OMException e) {
      omException = e;
    }
    Assert.assertEquals(KEY_NOT_FOUND, omException.getResult());

    // Verify the contents of the renamed key
    keyArgs = new KeyArgs(toKeyName, bucketArgs);
    InputStream in = storageHandler.newKeyReader(keyArgs);
    byte[] b = new byte[dataString.getBytes().length];
    in.read(b);
    Assert.assertEquals(new String(b), dataString);

    // Rewrite the renamed key. Rename to key which already exists should fail.
    keyArgs = new KeyArgs(keyName, bucketArgs);
    keyArgs.setSize(100);
    dataString = RandomStringUtils.randomAscii(100);
    try (OutputStream stream = storageHandler.newKeyWriter(keyArgs)) {
      stream.write(dataString.getBytes());
      stream.close();
      testRenames++;
      storageHandler.renameKey(keyArgs, toKeyName);
    } catch (OMException e) {
      testRenameFails++;
      omException = e;
    }
    Assert.assertEquals(ResultCodes.KEY_ALREADY_EXISTS,
        omException.getResult());

    // Rename to empty string should fail
    toKeyName = "";
    try {
      testRenames++;
      storageHandler.renameKey(keyArgs, toKeyName);
    } catch (OMException e) {
      testRenameFails++;
      omException = e;
    }
    Assert.assertEquals(ResultCodes.INVALID_KEY_NAME, omException.getResult());

    // Rename from empty string should fail
    keyArgs = new KeyArgs("", bucketArgs);
    toKeyName = "key" + RandomStringUtils.randomNumeric(5);
    try {
      testRenames++;
      storageHandler.renameKey(keyArgs, toKeyName);
    } catch (OMException e) {
      testRenameFails++;
      omException = e;
    }
    Assert.assertEquals(ResultCodes.INVALID_KEY_NAME, omException.getResult());

    Assert.assertEquals(numKeyRenames + testRenames,
        omMetrics.getNumKeyRenames());
    Assert.assertEquals(numKeyRenameFails + testRenameFails,
        omMetrics.getNumKeyRenameFails());
  }

  @Test
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
    } catch (OMException e) {
      Assert.assertEquals(VOLUME_NOT_FOUND, e.getResult());
    }
  }

  /**
   * Test list keys.
   * @throws IOException
   * @throws OzoneException
   */
  @Test
  public void testListKeys() throws Exception {
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

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND, () -> {
      // Provide an invalid bucket name.
      BucketArgs bucket = new BucketArgs("invalid_bucket", createVolumeArgs);
      ListArgs ks = new ListArgs(bucket, null, numKeys, null);
      storageHandler.listKeys(ks);
    });
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

    // Test list all volumes - Removed Support for this operation for time
    // being. TODO: we will need to bring this back if needed.
    UserArgs userArgs0 = new UserArgs(user0, OzoneUtils.getRequestID(),
        null, null, null, null);
    //listVolumeArgs = new ListArgs(userArgs0,"Vol-testListVolumes", 100, null);
    // listVolumeArgs.setRootScan(true);
    // volumes = storageHandler.listVolumes(listVolumeArgs);
    // Assert.assertEquals(20, volumes.getVolumes().size());

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
    Assert.assertTrue(
        (HddsClientUtils.formatDateTime(keyInfo.getCreatedOn()) / 1000) >= (
            currentTime / 1000));
    Assert.assertTrue(
        (HddsClientUtils.formatDateTime(keyInfo.getModifiedOn()) / 1000) >= (
            currentTime / 1000));
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
    ScmInfo info = cluster.getOzoneManager().getScmInfo();
    Assert.assertEquals(clusterId, info.getClusterId());
    Assert.assertEquals(scmId, info.getScmId());
  }


  //Disabling this test
  @Ignore("Disabling this test until Open Key is fixed.")
  public void testExpiredOpenKey() throws Exception {
//    BackgroundService openKeyCleanUpService = ((BlockManagerImpl)cluster
//        .getOzoneManager().getBlockManager()).getOpenKeyCleanupService();

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
    openKeys = cluster.getOzoneManager()
        .getMetadataManager().getExpiredOpenKeys();

    for (BlockGroup bg : openKeys) {
      String[] subs = bg.getGroupID().split("/");
      String keyName = subs[subs.length - 1];
      Assert.assertFalse(expected.contains(keyName));
    }

    Thread.sleep(2000);
    // Now all k1-k4 should be in ExpiredOpenKeys
    openKeys = cluster.getOzoneManager()
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

    //openKeyCleanUpService.triggerBackgroundTaskForTesting();
    Thread.sleep(2000);
    // now all k1-k4 should have been removed by the clean-up task, only k5
    // should be present in ExpiredOpenKeys.
    openKeys =
        cluster.getOzoneManager().getMetadataManager().getExpiredOpenKeys();
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
   * Tests the OM Initialization.
   * @throws IOException
   */
  @Test
  public void testOmInitialization() throws IOException {
    // Read the version file info from OM version file
    OMStorage omStorage = cluster.getOzoneManager().getOmStorage();
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf);
    // asserts whether cluster Id and SCM ID are properly set in SCM Version
    // file.
    Assert.assertEquals(clusterId, scmStorageConfig.getClusterID());
    Assert.assertEquals(scmId, scmStorageConfig.getScmId());
    // asserts whether OM Id is properly set in OM Version file.
    Assert.assertEquals(omId, omStorage.getOmId());
    // asserts whether the SCM info is correct in OM Version file.
    Assert.assertEquals(clusterId, omStorage.getClusterID());
    Assert.assertEquals(scmId, omStorage.getScmId());
  }

  /**
   * Tests the OM Initialization Failure.
   * @throws IOException
   */
  @Test
  public void testOmInitializationFailure() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    Path metaDirPath = Paths.get(path, "om-meta");
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    config.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    config.set(OZONE_OM_ADDRESS_KEY, "127.0.0.1:0");
    config.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    config.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        conf.get(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY));

    OzoneTestUtils.expectOmException(ResultCodes.OM_NOT_INITIALIZED, () -> {
      OzoneManager.createOm(null, config);
    });

    OzoneTestUtils
        .expectOmException(ResultCodes.SCM_VERSION_MISMATCH_ERROR, () -> {
          OMStorage omStore = new OMStorage(config);
          omStore.setClusterId("testClusterId");
          omStore.setScmId("testScmId");
          // writes the version file properties
          omStore.initialize();
          OzoneManager.createOm(null, config);
        });
  }

  @Test
  public void testGetServiceList() throws IOException {
    long numGetServiceListCalls = omMetrics.getNumGetServiceLists();
    List<ServiceInfo> services = cluster.getOzoneManager().getServiceList();

    Assert.assertEquals(numGetServiceListCalls + 1,
        omMetrics.getNumGetServiceLists());

    ServiceInfo omInfo = services.stream().filter(
        a -> a.getNodeType().equals(HddsProtos.NodeType.OM))
        .collect(Collectors.toList()).get(0);
    InetSocketAddress omAddress = new InetSocketAddress(omInfo.getHostname(),
        omInfo.getPort(ServicePort.Type.RPC));
    Assert.assertEquals(NetUtils.createSocketAddr(
        conf.get(OZONE_OM_ADDRESS_KEY)), omAddress);

    ServiceInfo scmInfo = services.stream().filter(
        a -> a.getNodeType().equals(HddsProtos.NodeType.SCM))
        .collect(Collectors.toList()).get(0);
    InetSocketAddress scmAddress = new InetSocketAddress(scmInfo.getHostname(),
        scmInfo.getPort(ServicePort.Type.RPC));
    Assert.assertEquals(NetUtils.createSocketAddr(
        conf.get(OZONE_SCM_CLIENT_ADDRESS_KEY)), scmAddress);
  }

  /**
   * Test that OM Ratis server is started only when OZONE_OM_RATIS_ENABLE_KEY is
   * set to true.
   */
  @Test
  public void testRatisServerOnOMInitialization() throws IOException {
    // OM Ratis server should not be started when OZONE_OM_RATIS_ENABLE_KEY
    // is not set to true
    Assert.assertNull("OM Ratis server started though OM Ratis is disabled.",
        cluster.getOzoneManager().getOmRatisServerState());

    // Enable OM Ratis and restart OM
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    cluster.restartOzoneManager();

    // On enabling OM Ratis, the Ratis server should be started
    Assert.assertEquals("OM Ratis server did not start",
        LifeCycle.State.RUNNING,
        cluster.getOzoneManager().getOmRatisServerState());
  }

  @Test
  public void testVersion() {
    String expectedVersion = OzoneVersionInfo.OZONE_VERSION_INFO.getVersion();
    String actualVersion = cluster.getOzoneManager().getSoftwareVersion();
    Assert.assertEquals(expectedVersion, actualVersion);
  }
}
