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

package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueBlockIterator;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProvider;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.s3.util.OzoneS3Util;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneAclConfig;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;

import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract class to test all the public facing APIs of Ozone
 * Client, w/o OM Ratis server.
 * {@link TestOzoneRpcClient} tests the Ozone Client by submitting the
 * requests directly to OzoneManager. {@link TestOzoneRpcClientWithRatis}
 * tests the Ozone Client by submitting requests to OM's Ratis server.
 */
public abstract class TestOzoneRpcClientAbstract {

  static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneRpcClientAbstract.class);
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static OzoneManager ozoneManager;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  private static String remoteUserName = "remoteUser";
  private static OzoneAcl defaultUserAcl = new OzoneAcl(USER, remoteUserName,
      READ, DEFAULT);
  private static OzoneAcl defaultGroupAcl = new OzoneAcl(GROUP, remoteUserName,
      READ, DEFAULT);
  private static OzoneAcl inheritedUserAcl = new OzoneAcl(USER, remoteUserName,
      READ, ACCESS);
  private static OzoneAcl inheritedGroupAcl = new OzoneAcl(GROUP,
      remoteUserName, READ, ACCESS);

  private static String scmId = UUID.randomUUID().toString();

  /**
   * Create a MiniOzoneCluster for testing.
   * @param conf Configurations to start the cluster.
   * @throws Exception
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .setScmId(scmId)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.getStorageContainerLocationClient();
    ozoneManager = cluster.getOzoneManager();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static void setCluster(MiniOzoneCluster cluster) {
    TestOzoneRpcClientAbstract.cluster = cluster;
  }

  public static void setOzClient(OzoneClient ozClient) {
    TestOzoneRpcClientAbstract.ozClient = ozClient;
  }

  public static void setOzoneManager(OzoneManager ozoneManager){
    TestOzoneRpcClientAbstract.ozoneManager = ozoneManager;
  }

  public static void setStorageContainerLocationClient(
      StorageContainerLocationProtocolClientSideTranslatorPB
          storageContainerLocationClient) {
    TestOzoneRpcClientAbstract.storageContainerLocationClient =
        storageContainerLocationClient;
  }

  public static void setStore(ObjectStore store) {
    TestOzoneRpcClientAbstract.store = store;
  }

  public static ObjectStore getStore() {
    return TestOzoneRpcClientAbstract.store;
  }

  public static void setScmId(String scmId){
    TestOzoneRpcClientAbstract.scmId = scmId;
  }

  /**
   * Test OM Proxy Provider.
   */
  @Test
  public void testOMClientProxyProvider() {
    OMFailoverProxyProvider omFailoverProxyProvider = store.getClientProxy()
        .getOMProxyProvider();
    List<OMProxyInfo> omProxies = omFailoverProxyProvider.getOMProxyInfos();

    // For a non-HA OM service, there should be only one OM proxy.
    Assert.assertEquals(1, omProxies.size());
    // The address in OMProxyInfo object, which client will connect to,
    // should match the OM's RPC address.
    Assert.assertTrue(omProxies.get(0).getAddress().equals(
        ozoneManager.getOmRpcServerAddr()));
  }

  @Test
  public void testSetVolumeQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).setQuota(
        OzoneQuota.parseQuota("100000000 BYTES"));
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(100000000L, volume.getQuota());
  }

  @Test
  public void testDeleteVolume()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertNotNull(volume);
    store.deleteVolume(volumeName);
    OzoneTestUtils.expectOmException(ResultCodes.VOLUME_NOT_FOUND,
        () -> store.getVolume(volumeName));

  }

  @Test
  public void testCreateVolumeWithMetadata()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs volumeArgs = VolumeArgs.newBuilder()
        .addMetadata("key1", "val1")
        .build();
    store.createVolume(volumeName, volumeArgs);
    OzoneVolume volume = store.getVolume(volumeName);

    Assert.assertEquals("val1", volume.getMetadata().get("key1"));
    Assert.assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateBucketWithMetadata()
      throws IOException, OzoneException {
    long currentTime = Time.now();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs args = BucketArgs.newBuilder()
        .addMetadata("key1", "value1").build();
    volume.createBucket(bucketName, args);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertNotNull(bucket.getMetadata());
    Assert.assertEquals("value1", bucket.getMetadata().get("key1"));

  }


  @Test
  public void testCreateBucket()
      throws IOException, OzoneException {
    long currentTime = Time.now();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getCreationTime() >= currentTime);
    Assert.assertTrue(volume.getCreationTime() >= currentTime);
  }

  @Test
  public void testCreateS3Bucket()
      throws IOException, OzoneException {
    long currentTime = Time.now();
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(userName, bucketName);
    String volumeName = store.getOzoneVolumeName(bucketName);
    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getCreationTime() >= currentTime);
    Assert.assertTrue(volume.getCreationTime() >= currentTime);
  }

  @Test
  public void testCreateSecureS3Bucket() throws IOException {
    long currentTime = Time.now();
    String userName = "ozone/localhost@EXAMPLE.COM";
    String bucketName = UUID.randomUUID().toString();
    String s3VolumeName = OzoneS3Util.getVolumeName(userName);
    store.createS3Bucket(s3VolumeName, bucketName);
    String volumeName = store.getOzoneVolumeName(bucketName);
    assertEquals(volumeName, "s3" + s3VolumeName);

    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getCreationTime() >= currentTime);
    Assert.assertTrue(volume.getCreationTime() >= currentTime);
  }


  @Test
  public void testListS3Buckets()
      throws IOException, OzoneException {
    String userName = "ozone100";
    String bucketName1 = UUID.randomUUID().toString();
    String bucketName2 = UUID.randomUUID().toString();
    store.createS3Bucket(userName, bucketName1);
    store.createS3Bucket(userName, bucketName2);
    Iterator<? extends OzoneBucket> iterator = store.listS3Buckets(userName,
        null);

    while (iterator.hasNext()) {
      assertThat(iterator.next().getName(), either(containsString(bucketName1))
          .or(containsString(bucketName2)));
    }

  }

  @Test
  public void testListS3BucketsFail()
      throws IOException, OzoneException {
    String userName = "randomUser";
    Iterator<? extends OzoneBucket> iterator = store.listS3Buckets(userName,
        null);

    Assert.assertFalse(iterator.hasNext());

  }

  @Test
  public void testDeleteS3Bucket()
      throws Exception {
    long currentTime = Time.now();
    String userName = "ozone1";
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(userName, bucketName);
    String volumeName = store.getOzoneVolumeName(bucketName);
    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getCreationTime() >= currentTime);
    Assert.assertTrue(volume.getCreationTime() >= currentTime);
    store.deleteS3Bucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.S3_BUCKET_NOT_FOUND,
        () -> store.getOzoneVolumeName(bucketName));
  }

  @Test
  public void testDeleteS3NonExistingBucket() {
    try {
      store.deleteS3Bucket(UUID.randomUUID().toString());
    } catch (IOException ex) {
      GenericTestUtils.assertExceptionContains("NOT_FOUND", ex);
    }
  }

  @Test
  public void testCreateS3BucketMapping()
      throws IOException, OzoneException {
    long currentTime = Time.now();
    String userName = "ozone";
    String bucketName = UUID.randomUUID().toString();
    store.createS3Bucket(userName, bucketName);
    String volumeName = store.getOzoneVolumeName(bucketName);
    OzoneVolume volume = store.getVolume(volumeName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());

    String mapping = store.getOzoneBucketMapping(bucketName);
    Assert.assertEquals("s3"+userName+"/"+bucketName, mapping);
    Assert.assertEquals(bucketName, store.getOzoneBucketName(bucketName));
    Assert.assertEquals("s3"+userName, store.getOzoneVolumeName(bucketName));

  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
  }

  @Test
  public void testCreateBucketWithStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.SSD);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        READ, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setVersioning(true)
        .setStorageType(StorageType.SSD)
        .setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
    Assert.assertEquals(true, bucket.getVersioning());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testInvalidBucketCreation() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = "invalid#bucket";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Bucket or Volume name has an unsupported" +
            " character : #",
        () -> volume.createBucket(bucketName));

  }

  @Test
  public void testAddBucketAcl()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(new OzoneAcl(USER, "test", ACLType.ALL, ACCESS));
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.addAcls(acls);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testRemoveBucketAcl()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.removeAcls(acls);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertTrue(!bucket.getAcls().contains(acls.get(0)));
  }

  @Test
  public void testRemoveBucketAclUsingRpcClientRemoveAcl()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(USER, "test",
        ACLType.ALL, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(userAcl);
    acls.add(new OzoneAcl(USER, "test1",
        ACLType.ALL, ACCESS));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setAcls(acls);
    volume.createBucket(bucketName, builder.build());
    OzoneObj ozoneObj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setResType(OzoneObj.ResourceType.BUCKET).build();

    // Remove the 2nd acl added to the list.
    boolean remove = store.removeAcl(ozoneObj, acls.get(1));
    Assert.assertTrue(remove);
    Assert.assertFalse(store.getAcl(ozoneObj).contains(acls.get(1)));

    remove = store.removeAcl(ozoneObj, acls.get(0));
    Assert.assertTrue(remove);
    Assert.assertFalse(store.getAcl(ozoneObj).contains(acls.get(0)));
  }

  @Test
  public void testSetBucketVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setVersioning(true);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(true, newBucket.getVersioning());
  }

  @Test
  public void testSetBucketStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    bucket.setStorageType(StorageType.SSD);
    OzoneBucket newBucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, newBucket.getName());
    Assert.assertEquals(StorageType.SSD, newBucket.getStorageType());
  }


  @Test
  public void testDeleteBucket()
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertNotNull(bucket);
    volume.deleteBucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.BUCKET_NOT_FOUND,
        () -> volume.getBucket(bucketName)
    );
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setRefreshPipeline(true)
        .build();
    HddsProtos.ReplicationType replicationType =
        HddsProtos.ReplicationType.valueOf(type.toString());
    HddsProtos.ReplicationFactor replicationFactor =
        HddsProtos.ReplicationFactor.valueOf(factor.getValue());
    OmKeyInfo keyInfo = ozoneManager.lookupKey(keyArgs);
    for (OmKeyLocationInfo info:
        keyInfo.getLatestVersionLocations().getLocationList()) {
      ContainerInfo container =
          storageContainerLocationClient.getContainer(info.getContainerID());
      if (!container.getReplicationFactor().equals(replicationFactor) || (
          container.getReplicationType() != replicationType)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testPutKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, STAND_ALONE,
          ONE, new HashMap<>());
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, STAND_ALONE,
          ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }

  @Test
  public void testValidateBlockLengthWithCommitKey() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(RandomUtils.nextInt(0, 1024));
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName, 0,
        STAND_ALONE, ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);
    OmKeyInfo keyInfo = ozoneManager.lookupKey(builder.build());

    List<OmKeyLocationInfo> locationInfoList =
        keyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly();
    // LocationList should have only 1 block
    Assert.assertEquals(1, locationInfoList.size());
    // make sure the data block size is updated
    Assert.assertEquals(value.getBytes().length,
        locationInfoList.get(0).getLength());
    // make sure the total data size is set correctly
    Assert.assertEquals(value.getBytes().length, keyInfo.getDataSize());
  }

  @Test
  public void testPutKeyRatisOneNode()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, ReplicationType.RATIS,
          ONE, new HashMap<>());
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS, ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }

  @Test
  public void testPutKeyRatisThreeNodes()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    for (int i = 0; i < 10; i++) {
      String keyName = UUID.randomUUID().toString();

      OzoneOutputStream out = bucket.createKey(keyName,
          value.getBytes().length, ReplicationType.RATIS,
          ReplicationFactor.THREE, new HashMap<>());
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS,
          ReplicationFactor.THREE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
  }


  @Ignore("Debug Jenkins Timeout")
  @Test
  public void testPutKeyRatisThreeNodesParallel() throws IOException,
      InterruptedException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    long currentTime = Time.now();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger failCount = new AtomicInteger(0);

    Runnable r = () -> {
      try {
        for (int i = 0; i < 5; i++) {
          String keyName = UUID.randomUUID().toString();
          String data = generateData(5 * 1024 * 1024,
              (byte) RandomUtils.nextLong()).toString();
          OzoneOutputStream out = bucket.createKey(keyName,
              data.getBytes().length, ReplicationType.RATIS,
              ReplicationFactor.THREE, new HashMap<>());
          out.write(data.getBytes());
          out.close();
          OzoneKey key = bucket.getKey(keyName);
          Assert.assertEquals(keyName, key.getName());
          OzoneInputStream is = bucket.readKey(keyName);
          byte[] fileContent = new byte[data.getBytes().length];
          is.read(fileContent);
          is.close();
          Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
              keyName, ReplicationType.RATIS,
              ReplicationFactor.THREE));
          Assert.assertEquals(data, new String(fileContent));
          Assert.assertTrue(key.getCreationTime() >= currentTime);
          Assert.assertTrue(key.getModificationTime() >= currentTime);
        }
        latch.countDown();
      } catch (IOException ex) {
        latch.countDown();
        failCount.incrementAndGet();
      }
    };

    Thread thread1 = new Thread(r);
    Thread thread2 = new Thread(r);

    thread1.start();
    thread2.start();

    latch.await(600, TimeUnit.SECONDS);

    if (failCount.get() > 0) {
      fail("testPutKeyRatisThreeNodesParallel failed");
    }

  }


  @Test
  public void testReadKeyWithVerifyChecksumFlagEnable() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Create and corrupt key
    createAndCorruptKey(volumeName, bucketName, keyName);

    // read corrupt key with verify checksum enabled
    readCorruptedKey(volumeName, bucketName, keyName, true);

  }


  @Test
  public void testReadKeyWithVerifyChecksumFlagDisable() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Create and corrupt key
    createAndCorruptKey(volumeName, bucketName, keyName);

    // read corrupt key with verify checksum enabled
    readCorruptedKey(volumeName, bucketName, keyName, false);

  }

  private void createAndCorruptKey(String volumeName, String bucketName,
      String keyName) throws IOException {
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.RATIS,
        ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    Assert.assertNotNull("Container not found", container);
    corruptData(container, key);
  }


  private void readCorruptedKey(String volumeName, String bucketName,
      String keyName, boolean verifyChecksum) throws IOException {
    try {
      Configuration configuration = cluster.getConf();
      configuration.setBoolean(OzoneConfigKeys.OZONE_CLIENT_VERIFY_CHECKSUM,
          verifyChecksum);
      RpcClient client = new RpcClient(configuration);
      OzoneInputStream is = client.getKey(volumeName, bucketName, keyName);
      is.read(new byte[100]);
      is.close();
      if (verifyChecksum) {
        fail("Reading corrupted data should fail, as verify checksum is " +
            "enabled");
      }
    } catch (IOException e) {
      if (!verifyChecksum) {
        fail("Reading corrupted data should not fail, as verify checksum is " +
            "disabled");
      }
    }
  }


  private void readKey(OzoneBucket bucket, String keyName, String data)
      throws IOException {
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[data.getBytes().length];
    is.read(fileContent);
    is.close();
  }

  @Test
  public void testGetKeyDetails() throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();
    String keyValue = RandomStringUtils.random(128);
    //String keyValue = "this is a test value.glx";
    // create the initial key with size 0, write will allocate the first block.
    OzoneOutputStream out = bucket.createKey(keyName,
        keyValue.getBytes().length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(keyValue.getBytes());
    out.close();

    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[32];
    is.read(fileContent);

    // First, confirm the key info from the client matches the info in OM.
    OmKeyArgs.Builder builder = new OmKeyArgs.Builder();
    builder.setVolumeName(volumeName).setBucketName(bucketName)
        .setKeyName(keyName).setRefreshPipeline(true);
    OmKeyLocationInfo keyInfo = ozoneManager.lookupKey(builder.build()).
        getKeyLocationVersions().get(0).getBlocksLatestVersionOnly().get(0);
    long containerID = keyInfo.getContainerID();
    long localID = keyInfo.getLocalID();
    OzoneKeyDetails keyDetails = (OzoneKeyDetails)bucket.getKey(keyName);
    Assert.assertEquals(keyName, keyDetails.getName());

    List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
    Assert.assertEquals(1, keyLocations.size());
    Assert.assertEquals(containerID, keyLocations.get(0).getContainerID());
    Assert.assertEquals(localID, keyLocations.get(0).getLocalID());

    // Make sure that the data size matched.
    Assert.assertEquals(keyValue.getBytes().length,
        keyLocations.get(0).getLength());

    // Second, sum the data size from chunks in Container via containerID
    // and localID, make sure the size equals to the size from keyDetails.
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainer(ContainerID.valueof(containerID));
    Pipeline pipeline = cluster.getStorageContainerManager()
        .getPipelineManager().getPipeline(container.getPipelineID());
    List<DatanodeDetails> datanodes = pipeline.getNodes();
    Assert.assertEquals(datanodes.size(), 1);

    DatanodeDetails datanodeDetails = datanodes.get(0);
    Assert.assertNotNull(datanodeDetails);
    HddsDatanodeService datanodeService = null;
    for (HddsDatanodeService datanodeServiceItr : cluster.getHddsDatanodes()) {
      if (datanodeDetails.equals(datanodeServiceItr.getDatanodeDetails())) {
        datanodeService = datanodeServiceItr;
        break;
      }
    }
    KeyValueContainerData containerData =
        (KeyValueContainerData)(datanodeService.getDatanodeStateMachine()
            .getContainer().getContainerSet().getContainer(containerID)
            .getContainerData());
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath))) {
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          long length = 0;
          List<ContainerProtos.ChunkInfo> chunks = blockData.getChunks();
          for (ContainerProtos.ChunkInfo chunk : chunks) {
            length += chunk.getLen();
          }
          Assert.assertEquals(length, keyValue.getBytes().length);
          break;
        }
      }
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testReadKeyWithCorruptedData() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.RATIS,
        ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes. Atleast one of the
    // datanode must have this container.
    Container container = null;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        break;
      }
    }
    Assert.assertNotNull("Container not found", container);
    corruptData(container, key);

    // Try reading the key. Since the chunk file is corrupted, it should
    // throw a checksum mismatch exception.
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      is.read(new byte[100]);
      fail("Reading corrupted data should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Checksum mismatch", e);
    }
  }

  /**
   * Tests reading a corrputed chunk file throws checksum exception.
   * @throws IOException
   */
  @Test
  public void testReadKeyWithCorruptedDataWithMutiNodes() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = "sample value";
    byte[] data = value.getBytes();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    String keyName = UUID.randomUUID().toString();

    // Write data into a key
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.RATIS,
        ReplicationFactor.THREE, new HashMap<>());
    out.write(value.getBytes());
    out.close();

    // We need to find the location of the chunk file corresponding to the
    // data we just wrote.
    OzoneKey key = bucket.getKey(keyName);
    List<OzoneKeyLocation> keyLocation =
        ((OzoneKeyDetails) key).getOzoneKeyLocations();
    Assert.assertTrue("Key location not found in OM", !keyLocation.isEmpty());
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();

    // Get the container by traversing the datanodes.
    List<Container> containerList = new ArrayList<>();
    Container container;
    for (HddsDatanodeService hddsDatanode : cluster.getHddsDatanodes()) {
      container = hddsDatanode.getDatanodeStateMachine().getContainer()
          .getContainerSet().getContainer(containerID);
      if (container != null) {
        containerList.add(container);
        if (containerList.size() == 3) {
          break;
        }
      }
    }
    Assert.assertTrue("Container not found", !containerList.isEmpty());
    corruptData(containerList.get(0), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, data));
    } catch (OzoneChecksumException e) {
      fail("Reading corrupted data should not fail.");
    }
    corruptData(containerList.get(1), key);
    // Try reading the key. Read will fail on the first node and will eventually
    // failover to next replica
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      Assert.assertTrue(Arrays.equals(b, data));
    } catch (OzoneChecksumException e) {
      fail("Reading corrupted data should not fail.");
    }
    corruptData(containerList.get(2), key);
    // Try reading the key. Read will fail here as all the replica are corrupt
    try {
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] b = new byte[data.length];
      is.read(b);
      fail("Reading corrupted data should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Checksum mismatch", e);
    }
  }

  private void corruptData(Container container, OzoneKey key)
      throws IOException {
    long containerID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getContainerID();
    long localID = ((OzoneKeyDetails) key).getOzoneKeyLocations().get(0)
        .getLocalID();
    // From the containerData, get the block iterator for all the blocks in
    // the container.
    KeyValueContainerData containerData =
        (KeyValueContainerData) container.getContainerData();
    String containerPath =
        new File(containerData.getMetadataPath()).getParent();
    try (KeyValueBlockIterator keyValueBlockIterator =
        new KeyValueBlockIterator(containerID, new File(containerPath))) {

      // Find the block corresponding to the key we put. We use the localID of
      // the BlockData to identify out key.
      BlockData blockData = null;
      while (keyValueBlockIterator.hasNext()) {
        blockData = keyValueBlockIterator.nextBlock();
        if (blockData.getBlockID().getLocalID() == localID) {
          break;
        }
      }
      Assert.assertNotNull("Block not found", blockData);

      // Get the location of the chunk file
      String chunkName = blockData.getChunks().get(0).getChunkName();
      String containreBaseDir =
          container.getContainerData().getVolume().getHddsRootDir().getPath();
      File chunksLocationPath = KeyValueContainerLocationUtil
          .getChunksLocationPath(containreBaseDir, scmId, containerID);
      File chunkFile = new File(chunksLocationPath, chunkName);

      // Corrupt the contents of the chunk file
      String newData = new String("corrupted data");
      FileUtils.writeByteArrayToFile(chunkFile, newData.getBytes());
    }
  }

  @Test
  public void testDeleteKey()
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    bucket.deleteKey(keyName);

    OzoneTestUtils.expectOmException(ResultCodes.KEY_NOT_FOUND,
        () -> bucket.getKey(keyName));
  }

  @Test
  public void testRenameKey()
      throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String fromKeyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(fromKeyName,
        value.getBytes().length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(value.getBytes());
    out.close();
    OzoneKey key = bucket.getKey(fromKeyName);
    Assert.assertEquals(fromKeyName, key.getName());

    // Rename to empty string should fail.
    OMException oe = null;
    String toKeyName = "";
    try {
      bucket.renameKey(fromKeyName, toKeyName);
    } catch (OMException e) {
      oe = e;
    }
    Assert.assertEquals(ResultCodes.INVALID_KEY_NAME, oe.getResult());

    toKeyName = UUID.randomUUID().toString();
    bucket.renameKey(fromKeyName, toKeyName);

    // Lookup for old key should fail.
    try {
      bucket.getKey(fromKeyName);
    } catch (OMException e) {
      oe = e;
    }
    Assert.assertEquals(ResultCodes.KEY_NOT_FOUND, oe.getResult());

    key = bucket.getKey(toKeyName);
    Assert.assertEquals(toKeyName, key.getName());
  }

  // Listing all volumes in the cluster feature has to be fixed after HDDS-357.
  // TODO: fix this
  @Ignore
  @Test
  public void testListVolume() throws IOException {
    String volBase = "vol-" + RandomStringUtils.randomNumeric(3);
    //Create 10 volume vol-<random>-a-0-<random> to vol-<random>-a-9-<random>
    String volBaseNameA = volBase + "-a-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 volume vol-<random>-b-0-<random> to vol-<random>-b-9-<random>
    String volBaseNameB = volBase + "-b-";
    for(int i = 0; i < 10; i++) {
      store.createVolume(
          volBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneVolume> volIterator = store.listVolumes(volBase);
    int totalVolumeCount = 0;
    while(volIterator.hasNext()) {
      volIterator.next();
      totalVolumeCount++;
    }
    Assert.assertEquals(20, totalVolumeCount);
    Iterator<? extends OzoneVolume> volAIterator = store.listVolumes(
        volBaseNameA);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volAIterator.next().getName()
          .startsWith(volBaseNameA + i + "-"));
    }
    Assert.assertFalse(volAIterator.hasNext());
    Iterator<? extends OzoneVolume> volBIterator = store.listVolumes(
        volBaseNameB);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBIterator.next().getName()
          .startsWith(volBaseNameB + i + "-"));
    }
    Assert.assertFalse(volBIterator.hasNext());
    Iterator<? extends OzoneVolume> iter = store.listVolumes(volBaseNameA +
        "1-");
    Assert.assertTrue(iter.next().getName().startsWith(volBaseNameA + "1-"));
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testListBucket()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);

    //Create 10 buckets in  vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameA = "bucket-a-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameA + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    //Create 10 buckets in vol-a-<random> and 10 in vol-b-<random>
    String bucketBaseNameB = "bucket-b-";
    for(int i = 0; i < 10; i++) {
      volA.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
      volB.createBucket(
          bucketBaseNameB + i + "-" + RandomStringUtils.randomNumeric(5));
    }
    Iterator<? extends OzoneBucket> volABucketIter =
        volA.listBuckets("bucket-");
    int volABucketCount = 0;
    while(volABucketIter.hasNext()) {
      volABucketIter.next();
      volABucketCount++;
    }
    Assert.assertEquals(20, volABucketCount);
    Iterator<? extends OzoneBucket> volBBucketIter =
        volA.listBuckets("bucket-");
    int volBBucketCount = 0;
    while(volBBucketIter.hasNext()) {
      volBBucketIter.next();
      volBBucketCount++;
    }
    Assert.assertEquals(20, volBBucketCount);

    Iterator<? extends OzoneBucket> volABucketAIter =
        volA.listBuckets("bucket-a-");
    int volABucketACount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketACount++;
    }
    Assert.assertEquals(10, volABucketACount);
    Iterator<? extends OzoneBucket> volBBucketBIter =
        volA.listBuckets("bucket-b-");
    int volBBucketBCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBCount++;
    }
    Assert.assertEquals(10, volBBucketBCount);
    Iterator<? extends OzoneBucket> volABucketBIter = volA.listBuckets(
        "bucket-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketBIter.next().getName()
          .startsWith(bucketBaseNameB + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
    Iterator<? extends OzoneBucket> volBBucketAIter = volB.listBuckets(
        "bucket-a-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBBucketAIter.next().getName()
          .startsWith(bucketBaseNameA + i + "-"));
    }
    Assert.assertFalse(volBBucketAIter.hasNext());

  }

  @Test
  public void testListBucketsOnEmptyVolume()
      throws IOException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    Iterator<? extends OzoneBucket> buckets = vol.listBuckets("");
    while(buckets.hasNext()) {
      fail();
    }
  }

  @Test
  public void testListKey()
      throws IOException {
    String volumeA = "vol-a-" + RandomStringUtils.randomNumeric(5);
    String volumeB = "vol-b-" + RandomStringUtils.randomNumeric(5);
    String bucketA = "buc-a-" + RandomStringUtils.randomNumeric(5);
    String bucketB = "buc-b-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volumeA);
    store.createVolume(volumeB);
    OzoneVolume volA = store.getVolume(volumeA);
    OzoneVolume volB = store.getVolume(volumeB);
    volA.createBucket(bucketA);
    volA.createBucket(bucketB);
    volB.createBucket(bucketA);
    volB.createBucket(bucketB);
    OzoneBucket volAbucketA = volA.getBucket(bucketA);
    OzoneBucket volAbucketB = volA.getBucket(bucketB);
    OzoneBucket volBbucketA = volB.getBucket(bucketA);
    OzoneBucket volBbucketB = volB.getBucket(bucketB);

    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseA = "key-a-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes();
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }
    /*
    Create 10 keys in  vol-a-<random>/buc-a-<random>,
    vol-a-<random>/buc-b-<random>, vol-b-<random>/buc-a-<random> and
    vol-b-<random>/buc-b-<random>
     */
    String keyBaseB = "key-b-";
    for (int i = 0; i < 10; i++) {
      byte[] value = RandomStringUtils.randomAscii(10240).getBytes();
      OzoneOutputStream one = volAbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, STAND_ALONE, ONE,
          new HashMap<>());
      four.write(value);
      four.close();
    }
    Iterator<? extends OzoneKey> volABucketAIter =
        volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);
    Iterator<? extends OzoneKey> volABucketBIter =
        volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while(volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);
    Iterator<? extends OzoneKey> volBBucketAIter =
        volBbucketA.listKeys("key-");
    int volBBucketAKeyCount = 0;
    while(volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);
    Iterator<? extends OzoneKey> volBBucketBIter =
        volBbucketB.listKeys("key-");
    int volBBucketBKeyCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);
    Iterator<? extends OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while(volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<? extends OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys("key-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith("key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<? extends OzoneKey> keys = buc.listKeys("");
    while(keys.hasNext()) {
      fail();
    }
  }

  @Test
  public void testInitiateMultipartUploadWithReplicationInformationSet() throws
      IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }


  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
      IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }


  @Test
  public void testUploadPartWithNoOverride() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(DFSUtil.string2Bytes(sampleData), 0,
        sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

  }

  @Test
  public void testUploadPartOverrideWithStandAlone() throws IOException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";
    int partNumber = 1;

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        STAND_ALONE, ONE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(DFSUtil.string2Bytes(sampleData), 0,
        sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

    //Overwrite the part by creating part key with same part number.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(DFSUtil.string2Bytes(sampleData), 0, "name"
        .length());
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getPartName());

    // PartName should be different from old part Name.
    assertNotEquals("Part names should be different", partName,
        commitUploadPartInfo.getPartName());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws IOException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        ReplicationType.RATIS, ReplicationFactor.THREE);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    int partNumber = 1;

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(DFSUtil.string2Bytes(sampleData), 0,
        sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    assertNotNull(commitUploadPartInfo.getPartName());

    //Overwrite the part by creating part key with same part number.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(DFSUtil.string2Bytes(sampleData), 0, "name"
        .length());
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getPartName());

    // PartName should be different from old part Name.
    assertNotEquals("Part names should be different", partName,
        commitUploadPartInfo.getPartName());
  }

  @Test
  public void testNoSuchUploadError() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = "random";
    OzoneTestUtils
        .expectOmException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR, () ->
            bucket
                .createMultipartKey(keyName, sampleData.length(), 1, uploadID));
  }

  @Test
  public void testMultipartUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    doMultipartUpload(bucket, keyName, (byte)98);
  }


  @Test
  public void testMultipartUploadOverride() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    doMultipartUpload(bucket, keyName, (byte)96);

    // Initiate Multipart upload again, now we should read latest version, as
    // read always reads latest blocks.
    doMultipartUpload(bucket, keyName, (byte)97);

  }


  @Test
  public void testMultipartUploadWithPartsLessThanMinSize() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    // Upload Parts
    Map<Integer, String> partsMap = new TreeMap<>();
    // Uploading part 1 with less than min size
    String partName = uploadPart(bucket, keyName, uploadID, 1,
        "data".getBytes(UTF_8));
    partsMap.put(1, partName);

    partName = uploadPart(bucket, keyName, uploadID, 2,
        "data".getBytes(UTF_8));
    partsMap.put(2, partName);


    // Complete multipart upload

    OzoneTestUtils.expectOmException(ResultCodes.ENTITY_TOO_SMALL,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }
  @Test
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(ResultCodes.MISMATCH_MULTIPART_LIST,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }

  @Test
  public void testMultipartUploadWithPartsMisMatchWithIncorrectPartName()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(ResultCodes.MISMATCH_MULTIPART_LIST,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));

  }

  @Test
  public void testMultipartUploadWithMissingParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(3, "random");

    OzoneTestUtils.expectOmException(ResultCodes.MISSING_UPLOAD_PARTS,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testAbortUploadFail() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OzoneTestUtils.expectOmException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        () -> bucket.abortMultipartUpload(keyName, "random"));
  }


  @Test
  public void testAbortUploadSuccessWithOutAnyParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testAbortUploadSuccessWithParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testListMultipartUploadParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());
    Assert.assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(2).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(2)
            .getPartName());

    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  @Test
  public void testListMultipartUploadPartsWithContinuation()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());

    Assert.assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    Assert.assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    Assert.assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsInvalidPartMarker() throws Exception {
    try {
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);


      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          bucket.listParts(keyName, "random", -1, 2);
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Should be greater than or " +
          "equal to zero", ex);
    }
  }

  @Test
  public void testListPartsInvalidMaxParts() throws Exception {
    try {
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);


      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          bucket.listParts(keyName, "random", 1,  -1);
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Max Parts Should be greater " +
          "than zero", ex);
    }
  }

  @Test
  public void testListPartsWithPartMarkerGreaterThanPartCount()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);


    String uploadID = initiateMultipartUpload(bucket, keyName, STAND_ALONE,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty

    Assert.assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(STAND_ALONE,
        ozoneMultipartUploadPartListParts.getReplicationType());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsWithInvalidUploadID() throws Exception {
    OzoneTestUtils
        .expectOmException(ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR, () -> {
          String volumeName = UUID.randomUUID().toString();
          String bucketName = UUID.randomUUID().toString();
          String keyName = UUID.randomUUID().toString();

          store.createVolume(volumeName);
          OzoneVolume volume = store.getVolume(volumeName);
          volume.createBucket(bucketName);
          OzoneBucket bucket = volume.getBucket(bucketName);
          OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
              bucket.listParts(keyName, "random", 100, 2);
        });
  }

  @Test
  public void testNativeAclsForVolume() throws Exception {
    assumeFalse("Remove this once ACL HA is supported",
        getClass().equals(TestOzoneRpcClientWithRatis.class));
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateOzoneAccessAcl(ozObj);
  }

  @Test
  public void testNativeAclsForBucket() throws Exception {
    assumeFalse("Remove this once ACL HA is supported",
        getClass().equals(TestOzoneRpcClientWithRatis.class));
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateOzoneAccessAcl(ozObj);

    OzoneObj volObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setResType(OzoneObj.ResourceType.VOLUME)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    validateDefaultAcls(volObj, ozObj, volume, null);
  }

  private void validateDefaultAcls(OzoneObj parentObj, OzoneObj childObj,
      OzoneVolume volume,  OzoneBucket bucket) throws Exception {
    assertTrue(store.addAcl(parentObj, defaultUserAcl));
    assertTrue(store.addAcl(parentObj, defaultGroupAcl));
    if (volume != null) {
      volume.deleteBucket(childObj.getBucketName());
      volume.createBucket(childObj.getBucketName());
    } else {
      if (childObj.getResourceType().equals(OzoneObj.ResourceType.KEY)) {
        bucket.deleteKey(childObj.getKeyName());
        writeKey(childObj.getKeyName(), bucket);
      } else {
        store.setAcl(childObj, getAclList(new OzoneConfiguration()));
      }
    }
    List<OzoneAcl> acls = store.getAcl(parentObj);
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedUserAcl:" + inheritedUserAcl,
        acls.contains(defaultUserAcl));
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedUserAcl:" + inheritedUserAcl,
        acls.contains(defaultGroupAcl));

    acls = store.getAcl(childObj);
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedUserAcl:" + inheritedUserAcl,
        acls.contains(inheritedUserAcl));
    assertTrue("Current acls:" + StringUtils.join(",", acls) +
            " inheritedUserAcl:" + inheritedUserAcl,
        acls.contains(inheritedGroupAcl));
  }

  @Test
  public void testNativeAclsForKey() throws Exception {
    assumeFalse("Remove this once ACL HA is supported",
        getClass().equals(TestOzoneRpcClientWithRatis.class));
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String key1 = "dir1/dir2" + UUID.randomUUID().toString();
    String key2 = "dir1/dir2" + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    writeKey(key1, bucket);
    writeKey(key2, bucket);

    OzoneObj ozObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // Validates access acls.
    validateOzoneAccessAcl(ozObj);

    // Check default acls inherited from bucket.
    OzoneObj buckObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.BUCKET)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    validateDefaultAcls(buckObj, ozObj, null, bucket);

    // Check default acls inherited from prefix.
    OzoneObj prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setPrefixName("dir1/")
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    store.setAcl(prefixObj, getAclList(new OzoneConfiguration()));
    // Prefix should inherit DEFAULT acl from bucket.

    List<OzoneAcl> acls = store.getAcl(prefixObj);
    assertTrue("Current acls:" + StringUtils.join(",", acls),
        acls.contains(inheritedUserAcl));
    assertTrue("Current acls:" + StringUtils.join(",", acls),
        acls.contains(inheritedGroupAcl));
    // Remove inherited acls from prefix.
    assertTrue(store.removeAcl(prefixObj, inheritedUserAcl));
    assertTrue(store.removeAcl(prefixObj, inheritedGroupAcl));

    validateDefaultAcls(prefixObj, ozObj, null, bucket);
  }

  @Test
  public void testNativeAclsForPrefix() throws Exception {
    assumeFalse("Remove this once ACL HA is supported",
        getClass().equals(TestOzoneRpcClientWithRatis.class));
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String prefix1 = "PF" + UUID.randomUUID().toString() + "/";
    String key1 = prefix1 + "KEY" + UUID.randomUUID().toString();

    String prefix2 = "PF" + UUID.randomUUID().toString() + "/";
    String key2 = prefix2 + "KEY" + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    assertNotNull("Bucket creation failed", bucket);

    writeKey(key1, bucket);
    writeKey(key2, bucket);

    OzoneObj prefixObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix1)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneObj prefixObj2 = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setPrefixName(prefix2)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // add acl
    BitSet aclRights1 = new BitSet();
    aclRights1.set(READ.ordinal());
    OzoneAcl user1Acl = new OzoneAcl(USER,
        "user1", aclRights1, ACCESS);
    assertTrue(store.addAcl(prefixObj, user1Acl));

    // get acl
    List<OzoneAcl> aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(1, aclsGet.size());
    Assert.assertEquals(user1Acl, aclsGet.get(0));

    // remove acl
    Assert.assertTrue(store.removeAcl(prefixObj, user1Acl));
    aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(0, aclsGet.size());

    // set acl
    BitSet aclRights2 = new BitSet();
    aclRights2.set(ACLType.ALL.ordinal());
    OzoneAcl group1Acl = new OzoneAcl(GROUP,
        "group1", aclRights2, ACCESS);
    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(user1Acl);
    acls.add(group1Acl);
    Assert.assertTrue(store.setAcl(prefixObj, acls));

    // get acl
    aclsGet = store.getAcl(prefixObj);
    Assert.assertEquals(2, aclsGet.size());

    OzoneObj keyObj = new OzoneObjInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(key1)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    // Check default acls inherited from prefix.
    validateDefaultAcls(prefixObj, keyObj, null, bucket);

    // Check default acls inherited from bucket when prefix does not exist.
    validateDefaultAcls(prefixObj2, keyObj, null, bucket);
  }

  /**
   * Helper function to get default acl list for current user.
   *
   * @return list of default Acls.
   * @throws IOException
   * */
  private List<OzoneAcl> getAclList(OzoneConfiguration conf)
      throws IOException {
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    //User ACL
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    OzoneAclConfig aclConfig = conf.getObject(OzoneAclConfig.class);
    ACLType userRights = aclConfig.getUserDefaultRights();
    ACLType groupRights = aclConfig.getGroupDefaultRights();

    listOfAcls.add(new OzoneAcl(USER,
        ugi.getUserName(), userRights, ACCESS));
    //Group ACLs of the User
    List<String> userGroups = Arrays.asList(ugi.getGroupNames());
    userGroups.stream().forEach((group) -> listOfAcls.add(
        new OzoneAcl(GROUP, group, groupRights, ACCESS)));
    return listOfAcls;
  }

  /**
   * Helper function to validate ozone Acl for given object.
   * @param ozObj
   * */
  private void validateOzoneAccessAcl(OzoneObj ozObj) throws IOException {
    // Get acls for volume.
    List<OzoneAcl> expectedAcls = getAclList(new OzoneConfiguration());

    // Case:1 Add new acl permission to existing acl.
    if(expectedAcls.size()>0) {
      OzoneAcl oldAcl = expectedAcls.get(0);
      OzoneAcl newAcl = new OzoneAcl(oldAcl.getType(), oldAcl.getName(),
          ACLType.READ_ACL, ACCESS);
      // Verify that operation successful.
      assertTrue(store.addAcl(ozObj, newAcl));
      List<OzoneAcl> acls = store.getAcl(ozObj);

      assertTrue(acls.size() == expectedAcls.size());
      boolean aclVerified = false;
      for(OzoneAcl acl: acls) {
        if(acl.getName().equals(newAcl.getName())) {
          assertTrue(acl.getAclList().contains(ACLType.READ_ACL));
          aclVerified = true;
        }
      }
      assertTrue("New acl expected but not found.", aclVerified);
      aclVerified = false;

      // Case:2 Remove newly added acl permission.
      assertTrue(store.removeAcl(ozObj, newAcl));
      acls = store.getAcl(ozObj);
      assertTrue(acls.size() == expectedAcls.size());
      for(OzoneAcl acl: acls) {
        if(acl.getName().equals(newAcl.getName())) {
          assertFalse("READ_ACL should not exist in current acls:" +
              acls, acl.getAclList().contains(ACLType.READ_ACL));
          aclVerified = true;
        }
      }
      assertTrue("New acl expected but not found.", aclVerified);
    } else {
      fail("Default acl should not be empty.");
    }

    List<OzoneAcl> keyAcls = store.getAcl(ozObj);
    expectedAcls.forEach(a -> assertTrue(keyAcls.contains(a)));

    // Remove all acl's.
    for (OzoneAcl a : expectedAcls) {
      store.removeAcl(ozObj, a);
    }
    List<OzoneAcl> newAcls = store.getAcl(ozObj);
    assertTrue(newAcls.size() == 0);

    // Add acl's and then call getAcl.
    int aclCount = 0;
    for (OzoneAcl a : expectedAcls) {
      aclCount++;
      assertTrue(store.addAcl(ozObj, a));
      assertTrue(store.getAcl(ozObj).size() == aclCount);
    }
    newAcls = store.getAcl(ozObj);
    assertTrue(newAcls.size() == expectedAcls.size());
    List<OzoneAcl> finalNewAcls = newAcls;
    expectedAcls.forEach(a -> assertTrue(finalNewAcls.contains(a)));

    // Reset acl's.
    OzoneAcl ua = new OzoneAcl(USER, "userx",
        ACLType.READ_ACL, ACCESS);
    OzoneAcl ug = new OzoneAcl(GROUP, "userx",
        ACLType.ALL, ACCESS);
    store.setAcl(ozObj, Arrays.asList(ua, ug));
    newAcls = store.getAcl(ozObj);
    assertTrue(newAcls.size() == 2);
    assertTrue(newAcls.contains(ua));
    assertTrue(newAcls.contains(ug));
  }

  private void writeKey(String key1, OzoneBucket bucket) throws IOException {
    OzoneOutputStream out = bucket.createKey(key1, 1024, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(RandomStringUtils.random(1024).getBytes());
    out.close();
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }


  private void doMultipartUpload(OzoneBucket bucket, String keyName, byte val)
      throws Exception {
    // Initiate Multipart upload request
    String uploadID = initiateMultipartUpload(bucket, keyName, ReplicationType
        .RATIS, ReplicationFactor.THREE);

    // Upload parts
    Map<Integer, String> partsMap = new TreeMap<>();

    // get 5mb data, as each part should be of min 5mb, last part can be less
    // than 5mb
    int length = 0;
    byte[] data = generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, val);
    String partName = uploadPart(bucket, keyName, uploadID, 1, data);
    partsMap.put(1, partName);
    length += data.length;


    partName = uploadPart(bucket, keyName, uploadID, 2, data);
    partsMap.put(2, partName);
    length += data.length;

    String part3 = UUID.randomUUID().toString();
    partName = uploadPart(bucket, keyName, uploadID, 3, part3.getBytes(
        UTF_8));
    partsMap.put(3, partName);
    length += part3.getBytes(UTF_8).length;


    // Complete multipart upload request
    completeMultipartUpload(bucket, keyName, uploadID, partsMap);


    //Now Read the key which has been completed multipart upload.
    byte[] fileContent = new byte[data.length + data.length + part3.getBytes(
        UTF_8).length];
    OzoneInputStream inputStream = bucket.readKey(keyName);
    inputStream.read(fileContent);

    Assert.assertTrue(verifyRatisReplication(bucket.getVolumeName(),
        bucket.getName(), keyName, ReplicationType.RATIS,
        ReplicationFactor.THREE));

    StringBuilder sb = new StringBuilder(length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data);
    String part2 = new String(data);
    sb.append(part1);
    sb.append(part2);
    sb.append(part3);
    Assert.assertEquals(sb.toString(), new String(fileContent));
  }


  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationType replicationType, ReplicationFactor replicationFactor)
      throws Exception {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        replicationType, replicationFactor);

    String uploadID = multipartInfo.getUploadID();
    Assert.assertNotNull(uploadID);
    return uploadID;
  }

  private String uploadPart(OzoneBucket bucket, String keyName, String
      uploadID, int partNumber, byte[] data) throws Exception {
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0,
        data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());
    return omMultipartCommitUploadPartInfo.getPartName();

  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
        .completeMultipartUpload(keyName, uploadID, partsMap);

    Assert.assertNotNull(omMultipartUploadCompleteInfo);
    Assert.assertEquals(omMultipartUploadCompleteInfo.getBucket(), bucket
        .getName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getVolume(), bucket
        .getVolumeName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getKey(), keyName);
    Assert.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }
}
