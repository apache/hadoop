/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneQuota;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.ReplicationFactor;
import org.apache.hadoop.ozone.client.ReplicationType;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.ksm.protocolPB.
    KeySpaceManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.ozone.client.rest.OzoneException;
import org.apache.hadoop.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.scm.protocolPB.
    StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestOzoneRpcClient {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;
  private static KeySpaceManagerProtocolClientSideTranslatorPB
      keySpaceManagerClient;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;

  /**
   * Create a MiniOzoneCluster for testing.
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
    cluster = new MiniOzoneCluster.Builder(conf).numDataNodes(5)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();
    conf.set("ozone.client.protocol",
        "org.apache.hadoop.ozone.client.rpc.RpcClient");
    OzoneClientFactory.setConfiguration(conf);
    ozClient = OzoneClientFactory.getClient();
    store = ozClient.getObjectStore();
    storageContainerLocationClient =
        cluster.createStorageContainerLocationClient();
    keySpaceManagerClient = cluster.createKeySpaceManagerClient();
  }

  @Test
  public void testCreateVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
  }

  @Test
  public void testCreateVolumeWithOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs.Builder argsBuilder = VolumeArgs.newBuilder();
    argsBuilder.setOwner("test");
    store.createVolume(volumeName, argsBuilder.build());
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
    Assert.assertEquals("test", volume.getOwner());
  }

  @Test
  public void testCreateVolumeWithQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    VolumeArgs.Builder argsBuilder = VolumeArgs.newBuilder();
    argsBuilder.setOwner("test").setQuota("1000000000 BYTES");
    store.createVolume(volumeName, argsBuilder.build());
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals(volumeName, volume.getName());
    Assert.assertEquals("test", volume.getOwner());
    Assert.assertEquals(1000000000L, volume.getQuota());
  }

  @Test
  public void testInvalidVolumeCreation() throws IOException {
    thrown.expectMessage("Bucket or Volume name has an unsupported" +
        " character : #");
    String volumeName = "invalid#name";
    store.createVolume(volumeName);
  }

  @Test
  public void testVolumeAlreadyExist()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    try {
      store.createVolume(volumeName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Volume creation failed, error:VOLUME_ALREADY_EXISTS",
          ex.getMessage());
    }
  }

  @Test
  public void testSetVolumeOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    store.getVolume(volumeName).setOwner("test");
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertEquals("test", volume.getOwner());
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
      throws IOException, OzoneException {
    thrown.expectMessage("Info Volume failed, error");
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    Assert.assertNotNull(volume);
    store.deleteVolume(volumeName);
    store.getVolume(volumeName);
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
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
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
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
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
  public void testInvalidBucketCreation() throws IOException {
    thrown.expectMessage("Bucket or Volume name has an unsupported" +
        " character : #");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = "invalid#bucket";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
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
    acls.add(new OzoneAcl(
        OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE));
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
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
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
      throws IOException, OzoneException {
    thrown.expectMessage("Info Bucket failed, error");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertNotNull(bucket);
    volume.deleteBucket(bucketName);
    volume.getBucket(bucketName);
  }

  private boolean verifyRatisReplication(String volumeName, String bucketName,
      String keyName, ReplicationType type, ReplicationFactor factor)
      throws IOException {
    KsmKeyArgs keyArgs = new KsmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .build();
    OzoneProtos.ReplicationType replicationType =
        OzoneProtos.ReplicationType.valueOf(type.toString());
    OzoneProtos.ReplicationFactor replicationFactor =
        OzoneProtos.ReplicationFactor.valueOf(factor.getValue());
    KsmKeyInfo keyInfo = keySpaceManagerClient.lookupKey(keyArgs);
    for (KsmKeyLocationInfo info: keyInfo.getKeyLocationList()) {
      Pipeline pipeline =
          storageContainerLocationClient.getContainer(info.getContainerName());
      if ((pipeline.getFactor() != replicationFactor) ||
          (pipeline.getType() != replicationType)) {
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
          value.getBytes().length, ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE);
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE));
      Assert.assertEquals(value, new String(fileContent));
      Assert.assertTrue(key.getCreationTime() >= currentTime);
      Assert.assertTrue(key.getModificationTime() >= currentTime);
    }
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
          ReplicationFactor.ONE);
      out.write(value.getBytes());
      out.close();
      OzoneKey key = bucket.getKey(keyName);
      Assert.assertEquals(keyName, key.getName());
      OzoneInputStream is = bucket.readKey(keyName);
      byte[] fileContent = new byte[value.getBytes().length];
      is.read(fileContent);
      is.close();
      Assert.assertTrue(verifyRatisReplication(volumeName, bucketName,
          keyName, ReplicationType.RATIS, ReplicationFactor.ONE));
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
          ReplicationFactor.THREE);
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

  @Test
  public void testDeleteKey()
      throws IOException, OzoneException {
    thrown.expectMessage("Lookup key failed, error");
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes().length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
    out.write(value.getBytes());
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());
    bucket.deleteKey(keyName);
    bucket.getKey(keyName);
  }

  @Test
  public void testListVolume() throws IOException, OzoneException {
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
    Iterator<OzoneVolume> volIterator = store.listVolumes(volBase);
    int totalVolumeCount = 0;
    while(volIterator.hasNext()) {
      volIterator.next();
      totalVolumeCount++;
    }
    Assert.assertEquals(20, totalVolumeCount);
    Iterator<OzoneVolume> volAIterator = store.listVolumes(volBaseNameA);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volAIterator.next().getName()
          .startsWith(volBaseNameA + i + "-"));
    }
    Assert.assertFalse(volAIterator.hasNext());
    Iterator<OzoneVolume> volBIterator = store.listVolumes(volBaseNameB);
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBIterator.next().getName()
          .startsWith(volBaseNameB + i + "-"));
    }
    Assert.assertFalse(volBIterator.hasNext());
    Iterator<OzoneVolume> iter = store.listVolumes(volBaseNameA + "1-");
    Assert.assertTrue(iter.next().getName().startsWith(volBaseNameA + "1-"));
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testListBucket()
      throws IOException, OzoneException {
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
    Iterator<OzoneBucket> volABucketIter =
        volA.listBuckets("bucket-");
    int volABucketCount = 0;
    while(volABucketIter.hasNext()) {
      volABucketIter.next();
      volABucketCount++;
    }
    Assert.assertEquals(20, volABucketCount);
    Iterator<OzoneBucket> volBBucketIter =
        volA.listBuckets("bucket-");
    int volBBucketCount = 0;
    while(volBBucketIter.hasNext()) {
      volBBucketIter.next();
      volBBucketCount++;
    }
    Assert.assertEquals(20, volBBucketCount);

    Iterator<OzoneBucket> volABucketAIter =
        volA.listBuckets("bucket-a-");
    int volABucketACount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketACount++;
    }
    Assert.assertEquals(10, volABucketACount);
    Iterator<OzoneBucket> volBBucketBIter =
        volA.listBuckets("bucket-b-");
    int volBBucketBCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBCount++;
    }
    Assert.assertEquals(10, volBBucketBCount);
    Iterator<OzoneBucket> volABucketBIter = volA.listBuckets("bucket-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketBIter.next().getName()
          .startsWith(bucketBaseNameB + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
    Iterator<OzoneBucket> volBBucketAIter = volB.listBuckets("bucket-a-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volBBucketAIter.next().getName()
          .startsWith(bucketBaseNameA + i + "-"));
    }
    Assert.assertFalse(volBBucketAIter.hasNext());

  }

  @Test
  public void testListBucketsOnEmptyVolume()
      throws IOException, OzoneException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    Iterator<OzoneBucket> buckets = vol.listBuckets("");
    while(buckets.hasNext()) {
      Assert.fail();
    }
  }

  @Test
  public void testListKey()
      throws IOException, OzoneException {
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
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseA + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
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
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      one.write(value);
      one.close();
      OzoneOutputStream two = volAbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      two.write(value);
      two.close();
      OzoneOutputStream three = volBbucketA.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      three.write(value);
      three.close();
      OzoneOutputStream four = volBbucketB.createKey(
          keyBaseB + i + "-" + RandomStringUtils.randomNumeric(5),
          value.length, ReplicationType.STAND_ALONE, ReplicationFactor.ONE);
      four.write(value);
      four.close();
    }
    Iterator<OzoneKey> volABucketAIter =
        volAbucketA.listKeys("key-");
    int volABucketAKeyCount = 0;
    while(volABucketAIter.hasNext()) {
      volABucketAIter.next();
      volABucketAKeyCount++;
    }
    Assert.assertEquals(20, volABucketAKeyCount);
    Iterator<OzoneKey> volABucketBIter =
        volAbucketB.listKeys("key-");
    int volABucketBKeyCount = 0;
    while(volABucketBIter.hasNext()) {
      volABucketBIter.next();
      volABucketBKeyCount++;
    }
    Assert.assertEquals(20, volABucketBKeyCount);
    Iterator<OzoneKey> volBBucketAIter =
        volBbucketA.listKeys("key-");
    int volBBucketAKeyCount = 0;
    while(volBBucketAIter.hasNext()) {
      volBBucketAIter.next();
      volBBucketAKeyCount++;
    }
    Assert.assertEquals(20, volBBucketAKeyCount);
    Iterator<OzoneKey> volBBucketBIter =
        volBbucketB.listKeys("key-");
    int volBBucketBKeyCount = 0;
    while(volBBucketBIter.hasNext()) {
      volBBucketBIter.next();
      volBBucketBKeyCount++;
    }
    Assert.assertEquals(20, volBBucketBKeyCount);
    Iterator<OzoneKey> volABucketAKeyAIter =
        volAbucketA.listKeys("key-a-");
    int volABucketAKeyACount = 0;
    while(volABucketAKeyAIter.hasNext()) {
      volABucketAKeyAIter.next();
      volABucketAKeyACount++;
    }
    Assert.assertEquals(10, volABucketAKeyACount);
    Iterator<OzoneKey> volABucketAKeyBIter =
        volAbucketA.listKeys("key-b-");
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(volABucketAKeyBIter.next().getName()
          .startsWith("key-b-" + i + "-"));
    }
    Assert.assertFalse(volABucketBIter.hasNext());
  }

  @Test
  public void testListKeyOnEmptyBucket()
      throws IOException, OzoneException {
    String volume = "vol-" + RandomStringUtils.randomNumeric(5);
    String bucket = "buc-" + RandomStringUtils.randomNumeric(5);
    store.createVolume(volume);
    OzoneVolume vol = store.getVolume(volume);
    vol.createBucket(bucket);
    OzoneBucket buc = vol.getBucket(bucket);
    Iterator<OzoneKey> keys = buc.listKeys("");
    while(keys.hasNext()) {
      Assert.fail();
    }
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }

    if (storageContainerLocationClient != null) {
      storageContainerLocationClient.close();
    }

    if (keySpaceManagerClient != null) {
      keySpaceManagerClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
