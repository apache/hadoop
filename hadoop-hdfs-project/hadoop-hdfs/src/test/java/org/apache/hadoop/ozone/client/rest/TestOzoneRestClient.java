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

package org.apache.hadoop.ozone.client.rest;

import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConfigKeys;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class is to test all the public facing APIs of Ozone REST Client.
 */
public class TestOzoneRestClient {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static MiniOzoneClassicCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static ObjectStore store = null;

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
    cluster = new MiniOzoneClassicCluster.Builder(conf).numDataNodes(1)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED).build();

    InetSocketAddress ksmHttpAddress = cluster.getKeySpaceManager()
        .getHttpServer().getHttpAddress();
    ozClient = OzoneClientFactory.getRestClient(ksmHttpAddress.getHostName(),
        ksmHttpAddress.getPort(), conf);
    store = ozClient.getObjectStore();
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
  public void testVolumeAlreadyExist()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    try {
      store.createVolume(volumeName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Volume creation failed, error:VOLUME_ALREADY_EXISTS",
          ex.getCause().getMessage());
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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    Assert.assertEquals(bucketName, bucket.getName());
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


  @Test
  public void testPutKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

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
      Assert.assertEquals(value, new String(fileContent));
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

  /**
   * Close OzoneClient and shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() throws IOException {
    if(ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
