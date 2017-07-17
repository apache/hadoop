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

package org.apache.hadoop.ozone;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.ozone.io.OzoneOutputStream;
import org.apache.hadoop.ozone.web.exceptions.OzoneException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
public class TestOzoneClientImpl {

  private static MiniOzoneCluster cluster = null;
  private static OzoneClientImpl ozClient = null;

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
    ozClient = new OzoneClientImpl(conf);
  }

  @Test
  public void testCreateVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    OzoneVolume volume = ozClient.getVolumeDetails(volumeName);
    Assert.assertEquals(volumeName, volume.getVolumeName());
  }

  @Test
  public void testCreateVolumeWithOwner()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName, "test");
    OzoneVolume volume = ozClient.getVolumeDetails(volumeName);
    Assert.assertEquals(volumeName, volume.getVolumeName());
    Assert.assertEquals("test", volume.getOwnerName());
  }

  @Test
  public void testCreateVolumeWithQuota()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName, "test",
        10000000000L);
    OzoneVolume volume = ozClient.getVolumeDetails(volumeName);
    Assert.assertEquals(volumeName, volume.getVolumeName());
    Assert.assertEquals("test", volume.getOwnerName());
    Assert.assertEquals(10000000000L, volume.getQuota());
  }

  @Test
  public void testVolumeAlreadyExist()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    try {
      ozClient.createVolume(volumeName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Volume creation failed, error:VOLUME_ALREADY_EXISTS",
          ex.getMessage());
    }
  }

  @Test
  public void testCreateBucket()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName);
    OzoneBucket bucket = ozClient.getBucketDetails(volumeName, bucketName);
    Assert.assertEquals(bucketName, bucket.getBucketName());
  }

  @Test
  public void testCreateBucketWithVersioning()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName,
        OzoneConsts.Versioning.ENABLED);
    OzoneBucket bucket = ozClient.getBucketDetails(volumeName, bucketName);
    Assert.assertEquals(bucketName, bucket.getBucketName());
    Assert.assertEquals(OzoneConsts.Versioning.ENABLED,
        bucket.getVersioning());
  }

  @Test
  public void testCreateBucketWithStorageType()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName, StorageType.SSD);
    OzoneBucket bucket = ozClient.getBucketDetails(volumeName, bucketName);
    Assert.assertEquals(bucketName, bucket.getBucketName());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
  }

  @Test
  public void testCreateBucketWithAcls()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName, userAcl);
    OzoneBucket bucket = ozClient.getBucketDetails(volumeName, bucketName);
    Assert.assertEquals(bucketName, bucket.getBucketName());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testCreateBucketWithAllArgument()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl userAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER, "test",
        OzoneAcl.OzoneACLRights.READ_WRITE);
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName,
        OzoneConsts.Versioning.ENABLED,
        StorageType.SSD, userAcl);
    OzoneBucket bucket = ozClient.getBucketDetails(volumeName, bucketName);
    Assert.assertEquals(bucketName, bucket.getBucketName());
    Assert.assertEquals(OzoneConsts.Versioning.ENABLED,
        bucket.getVersioning());
    Assert.assertEquals(StorageType.SSD, bucket.getStorageType());
    Assert.assertTrue(bucket.getAcls().contains(userAcl));
  }

  @Test
  public void testCreateBucketInInvalidVolume()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    try {
      ozClient.createBucket(volumeName, bucketName);
    } catch (IOException ex) {
      Assert.assertEquals(
          "Bucket creation failed, error: VOLUME_NOT_FOUND",
          ex.getMessage());
    }
  }

  @Test
  public void testPutKey()
      throws IOException, OzoneException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    ozClient.createVolume(volumeName);
    ozClient.createBucket(volumeName, bucketName);
    OzoneOutputStream out = ozClient.createKey(volumeName, bucketName,
        keyName, value.getBytes().length);
    out.write(value.getBytes());
    OzoneKey key = ozClient.getkeyDetails(volumeName, bucketName, keyName);
    Assert.assertEquals(keyName, key.getKeyName());
    //Content validation has to be done after getKey implementation.
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

}
