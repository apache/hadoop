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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.*;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests BucketManagerImpl, mocks OMMetadataManager for testing.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBucketManagerImpl {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration createNewTestPath() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return conf;
  }

  private OmMetadataManagerImpl createSampleVol() throws IOException {
    OzoneConfiguration conf = createNewTestPath();
    OmMetadataManagerImpl metaMgr = new OmMetadataManagerImpl(conf);
    String volumeKey = metaMgr.getVolumeKey("sampleVol");
    // This is a simple hack for testing, we just test if the volume via a
    // null check, do not parse the value part. So just write some dummy value.
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("bilbo")
            .setOwnerName("bilbo")
            .build();
    metaMgr.getVolumeTable().put(volumeKey, args);
    return metaMgr;
  }

  @Test
  public void testCreateBucketWithoutVolume() throws Exception {
    thrown.expectMessage("Volume doesn't exist");
    OzoneConfiguration conf = createNewTestPath();
    OmMetadataManagerImpl metaMgr =
        new OmMetadataManagerImpl(conf);
    try {
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .build();
      bucketManager.createBucket(bucketInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.VOLUME_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  @Test
  public void testCreateBucket() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    KeyProviderCryptoExtension kmsProvider = Mockito.mock(
        KeyProviderCryptoExtension.class);
    String testBekName = "key1";
    String testCipherName = "AES/CTR/NoPadding";

    KeyProvider.Metadata mockMetadata = Mockito.mock(KeyProvider.Metadata
        .class);
    Mockito.when(kmsProvider.getMetadata(testBekName)).thenReturn(mockMetadata);
    Mockito.when(mockMetadata.getCipher()).thenReturn(testCipherName);

    BucketManager bucketManager = new BucketManagerImpl(metaMgr,
        kmsProvider);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setBucketEncryptionKey(new
            BucketEncryptionKeyInfo.Builder().setKeyName("key1").build())
        .build();
    bucketManager.createBucket(bucketInfo);
    Assert.assertNotNull(bucketManager.getBucketInfo("sampleVol", "bucketOne"));

    OmBucketInfo bucketInfoRead =
        bucketManager.getBucketInfo("sampleVol",  "bucketOne");

    Assert.assertTrue(bucketInfoRead.getEncryptionKeyInfo().getKeyName()
        .equals(bucketInfo.getEncryptionKeyInfo().getKeyName()));
    metaMgr.getStore().close();
  }


  @Test
  public void testCreateEncryptedBucket() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    bucketManager.createBucket(bucketInfo);
    Assert.assertNotNull(bucketManager.getBucketInfo("sampleVol",
        "bucketOne"));
    metaMgr.getStore().close();
  }

  @Test
  public void testCreateAlreadyExistingBucket() throws Exception {
    thrown.expectMessage("Bucket already exist");
    OmMetadataManagerImpl metaMgr = createSampleVol();

    try {
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .build();
      bucketManager.createBucket(bucketInfo);
      bucketManager.createBucket(bucketInfo);
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_ALREADY_EXISTS,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  @Test
  public void testGetBucketInfoForInvalidBucket() throws Exception {
    thrown.expectMessage("Bucket not found");
    OmMetadataManagerImpl metaMgr = createSampleVol();
    try {


      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      bucketManager.getBucketInfo("sampleVol", "bucketOne");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    } finally {
      metaMgr.getStore().close();
    }
  }

  @Test
  public void testGetBucketInfo() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    Assert.assertEquals(false, result.getIsVersionEnabled());
    metaMgr.getStore().close();
  }

  @Test
  public void testSetBucketPropertyAddACL() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl ozoneAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    acls.add(ozoneAcl);
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(1, result.getAcls().size());
    List<OzoneAcl> addAcls = new LinkedList<>();
    OzoneAcl newAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    addAcls.add(newAcl);
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAddAcls(addAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, updatedResult.getAcls().size());
    Assert.assertTrue(updatedResult.getAcls().contains(newAcl));
    metaMgr.getStore().close();
  }

  @Test
  public void testSetBucketPropertyRemoveACL() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl aclOne = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    OzoneAcl aclTwo = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    acls.add(aclOne);
    acls.add(aclTwo);
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, result.getAcls().size());
    List<OzoneAcl> removeAcls = new LinkedList<>();
    removeAcls.add(aclTwo);
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setRemoveAcls(removeAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(1, updatedResult.getAcls().size());
    Assert.assertFalse(updatedResult.getAcls().contains(aclTwo));
    metaMgr.getStore().close();
  }

  @Test
  public void testSetBucketPropertyChangeStorageType() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.DISK)
        .build();
    bucketManager.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.SSD)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(StorageType.SSD,
        updatedResult.getStorageType());
    metaMgr.getStore().close();
  }

  @Test
  public void testSetBucketPropertyChangeVersioning() throws Exception {
    OmMetadataManagerImpl metaMgr = createSampleVol();

    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    OmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertFalse(result.getIsVersionEnabled());
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setIsVersionEnabled(true)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    OmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertTrue(updatedResult.getIsVersionEnabled());
    metaMgr.getStore().close();
  }

  @Test
  public void testDeleteBucket() throws Exception {
    thrown.expectMessage("Bucket not found");
    OmMetadataManagerImpl metaMgr = createSampleVol();
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    for (int i = 0; i < 5; i++) {
      OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucket_" + i)
          .build();
      bucketManager.createBucket(bucketInfo);
    }
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals("bucket_" + i,
          bucketManager.getBucketInfo(
              "sampleVol", "bucket_" + i).getBucketName());
    }
    try {
      bucketManager.deleteBucket("sampleVol", "bucket_1");
      Assert.assertNotNull(bucketManager.getBucketInfo(
          "sampleVol", "bucket_2"));
    } catch (IOException ex) {
      Assert.fail(ex.getMessage());
    }
    try {
      bucketManager.getBucketInfo("sampleVol", "bucket_1");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_FOUND,
          omEx.getResult());
      throw omEx;
    }
    metaMgr.getStore().close();
  }

  @Test
  public void testDeleteNonEmptyBucket() throws Exception {
    thrown.expectMessage("Bucket is not empty");
    OmMetadataManagerImpl metaMgr = createSampleVol();
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    bucketManager.createBucket(bucketInfo);
    //Create keys in bucket
    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_one",
        new OmKeyInfo.Builder()
            .setBucketName("bucketOne")
            .setVolumeName("sampleVol")
            .setKeyName("key_one")
            .setReplicationFactor(ReplicationFactor.ONE)
            .setReplicationType(ReplicationType.STAND_ALONE)
            .build());
    metaMgr.getKeyTable().put("/sampleVol/bucketOne/key_two",
        new OmKeyInfo.Builder()
            .setBucketName("bucketOne")
            .setVolumeName("sampleVol")
            .setKeyName("key_two")
            .setReplicationFactor(ReplicationFactor.ONE)
            .setReplicationType(ReplicationType.STAND_ALONE)
            .build());
    try {
      bucketManager.deleteBucket("sampleVol", "bucketOne");
    } catch (OMException omEx) {
      Assert.assertEquals(ResultCodes.BUCKET_NOT_EMPTY,
          omEx.getResult());
      throw omEx;
    }
    metaMgr.getStore().close();
  }
}
