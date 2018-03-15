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
package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions
    .KSMException.ResultCodes;
import org.apache.hadoop.ozone.OzoneAcl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.mockito.Mockito.any;

/**
 * Tests BucketManagerImpl, mocks KSMMetadataManager for testing.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBucketManagerImpl {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private KSMMetadataManager getMetadataManagerMock(String... volumesToCreate)
      throws IOException {
    KSMMetadataManager metadataManager = Mockito.mock(KSMMetadataManager.class);
    Map<String, byte[]> metadataDB = new HashMap<>();
    ReadWriteLock lock = new ReentrantReadWriteLock();

    Mockito.when(metadataManager.writeLock()).thenReturn(lock.writeLock());
    Mockito.when(metadataManager.readLock()).thenReturn(lock.readLock());
    Mockito.when(metadataManager.getVolumeKey(any(String.class))).thenAnswer(
        (InvocationOnMock invocation) ->
            DFSUtil.string2Bytes(
                OzoneConsts.KSM_VOLUME_PREFIX + invocation.getArguments()[0]));
    Mockito.when(metadataManager
        .getBucketKey(any(String.class), any(String.class))).thenAnswer(
            (InvocationOnMock invocation) ->
                DFSUtil.string2Bytes(
                    OzoneConsts.KSM_VOLUME_PREFIX
                        + invocation.getArguments()[0]
                        + OzoneConsts.KSM_BUCKET_PREFIX
                        + invocation.getArguments()[1]));

    Mockito.doAnswer(
        new Answer<Boolean>() {
          @Override
          public Boolean answer(InvocationOnMock invocation)
              throws Throwable {
            String keyRootName =  OzoneConsts.KSM_KEY_PREFIX
                + invocation.getArguments()[0]
                + OzoneConsts.KSM_KEY_PREFIX
                + invocation.getArguments()[1]
                + OzoneConsts.KSM_KEY_PREFIX;
            Iterator<String> keyIterator = metadataDB.keySet().iterator();
            while(keyIterator.hasNext()) {
              if(keyIterator.next().startsWith(keyRootName)) {
                return false;
              }
            }
            return true;
          }
        }).when(metadataManager).isBucketEmpty(any(String.class),
        any(String.class));

    Mockito.doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            metadataDB.put(DFSUtil.bytes2String(
                (byte[])invocation.getArguments()[0]),
                (byte[])invocation.getArguments()[1]);
            return null;
          }
        }).when(metadataManager).put(any(byte[].class), any(byte[].class));

    Mockito.when(metadataManager.get(any(byte[].class))).thenAnswer(
        (InvocationOnMock invocation) ->
            metadataDB.get(DFSUtil.bytes2String(
                (byte[])invocation.getArguments()[0]))
    );
    Mockito.doAnswer(
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            metadataDB.remove(DFSUtil.bytes2String(
                (byte[])invocation.getArguments()[0]));
            return null;
          }
        }).when(metadataManager).delete(any(byte[].class));

    for(String volumeName : volumesToCreate) {
      byte[] dummyVolumeInfo = DFSUtil.string2Bytes(volumeName);
      metadataDB.put(OzoneConsts.KSM_VOLUME_PREFIX + volumeName,
                     dummyVolumeInfo);
    }
    return metadataManager;
  }

  @Test
  public void testCreateBucketWithoutVolume() throws IOException {
    thrown.expectMessage("Volume doesn't exist");
    KSMMetadataManager metaMgr = getMetadataManagerMock();
    try {
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .build();
      bucketManager.createBucket(bucketInfo);
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_VOLUME_NOT_FOUND,
          ksmEx.getResult());
      throw ksmEx;
    }
  }

  @Test
  public void testCreateBucket() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    bucketManager.createBucket(bucketInfo);
    Assert.assertNotNull(bucketManager.getBucketInfo("sampleVol", "bucketOne"));
  }

  @Test
  public void testCreateAlreadyExistingBucket() throws IOException {
    thrown.expectMessage("Bucket already exist");
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    try {
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .build();
      bucketManager.createBucket(bucketInfo);
      bucketManager.createBucket(bucketInfo);
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_BUCKET_ALREADY_EXISTS,
          ksmEx.getResult());
      throw ksmEx;
    }
  }

  @Test
  public void testGetBucketInfoForInvalidBucket() throws IOException {
    thrown.expectMessage("Bucket not found");
    try {
      KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      bucketManager.getBucketInfo("sampleVol", "bucketOne");
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_BUCKET_NOT_FOUND,
          ksmEx.getResult());
      throw ksmEx;
    }
  }

  @Test
  public void testGetBucketInfo() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    Assert.assertEquals(false, result.getIsVersionEnabled());
  }

  @Test
  public void testSetBucketPropertyAddACL() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl ozoneAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    acls.add(ozoneAcl);
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(1, result.getAcls().size());
    List<OzoneAcl> addAcls = new LinkedList<>();
    OzoneAcl newAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    addAcls.add(newAcl);
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAddAcls(addAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, updatedResult.getAcls().size());
    Assert.assertTrue(updatedResult.getAcls().contains(newAcl));
  }

  @Test
  public void testSetBucketPropertyRemoveACL() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    List<OzoneAcl> acls = new LinkedList<>();
    OzoneAcl aclOne = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    OzoneAcl aclTwo = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    acls.add(aclOne);
    acls.add(aclTwo);
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, result.getAcls().size());
    List<OzoneAcl> removeAcls = new LinkedList<>();
    removeAcls.add(aclTwo);
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setRemoveAcls(removeAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(1, updatedResult.getAcls().size());
    Assert.assertFalse(updatedResult.getAcls().contains(aclTwo));
  }

  @Test
  public void testSetBucketPropertyChangeStorageType() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.DISK)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(StorageType.DISK,
        result.getStorageType());
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.SSD)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(StorageType.SSD,
        updatedResult.getStorageType());
  }

  @Test
  public void testSetBucketPropertyChangeVersioning() throws IOException {
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertFalse(result.getIsVersionEnabled());
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setIsVersionEnabled(true)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertTrue(updatedResult.getIsVersionEnabled());
  }

  @Test
  public void testDeleteBucket() throws IOException {
    thrown.expectMessage("Bucket not found");
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    for(int i = 0; i < 5; i++) {
      KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucket_" + i)
          .build();
      bucketManager.createBucket(bucketInfo);
    }
    for(int i = 0; i < 5; i++) {
      Assert.assertEquals("bucket_" + i,
          bucketManager.getBucketInfo(
              "sampleVol", "bucket_" + i).getBucketName());
    }
    try {
      bucketManager.deleteBucket("sampleVol", "bucket_1");
      Assert.assertNotNull(bucketManager.getBucketInfo(
          "sampleVol", "bucket_2"));
    } catch(IOException ex) {
      Assert.fail(ex.getMessage());
    }
    try {
      bucketManager.getBucketInfo("sampleVol", "bucket_1");
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_BUCKET_NOT_FOUND,
          ksmEx.getResult());
      throw ksmEx;
    }
  }

  @Test
  public void testDeleteNonEmptyBucket() throws IOException {
    thrown.expectMessage("Bucket is not empty");
    KSMMetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();
    bucketManager.createBucket(bucketInfo);
    //Create keys in bucket
    metaMgr.put(DFSUtil.string2Bytes("/sampleVol/bucketOne/key_one"),
        DFSUtil.string2Bytes("value_one"));
    metaMgr.put(DFSUtil.string2Bytes("/sampleVol/bucketOne/key_two"),
        DFSUtil.string2Bytes("value_two"));
    try {
      bucketManager.deleteBucket("sampleVol", "bucketOne");
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_BUCKET_NOT_EMPTY,
          ksmEx.getResult());
      throw ksmEx;
    }
  }
}
