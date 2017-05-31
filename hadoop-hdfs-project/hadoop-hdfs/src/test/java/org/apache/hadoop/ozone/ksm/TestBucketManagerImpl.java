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

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions
    .KSMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocolPB.KSMPBHelper;
import org.apache.hadoop.ozone.web.request.OzoneAcl;
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
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.mockito.Mockito.any;

/**
 * Tests BucketManagerImpl, mocks MetadataManager for testing.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestBucketManagerImpl {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private MetadataManager getMetadataManagerMock(String... volumesToCreate)
      throws IOException {
    MetadataManager metadataManager = Mockito.mock(MetadataManager.class);
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
    MetadataManager metaMgr = getMetadataManagerMock();
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
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
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
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
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
      MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
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
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(HdfsProtos.StorageTypeProto.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(HdfsProtos.StorageTypeProto.DISK,
        result.getStorageType());
    Assert.assertEquals(false, result.getIsVersionEnabled());
  }

  @Test
  public void testSetBucketPropertyAddACL() throws IOException {
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    List<OzoneAclInfo> acls = new LinkedList<>();
    OzoneAcl ozoneAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    acls.add(KSMPBHelper.convertOzoneAcl(ozoneAcl));
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(HdfsProtos.StorageTypeProto.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals("sampleVol", result.getVolumeName());
    Assert.assertEquals("bucketOne", result.getBucketName());
    Assert.assertEquals(1, result.getAcls().size());
    List<OzoneAclInfo> addAcls = new LinkedList<>();
    OzoneAcl newAcl = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    addAcls.add(KSMPBHelper.convertOzoneAcl(newAcl));
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAddAcls(addAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, updatedResult.getAcls().size());
    Assert.assertTrue(updatedResult.getAcls().contains(
        KSMPBHelper.convertOzoneAcl(newAcl)));
  }

  @Test
  public void testSetBucketPropertyRemoveACL() throws IOException {
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    List<OzoneAclInfo> acls = new LinkedList<>();
    OzoneAcl aclOne = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "root", OzoneAcl.OzoneACLRights.READ);
    OzoneAcl aclTwo = new OzoneAcl(OzoneAcl.OzoneACLType.USER,
        "ozone", OzoneAcl.OzoneACLRights.READ);
    acls.add(KSMPBHelper.convertOzoneAcl(aclOne));
    acls.add(KSMPBHelper.convertOzoneAcl(aclTwo));
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setAcls(acls)
        .setStorageType(HdfsProtos.StorageTypeProto.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(2, result.getAcls().size());
    List<OzoneAclInfo> removeAcls = new LinkedList<>();
    removeAcls.add(KSMPBHelper.convertOzoneAcl(aclTwo));
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setRemoveAcls(removeAcls)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(1, updatedResult.getAcls().size());
    Assert.assertFalse(updatedResult.getAcls().contains(
        KSMPBHelper.convertOzoneAcl(aclTwo)));
  }

  @Test
  public void testSetBucketPropertyChangeStorageType() throws IOException {
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    BucketManager bucketManager = new BucketManagerImpl(metaMgr);
    KsmBucketInfo bucketInfo = KsmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(HdfsProtos.StorageTypeProto.DISK)
        .build();
    bucketManager.createBucket(bucketInfo);
    KsmBucketInfo result = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(HdfsProtos.StorageTypeProto.DISK,
        result.getStorageType());
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(HdfsProtos.StorageTypeProto.SSD)
        .build();
    bucketManager.setBucketProperty(bucketArgs);
    KsmBucketInfo updatedResult = bucketManager.getBucketInfo(
        "sampleVol", "bucketOne");
    Assert.assertEquals(HdfsProtos.StorageTypeProto.SSD,
        updatedResult.getStorageType());
  }

  @Test
  public void testSetBucketPropertyChangeVersioning() throws IOException {
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
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
}