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
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions
    .KSMException.ResultCodes;
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
import java.util.HashMap;
import java.util.Map;
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
      KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .setStorageType(StorageType.DISK)
          .setIsVersionEnabled(false)
          .build();
      bucketManager.createBucket(bucketArgs);
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
    KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();
    bucketManager.createBucket(bucketArgs);
    //TODO: Use BucketManagerImpl#getBucketInfo to verify creation of bucket.
    Assert.assertNotNull(metaMgr
        .get(DFSUtil.string2Bytes("/sampleVol/bucketOne")));
  }

  @Test
  public void testCreateAlreadyExistingBucket() throws IOException {
    thrown.expectMessage("Bucket already exist");
    MetadataManager metaMgr = getMetadataManagerMock("sampleVol");
    try {
      BucketManager bucketManager = new BucketManagerImpl(metaMgr);
      KsmBucketArgs bucketArgs = KsmBucketArgs.newBuilder()
          .setVolumeName("sampleVol")
          .setBucketName("bucketOne")
          .setStorageType(StorageType.DISK)
          .setIsVersionEnabled(false)
          .build();
      bucketManager.createBucket(bucketArgs);
      bucketManager.createBucket(bucketArgs);
    } catch(KSMException ksmEx) {
      Assert.assertEquals(ResultCodes.FAILED_BUCKET_ALREADY_EXISTS,
          ksmEx.getResult());
      throw ksmEx;
    }
  }
}