/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Optional;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test key manager.
 */
public class TestKeyManagerUnit {

  private OmMetadataManagerImpl metadataManager;
  private KeyManagerImpl keyManager;

  @Before
  public void setup() throws IOException {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTestDir().toString());
    metadataManager = new OmMetadataManagerImpl(configuration);
    keyManager = new KeyManagerImpl(
        Mockito.mock(ScmBlockLocationProtocol.class),
        metadataManager,
        configuration,
        "omtest",
        Mockito.mock(OzoneBlockTokenSecretManager.class)
    );
  }

  @Test
  public void listMultipartUploadPartsWithZeroUpload() throws IOException {
    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");

    OmMultipartInfo omMultipartInfo =
        initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key1");

    //WHEN
    OmMultipartUploadListParts omMultipartUploadListParts = keyManager
        .listParts("vol1", "bucket1", "dir/key1", omMultipartInfo.getUploadID(),
            0, 10);

    Assert.assertEquals(0,
        omMultipartUploadListParts.getPartInfoList().size());

  }

  private void createBucket(OmMetadataManagerImpl omMetadataManager,
      String volume, String bucket)
      throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volume, bucket);
    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .setAcls(new ArrayList<>())
        .build();
    omMetadataManager.getBucketTable()
        .put(bucketKey, omBucketInfo);
    omMetadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey),
        new CacheValue<>(Optional.of(omBucketInfo), 1L));
  }

  private OmMultipartInfo initMultipartUpload(KeyManagerImpl omtest,
      String volume, String bucket, String key)
      throws IOException {
    OmKeyArgs key1 = new Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setType(ReplicationType.RATIS)
        .setFactor(ReplicationFactor.THREE)
        .setAcls(new ArrayList<>())
        .build();
    return omtest.initiateMultipartUpload(key1);
  }
}