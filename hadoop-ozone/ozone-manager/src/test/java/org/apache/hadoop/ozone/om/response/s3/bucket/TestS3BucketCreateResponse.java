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

package org.apache.hadoop.ozone.om.response.s3.bucket;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.s3.bucket.S3BucketCreateRequest;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Class to test S3BucketCreateResponse.
 */
public class TestS3BucketCreateResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }


  @Test
  public void testAddToDBBatch() throws Exception {
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateS3Bucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setSuccess(true)
            .setCreateS3BucketResponse(
                OzoneManagerProtocolProtos.S3CreateBucketResponse
                    .getDefaultInstance())
            .build();

    String volumeName = S3BucketCreateRequest.formatOzoneVolumeName(userName);
    OzoneManagerProtocolProtos.VolumeList volumeList =
        OzoneManagerProtocolProtos.VolumeList.newBuilder()
            .addVolumeNames(volumeName).build();

    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setOwnerName(userName).setAdminName(userName)
        .setVolume(volumeName).setCreationTime(Time.now()).build();

    OMVolumeCreateResponse omVolumeCreateResponse =
        new OMVolumeCreateResponse(omVolumeArgs, volumeList, omResponse);


    OmBucketInfo omBucketInfo = TestOMResponseUtils.createBucket(
        volumeName, s3BucketName);
    OMBucketCreateResponse omBucketCreateResponse =
        new OMBucketCreateResponse(omBucketInfo, omResponse);

    String s3Mapping = S3BucketCreateRequest.formatS3MappingName(volumeName,
        s3BucketName);
    S3BucketCreateResponse s3BucketCreateResponse =
        new S3BucketCreateResponse(omVolumeCreateResponse,
            omBucketCreateResponse, s3BucketName, s3Mapping, omResponse);

    s3BucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNotNull(omMetadataManager.getS3Table().get(s3BucketName));
    Assert.assertEquals(s3Mapping,
        omMetadataManager.getS3Table().get(s3BucketName));
    Assert.assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));
    Assert.assertNotNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, s3BucketName)));

  }
}

