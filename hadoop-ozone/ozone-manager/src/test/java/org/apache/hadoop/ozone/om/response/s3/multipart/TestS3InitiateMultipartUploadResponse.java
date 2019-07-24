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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.util.Time;

/**
 * Class tests S3 Initiate MPU response.
 */
public class TestS3InitiateMultipartUploadResponse
    extends TestS3InitiateMultipartResponse {

  @Test
  public void addDBToBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String multipartUploadID = UUID.randomUUID().toString();


    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo(
        multipartUploadID, new HashMap<>());

    OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .build();

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.InitiateMultiPartUpload)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true).setInitiateMultiPartUploadResponse(
            MultipartInfoInitiateResponse.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setMultipartUploadID(multipartUploadID)).build();

    S3InitiateMultipartUploadResponse s3InitiateMultipartUploadResponse =
        new S3InitiateMultipartUploadResponse(multipartKeyInfo, omKeyInfo,
            omResponse);

    s3InitiateMultipartUploadResponse.addToDBBatch(omMetadataManager,
        batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    Assert.assertNotNull(omMetadataManager.getOpenKeyTable().get(multipartKey));
    Assert.assertNotNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));

    Assert.assertEquals(multipartUploadID,
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());
  }
}
