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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

/**
 * Tests S3 Multipart upload commit part request.
 */
public class TestS3MultipartUploadCommitPartRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteCommitMPU(volumeName, bucketName, keyName, Time.now(),
        UUID.randomUUID().toString(), 1);
  }


  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMRequest initiateMPURequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        new S3InitiateMultipartUploadRequest(initiateMPURequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
        1L, ozoneManagerDoubleBufferHelper);

    long clientID = Time.now();
    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(commitMultipartRequest);

    // Add key to open key table.
    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName,
        keyName, clientID, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
        2L, ozoneManagerDoubleBufferHelper);


    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.OK);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    Assert.assertNotNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    Assert.assertTrue(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey).getPartKeyInfoMap().size() == 1);
    Assert.assertNull(omMetadataManager.getOpenKeyTable()
        .get(omMetadataManager.getOpenKey(volumeName, bucketName, keyName,
            clientID)));

  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(commitMultipartRequest);

    // Add key to open key table.
    TestOMRequestUtils.addKeyToTable(true, volumeName, bucketName,
        keyName, clientID, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE, omMetadataManager);

    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L, ozoneManagerDoubleBufferHelper);


    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR);

    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));

  }

  @Test
  public void testValidateAndUpdateCacheKeyNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L, ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND);

  }


  @Test
  public void testValidateAndUpdateCacheBucketFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);


    long clientID = Time.now();
    String multipartUploadID = UUID.randomUUID().toString();

    OMRequest commitMultipartRequest = doPreExecuteCommitMPU(volumeName,
        bucketName, keyName, clientID, multipartUploadID, 1);

    // Don't add key to open table entry, and we are trying to commit this MPU
    // part. It will fail with KEY_NOT_FOUND

    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        new S3MultipartUploadCommitPartRequest(commitMultipartRequest);


    OMClientResponse omClientResponse =
        s3MultipartUploadCommitPartRequest.validateAndUpdateCache(ozoneManager,
            2L, ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
        == OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

  }
}
