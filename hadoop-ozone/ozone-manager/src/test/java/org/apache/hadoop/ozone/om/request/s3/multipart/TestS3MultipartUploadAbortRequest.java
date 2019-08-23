/*
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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Test Multipart upload abort request.
 */
public class TestS3MultipartUploadAbortRequest extends TestS3MultipartRequest {


  @Test
  public void testPreExecute() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    doPreExecuteAbortMPU(volumeName, bucketName, keyName,
        UUID.randomUUID().toString());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
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

    String multipartUploadID = omClientResponse.getOMResponse()
        .getInitiateMultiPartUploadResponse().getMultipartUploadID();

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);


    String multipartKey = omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);

    // Check table and response.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertNull(
        omMetadataManager.getMultipartInfoTable().get(multipartKey));
    Assert.assertNull(omMetadataManager.getOpenKeyTable().get(multipartKey));

  }

  @Test
  public void testValidateAndUpdateCacheMultipartNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    // Check table and response.
    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.NO_SUCH_MULTIPART_UPLOAD_ERROR,
        omClientResponse.getOMResponse().getStatus());

  }


  @Test
  public void testValidateAndUpdateCacheVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();


    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    // Check table and response.
    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testValidateAndUpdateCacheBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();


    TestOMRequestUtils.addVolumeToDB(volumeName, omMetadataManager);

    String multipartUploadID = "randomMPU";

    OMRequest abortMPURequest =
        doPreExecuteAbortMPU(volumeName, bucketName, keyName,
            multipartUploadID);

    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        new S3MultipartUploadAbortRequest(abortMPURequest);

    OMClientResponse omClientResponse =
        s3MultipartUploadAbortRequest.validateAndUpdateCache(ozoneManager, 2L,
            ozoneManagerDoubleBufferHelper);

    // Check table and response.
    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

  }
}
