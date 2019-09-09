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

package org.apache.hadoop.ozone.om.request.s3.bucket;

import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static org.junit.Assert.fail;

/**
 * Tests S3BucketCreateRequest class, which handles S3 CreateBucket request.
 */
public class TestS3BucketCreateRequest extends TestS3BucketRequest {

  @Test
  public void testPreExecute() throws Exception {
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();
    doPreExecute(userName, s3BucketName);
  }

  @Test
  public void testPreExecuteInvalidBucketLength() throws Exception {
    String userName = UUID.randomUUID().toString();

    // set bucket name which is less than 3 characters length
    String s3BucketName = RandomStringUtils.randomAlphabetic(2);

    try {
      doPreExecute(userName, s3BucketName);
      fail("testPreExecuteInvalidBucketLength failed");
    } catch (OMException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_INVALID_LENGTH", ex);
    }

    // set bucket name which is greater than 63 characters length
    s3BucketName = RandomStringUtils.randomAlphabetic(64);

    try {
      doPreExecute(userName, s3BucketName);
      fail("testPreExecuteInvalidBucketLength failed");
    } catch (OMException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_INVALID_LENGTH", ex);
    }
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();

    S3BucketCreateRequest s3BucketCreateRequest = doPreExecute(userName,
        s3BucketName);

    doValidateAndUpdateCache(userName, s3BucketName,
        s3BucketCreateRequest.getOmRequest());

  }


  @Test
  public void testValidateAndUpdateCacheWithS3BucketAlreadyExists()
      throws Exception {
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();

    TestOMRequestUtils.addS3BucketToDB(
        S3BucketCreateRequest.formatOzoneVolumeName(userName), s3BucketName,
        omMetadataManager);

    S3BucketCreateRequest s3BucketCreateRequest =
        doPreExecute(userName, s3BucketName);


    // Try create same bucket again
    OMClientResponse omClientResponse =
        s3BucketCreateRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateBucketResponse());
    Assert.assertEquals(
        OzoneManagerProtocolProtos.Status.S3_BUCKET_ALREADY_EXISTS,
        omResponse.getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketAlreadyExists()
      throws Exception {
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();

    S3BucketCreateRequest s3BucketCreateRequest =
        doPreExecute(userName, s3BucketName);

    TestOMRequestUtils.addVolumeAndBucketToDB(
        s3BucketCreateRequest.formatOzoneVolumeName(userName),
        s3BucketName, omMetadataManager);


    // Try create same bucket again
    OMClientResponse omClientResponse =
        s3BucketCreateRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateBucketResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_ALREADY_EXISTS,
        omResponse.getStatus());
  }



  private S3BucketCreateRequest doPreExecute(String userName,
      String s3BucketName) throws Exception {
    OMRequest originalRequest =
        TestOMRequestUtils.createS3BucketRequest(userName, s3BucketName);

    S3BucketCreateRequest s3BucketCreateRequest =
        new S3BucketCreateRequest(originalRequest);

    OMRequest modifiedRequest = s3BucketCreateRequest.preExecute(ozoneManager);
    // Modification time will be set, so requests should not be equal.
    Assert.assertNotEquals(originalRequest, modifiedRequest);
    return new S3BucketCreateRequest(modifiedRequest);
  }

  private void doValidateAndUpdateCache(String userName, String s3BucketName,
      OMRequest modifiedRequest) throws Exception {

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assert.assertNull(omMetadataManager.getS3Table().get(s3BucketName));
    S3BucketCreateRequest s3BucketCreateRequest =
        new S3BucketCreateRequest(modifiedRequest);


    OMClientResponse omClientResponse =
        s3BucketCreateRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    // As now after validateAndUpdateCache it should add entry to cache, get
    // should return non null value.

    Assert.assertNotNull(omMetadataManager.getS3Table().get(s3BucketName));

    String bucketKey =
        omMetadataManager.getBucketKey(
            s3BucketCreateRequest.formatOzoneVolumeName(userName),
            s3BucketName);

    // check ozone bucket entry is created or not.
    Assert.assertNotNull(omMetadataManager.getBucketTable().get(bucketKey));

    String volumeKey = omMetadataManager.getVolumeKey(
        s3BucketCreateRequest.formatOzoneVolumeName(userName));

    // Check volume entry is created or not.
    Assert.assertNotNull(omMetadataManager.getVolumeTable().get(volumeKey));

    // check om response.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertEquals(OzoneManagerProtocolProtos.Type.CreateS3Bucket,
        omClientResponse.getOMResponse().getCmdType());

  }

}

