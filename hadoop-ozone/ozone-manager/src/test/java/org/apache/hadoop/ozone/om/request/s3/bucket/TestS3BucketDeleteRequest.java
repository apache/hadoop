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
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.test.GenericTestUtils;

import static org.junit.Assert.fail;

/**
 * Tests S3BucketDelete Request.
 */
public class TestS3BucketDeleteRequest extends TestS3BucketRequest {

  @Test
  public void testPreExecute() throws Exception {
    String s3BucketName = UUID.randomUUID().toString();
    doPreExecute(s3BucketName);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String s3BucketName = UUID.randomUUID().toString();
    OMRequest omRequest = doPreExecute(s3BucketName);

    // Add s3Bucket to s3Bucket table.
    TestOMRequestUtils.addS3BucketToDB("ozone", s3BucketName,
        omMetadataManager);

    S3BucketDeleteRequest s3BucketDeleteRequest =
        new S3BucketDeleteRequest(omRequest);

    OMClientResponse s3BucketDeleteResponse =
        s3BucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        s3BucketDeleteResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithS3BucketNotFound()
      throws Exception {
    String s3BucketName = UUID.randomUUID().toString();
    OMRequest omRequest = doPreExecute(s3BucketName);

    S3BucketDeleteRequest s3BucketDeleteRequest =
        new S3BucketDeleteRequest(omRequest);

    OMClientResponse s3BucketDeleteResponse =
        s3BucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1L,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.S3_BUCKET_NOT_FOUND,
        s3BucketDeleteResponse.getOMResponse().getStatus());
  }

  @Test
  public void testPreExecuteInvalidBucketLength() throws Exception {
    // set bucket name which is less than 3 characters length
    String s3BucketName = RandomStringUtils.randomAlphabetic(2);

    try {
      doPreExecute(s3BucketName);
      fail("testPreExecuteInvalidBucketLength failed");
    } catch (OMException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_INVALID_LENGTH", ex);
    }

    // set bucket name which is less than 3 characters length
    s3BucketName = RandomStringUtils.randomAlphabetic(65);

    try {
      doPreExecute(s3BucketName);
      fail("testPreExecuteInvalidBucketLength failed");
    } catch (OMException ex) {
      GenericTestUtils.assertExceptionContains("S3_BUCKET_INVALID_LENGTH", ex);
    }
  }

  private OMRequest doPreExecute(String s3BucketName) throws Exception {
    OMRequest omRequest =
        TestOMRequestUtils.deleteS3BucketRequest(s3BucketName);

    S3BucketDeleteRequest s3BucketDeleteRequest =
        new S3BucketDeleteRequest(omRequest);

    OMRequest modifiedOMRequest =
        s3BucketDeleteRequest.preExecute(ozoneManager);

    // As user name will be set both should not be equal.
    Assert.assertNotEquals(omRequest, modifiedOMRequest);

    return modifiedOMRequest;
  }
}
