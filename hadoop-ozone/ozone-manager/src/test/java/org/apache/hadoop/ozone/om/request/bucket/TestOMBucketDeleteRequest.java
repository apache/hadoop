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

package org.apache.hadoop.ozone.om.request.bucket;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;

/**
 * Tests OMBucketDeleteRequest class which handles DeleteBucket request.
 */
public class TestOMBucketDeleteRequest extends TestBucketRequest {

  @Test
  public void testPreExecute() throws Exception {
    OMRequest omRequest =
        createDeleteBucketRequest(UUID.randomUUID().toString(),
            UUID.randomUUID().toString());

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // As user info gets added.
    Assert.assertNotEquals(omRequest,
        omBucketDeleteRequest.preExecute(ozoneManager));
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);

    // Create Volume and bucket entries in DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
        ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));
  }


  @Test
  public void testValidateAndUpdateCacheFailure() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequest omRequest =
        createDeleteBucketRequest(volumeName, bucketName);

    OMBucketDeleteRequest omBucketDeleteRequest =
        new OMBucketDeleteRequest(omRequest);


    OMClientResponse omClientResponse =
        omBucketDeleteRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    Assert.assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName)));

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
  }




  private OMRequest createDeleteBucketRequest(String volumeName,
      String bucketName) {
    return OMRequest.newBuilder().setDeleteBucketRequest(
        DeleteBucketRequest.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName))
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
        .setClientId(UUID.randomUUID().toString()).build();
  }

}
