/**
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

package org.apache.hadoop.ozone.om.request.key;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;

/**
 * Tests OmKeyDelete request.
 */
public class TestOMKeyDeleteRequest extends TestOMKeyRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecute(createDeleteKeyRequest());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);


    // Add volume, bucket and key entries to OM DB.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);


    String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    // As we added manually to key table.
    Assert.assertNotNull(omKeyInfo);

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
        100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    // Now after calling validateAndUpdateCache, it should be deleted.

    omKeyInfo = omMetadataManager.getKeyTable().get(ozoneKey);

    Assert.assertNull(omKeyInfo);

  }


  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    // Add only volume and bucket entry to DB.
    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);


    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithOutVolumeAndBucket()
      throws Exception {
    OMRequest modifiedOmRequest =
        doPreExecute(createDeleteKeyRequest());

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(modifiedOmRequest);

    // In actual implementation we don't check for bucket/volume exists
    // during delete key. So it should still return error KEY_NOT_FOUND

    OMClientResponse omClientResponse =
        omKeyDeleteRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());
  }


  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {

    OMKeyDeleteRequest omKeyDeleteRequest =
        new OMKeyDeleteRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyDeleteRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates DeleteKeyRequest.
   * @return OMRequest
   */
  private OMRequest createDeleteKeyRequest() {
    KeyArgs keyArgs = KeyArgs.newBuilder().setBucketName(bucketName)
        .setVolumeName(volumeName).setKeyName(keyName).build();

    DeleteKeyRequest deleteKeyRequest =
        DeleteKeyRequest.newBuilder().setKeyArgs(keyArgs).build();

    return OMRequest.newBuilder().setDeleteKeyRequest(deleteKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.DeleteKey)
        .setClientId(UUID.randomUUID().toString()).build();
  }

}
