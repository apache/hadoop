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

package org.apache.hadoop.ozone.om.response.key;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

/**
 * Tests OMAllocateBlockResponse.
 */
public class TestOMAllocateBlockResponse extends TestOMKeyResponse {

  @Test
  public void testAddToDBBatch() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateBlockResponse(
            AllocateBlockResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .build();
    OMAllocateBlockResponse omAllocateBlockResponse =
        new OMAllocateBlockResponse(omKeyInfo, clientID, omResponse);

    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);

    // Not adding key entry before to test whether commit is successful or not.
    Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(openKey));
    omAllocateBlockResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertTrue(omMetadataManager.getOpenKeyTable().isExist(openKey));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);

    OMResponse omResponse = OMResponse.newBuilder()
        .setAllocateBlockResponse(
            AllocateBlockResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.AllocateBlock)
        .build();
    OMAllocateBlockResponse omAllocateBlockResponse =
        new OMAllocateBlockResponse(omKeyInfo, clientID, omResponse);

    // Before calling addToDBBatch
    String openKey = omMetadataManager.getOpenKey(volumeName, bucketName,
        keyName, clientID);
    Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(openKey));

    omAllocateBlockResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse is error it is a no-op.
    Assert.assertFalse(omMetadataManager.getOpenKeyTable().isExist(openKey));

  }
}
