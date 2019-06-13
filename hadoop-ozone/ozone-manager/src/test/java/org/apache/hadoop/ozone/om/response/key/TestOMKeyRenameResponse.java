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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Tests OMKeyRenameResponse.
 */
public class TestOMKeyRenameResponse extends TestOMKeyResponse {
  @Test
  public void testAddToDBBatch() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();

    String toKeyName = UUID.randomUUID().toString();

    OMKeyRenameResponse omKeyRenameResponse =
        new OMKeyRenameResponse(omKeyInfo, toKeyName, keyName, omResponse);

    String ozoneFromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    String ozoneToKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        toKeyName);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable().isExist(ozoneToKey));

    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(omMetadataManager.getKeyTable().isExist(ozoneFromKey));
    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneToKey));
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();

    String toKeyName = UUID.randomUUID().toString();

    OMKeyRenameResponse omKeyRenameResponse =
        new OMKeyRenameResponse(omKeyInfo, toKeyName, keyName, omResponse);

    String ozoneFromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    String ozoneToKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        toKeyName);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable().isExist(ozoneToKey));

    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse has error, it is a no-op. So, no changes should happen.
    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable().isExist(ozoneToKey));

  }

  @Test
  public void testAddToDBBatchWithSameKeyName() throws Exception {

    OmKeyInfo omKeyInfo = TestOMRequestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();


    // Passing toKeyName also same as KeyName.
    OMKeyRenameResponse omKeyRenameResponse =
        new OMKeyRenameResponse(omKeyInfo, keyName, keyName, omResponse);

    String ozoneFromKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);

    TestOMRequestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
        clientID, replicationType, replicationFactor, omMetadataManager);

    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneFromKey));

    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertTrue(omMetadataManager.getKeyTable().isExist(ozoneFromKey));

  }
}
