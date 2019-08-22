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

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;

/**
 * Response for CommitKey request.
 */
public class OMKeyCommitResponse extends OMClientResponse {

  private OmKeyInfo omKeyInfo;
  private long openKeySessionID;

  public OMKeyCommitResponse(OmKeyInfo omKeyInfo, long openKeySessionID,
      OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
    this.openKeySessionID = openKeySessionID;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      String volumeName = omKeyInfo.getVolumeName();
      String bucketName = omKeyInfo.getBucketName();
      String keyName = omKeyInfo.getKeyName();
      String openKey = omMetadataManager.getOpenKey(volumeName,
          bucketName, keyName, openKeySessionID);
      String ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);

      // Delete from open key table and add entry to key table.
      omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
          openKey);
      omMetadataManager.getKeyTable().putWithBatch(batchOperation, ozoneKey,
          omKeyInfo);
    }
  }
}
