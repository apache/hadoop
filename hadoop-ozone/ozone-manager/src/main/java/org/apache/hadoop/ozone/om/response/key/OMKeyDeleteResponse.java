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

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.hdds.utils.db.BatchOperation;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.annotation.Nonnull;

/**
 * Response for DeleteKey request.
 */
public class OMKeyDeleteResponse extends OMClientResponse {
  private OmKeyInfo omKeyInfo;

  public OMKeyDeleteResponse(@Nullable OmKeyInfo omKeyInfo,
      @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      String ozoneKey = omMetadataManager.getOzoneKey(omKeyInfo.getVolumeName(),
          omKeyInfo.getBucketName(), omKeyInfo.getKeyName());
      omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
          ozoneKey);

      // If Key is not empty add this to delete table.
      if (!isKeyEmpty(omKeyInfo)) {
        // If a deleted key is put in the table where a key with the same
        // name already exists, then the old deleted key information would be
        // lost. To avoid this, first check if a key with same name exists.
        // deletedTable in OM Metadata stores <KeyName, RepeatedOMKeyInfo>.
        // The RepeatedOmKeyInfo is the structure that allows us to store a
        // list of OmKeyInfo that can be tied to same key name. For a keyName
        // if RepeatedOMKeyInfo structure is null, we create a new instance,
        // if it is not null, then we simply add to the list and store this
        // instance in deletedTable.
        RepeatedOmKeyInfo repeatedOmKeyInfo =
            omMetadataManager.getDeletedTable().get(ozoneKey);
        repeatedOmKeyInfo = OmUtils.prepareKeyForDelete(
            omKeyInfo, repeatedOmKeyInfo);
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            ozoneKey, repeatedOmKeyInfo);
      }
    }
  }

  /**
   * Check if the key is empty or not. Key will be empty if it does not have
   * blocks.
   *
   * @param keyInfo
   * @return if empty true, else false.
   */
  private boolean isKeyEmpty(@Nullable OmKeyInfo keyInfo) {
    if (keyInfo == null) {
      return true;
    }
    for (OmKeyLocationInfoGroup keyLocationList : keyInfo
        .getKeyLocationVersions()) {
      if (keyLocationList.getLocationList().size() != 0) {
        return false;
      }
    }
    return true;
  }
}
