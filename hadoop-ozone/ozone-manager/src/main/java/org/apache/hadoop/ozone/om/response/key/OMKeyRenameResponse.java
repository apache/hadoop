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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;

/**
 * Response for RenameKey request.
 */
public class OMKeyRenameResponse extends OMClientResponse {

  private final OmKeyInfo renameKeyInfo;
  private final String toKeyName;
  private final String fromKeyName;

  public OMKeyRenameResponse(OmKeyInfo renameKeyInfo, String toKeyName,
      String fromKeyName, OMResponse omResponse) {
    super(omResponse);
    this.renameKeyInfo = renameKeyInfo;
    this.toKeyName = toKeyName;
    this.fromKeyName = fromKeyName;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {
    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {

      // If both from and toKeyName are equal do nothing
      if (!toKeyName.equals(fromKeyName)) {
        String volumeName = renameKeyInfo.getVolumeName();
        String bucketName = renameKeyInfo.getBucketName();
        omMetadataManager.getKeyTable().deleteWithBatch(batchOperation,
            omMetadataManager.getOzoneKey(volumeName, bucketName, fromKeyName));
        omMetadataManager.getKeyTable().putWithBatch(batchOperation,
            omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName),
            renameKeyInfo);
      }
    }
  }
}
