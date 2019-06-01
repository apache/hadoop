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

package org.apache.hadoop.ozone.om.response.volume;

import java.io.IOException;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for set owner request.
 */
public class OMVolumeSetOwnerResponse extends OMClientResponse {

  private String oldOwner;
  private VolumeList oldOwnerVolumeList;
  private VolumeList newOwnerVolumeList;
  private OmVolumeArgs newOwnerVolumeArgs;

  public OMVolumeSetOwnerResponse(String oldOwner,
      VolumeList oldOwnerVolumeList, VolumeList newOwnerVolumeList,
      OmVolumeArgs newOwnerVolumeArgs, OMResponse omResponse) {
    super(omResponse);
    this.oldOwner = oldOwner;
    this.oldOwnerVolumeList = oldOwnerVolumeList;
    this.newOwnerVolumeList = newOwnerVolumeList;
    this.newOwnerVolumeArgs = newOwnerVolumeArgs;
  }

  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      String oldOwnerKey = omMetadataManager.getUserKey(oldOwner);
      String newOwnerKey =
          omMetadataManager.getUserKey(newOwnerVolumeArgs.getOwnerName());
      if (oldOwnerVolumeList.getVolumeNamesList().size() == 0) {
        omMetadataManager.getUserTable().deleteWithBatch(batchOperation,
            oldOwnerKey);
      } else {
        omMetadataManager.getUserTable().putWithBatch(batchOperation,
            oldOwnerKey, oldOwnerVolumeList);
      }
      omMetadataManager.getUserTable().putWithBatch(batchOperation, newOwnerKey,
          newOwnerVolumeList);

      String dbVolumeKey =
          omMetadataManager.getVolumeKey(newOwnerVolumeArgs.getVolume());
      omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
          dbVolumeKey, newOwnerVolumeArgs);
    }
  }
}
