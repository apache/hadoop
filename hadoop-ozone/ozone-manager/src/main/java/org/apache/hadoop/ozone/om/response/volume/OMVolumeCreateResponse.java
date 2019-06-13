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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;

import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for CreateBucket request.
 */
public class OMVolumeCreateResponse extends OMClientResponse {

  private VolumeList volumeList;
  private OmVolumeArgs omVolumeArgs;

  public OMVolumeCreateResponse(OmVolumeArgs omVolumeArgs,
      VolumeList volumeList, OMResponse omResponse) {
    super(omResponse);
    this.omVolumeArgs = omVolumeArgs;
    this.volumeList = volumeList;
  }
  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      String dbVolumeKey =
          omMetadataManager.getVolumeKey(omVolumeArgs.getVolume());
      String dbUserKey =
          omMetadataManager.getUserKey(omVolumeArgs.getOwnerName());

      omMetadataManager.getVolumeTable().putWithBatch(batchOperation,
          dbVolumeKey, omVolumeArgs);
      omMetadataManager.getUserTable().putWithBatch(batchOperation, dbUserKey,
          volumeList);
    }
  }

  @VisibleForTesting
  public OmVolumeArgs getOmVolumeArgs() {
    return omVolumeArgs;
  }

}

