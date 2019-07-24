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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Response for Multipart Abort Request.
 */
public class S3MultipartUploadAbortResponse extends OMClientResponse {

  private String multipartKey;
  private long timeStamp;
  private OmMultipartKeyInfo omMultipartKeyInfo;

  public S3MultipartUploadAbortResponse(String multipartKey,
      long timeStamp,
      OmMultipartKeyInfo omMultipartKeyInfo,
      OMResponse omResponse) {
    super(omResponse);
    this.multipartKey = multipartKey;
    this.timeStamp = timeStamp;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {

      // Delete from openKey table and multipart info table.
      omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
          multipartKey);
      omMetadataManager.getMultipartInfoTable().deleteWithBatch(batchOperation,
          multipartKey);

      // Move all the parts to delete table
      TreeMap<Integer, PartKeyInfo > partKeyInfoMap =
          omMultipartKeyInfo.getPartKeyInfoMap();
      for (Map.Entry<Integer, PartKeyInfo > partKeyInfoEntry :
          partKeyInfoMap.entrySet()) {
        PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
        OmKeyInfo currentKeyPartInfo =
            OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            OmUtils.getDeletedKeyName(partKeyInfo.getPartName(), timeStamp),
            currentKeyPartInfo);
      }

    }
  }
}
