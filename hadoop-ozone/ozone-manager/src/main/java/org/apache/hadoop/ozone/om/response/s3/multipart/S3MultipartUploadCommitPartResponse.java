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
import org.apache.hadoop.utils.db.BatchOperation;

import java.io.IOException;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .Status.OK;

/**
 * Response for S3MultipartUploadCommitPart request.
 */
public class S3MultipartUploadCommitPartResponse extends OMClientResponse {

  private String multipartKey;
  private String openKey;
  private long deleteTimeStamp;
  private OmKeyInfo deletePartKeyInfo;
  private OmMultipartKeyInfo omMultipartKeyInfo;
  private OzoneManagerProtocolProtos.PartKeyInfo oldMultipartKeyInfo;


  public S3MultipartUploadCommitPartResponse(String multipartKey,
      String openKey, long deleteTimeStamp,
      OmKeyInfo deletePartKeyInfo, OmMultipartKeyInfo omMultipartKeyInfo,
      OzoneManagerProtocolProtos.PartKeyInfo oldPartKeyInfo,
      OMResponse omResponse) {
    super(omResponse);
    this.multipartKey = multipartKey;
    this.openKey = openKey;
    this.deleteTimeStamp = deleteTimeStamp;
    this.deletePartKeyInfo = deletePartKeyInfo;
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.oldMultipartKeyInfo = oldPartKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {


    if (getOMResponse().getStatus() == NO_SUCH_MULTIPART_UPLOAD_ERROR) {
      // Means by the time we try to commit part, some one has aborted this
      // multipart upload. So, delete this part information.
      omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
          OmUtils.getDeletedKeyName(openKey, deleteTimeStamp),
          deletePartKeyInfo);
    }

    if (getOMResponse().getStatus() == OK) {

      // If we have old part info:
      // Need to do 3 steps:
      //   1. add old part to delete table
      //   2. Commit multipart info which has information about this new part.
      //   3. delete this new part entry from open key table.

      // This means for this multipart upload part upload, we have an old
      // part information, so delete it.
      if (oldMultipartKeyInfo != null) {
        omMetadataManager.getDeletedTable().putWithBatch(batchOperation,
            OmUtils.getDeletedKeyName(oldMultipartKeyInfo.getPartName(),
                deleteTimeStamp),
            OmKeyInfo.getFromProtobuf(oldMultipartKeyInfo.getPartKeyInfo()));
      }


      omMetadataManager.getMultipartInfoTable().putWithBatch(batchOperation,
          multipartKey, omMultipartKeyInfo);

      //  This information has been added to multipartKeyInfo. So, we can
      //  safely delete part key info from open key table.
      omMetadataManager.getOpenKeyTable().deleteWithBatch(batchOperation,
          openKey);


    }
  }



}

