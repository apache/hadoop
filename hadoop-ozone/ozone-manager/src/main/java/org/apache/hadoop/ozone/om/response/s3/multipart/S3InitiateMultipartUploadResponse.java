/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.response.s3.multipart;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.utils.db.BatchOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Response for S3 Initiate Multipart Upload request.
 */
public class S3InitiateMultipartUploadResponse extends OMClientResponse {

  private OmMultipartKeyInfo omMultipartKeyInfo;
  private OmKeyInfo omKeyInfo;

  public S3InitiateMultipartUploadResponse(
      @Nullable OmMultipartKeyInfo omMultipartKeyInfo,
      @Nullable OmKeyInfo omKeyInfo,
      @Nonnull OzoneManagerProtocolProtos.OMResponse omResponse) {
    super(omResponse);
    this.omMultipartKeyInfo = omMultipartKeyInfo;
    this.omKeyInfo = omKeyInfo;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    // For OmResponse with failure, this should do nothing. This method is
    // not called in failure scenario in OM code.
    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {

      String multipartKey =
          omMetadataManager.getMultipartKey(omKeyInfo.getVolumeName(),
              omKeyInfo.getBucketName(), omKeyInfo.getKeyName(),
              omMultipartKeyInfo.getUploadID());

      omMetadataManager.getOpenKeyTable().putWithBatch(batchOperation,
          multipartKey, omKeyInfo);
      omMetadataManager.getMultipartInfoTable().putWithBatch(batchOperation,
          multipartKey, omMultipartKeyInfo);
    }
  }

  @VisibleForTesting
  public OmMultipartKeyInfo getOmMultipartKeyInfo() {
    return omMultipartKeyInfo;
  }

  @VisibleForTesting
  public OmKeyInfo getOmKeyInfo() {
    return omKeyInfo;
  }
}
