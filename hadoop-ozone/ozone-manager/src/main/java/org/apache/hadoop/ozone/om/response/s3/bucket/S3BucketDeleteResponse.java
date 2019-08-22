/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.response.s3.bucket;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Response for S3Bucket Delete request.
 */
public class S3BucketDeleteResponse extends OMClientResponse {

  private String s3BucketName;
  private String volumeName;
  public S3BucketDeleteResponse(@Nullable String s3BucketName,
      @Nullable String volumeName, @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.s3BucketName = s3BucketName;
    this.volumeName = volumeName;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      omMetadataManager.getBucketTable().deleteWithBatch(batchOperation,
          omMetadataManager.getBucketKey(volumeName, s3BucketName));
      omMetadataManager.getS3Table().deleteWithBatch(batchOperation,
          s3BucketName);
    }
  }
}