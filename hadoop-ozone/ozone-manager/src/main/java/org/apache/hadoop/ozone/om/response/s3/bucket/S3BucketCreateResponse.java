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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Response for S3Bucket create request.
 */
public class S3BucketCreateResponse extends OMClientResponse {

  private OMVolumeCreateResponse omVolumeCreateResponse;
  private OMBucketCreateResponse omBucketCreateResponse;
  private String s3Bucket;
  private String s3Mapping;

  public S3BucketCreateResponse(
      @Nullable OMVolumeCreateResponse omVolumeCreateResponse,
      @Nullable OMBucketCreateResponse omBucketCreateResponse,
      @Nullable String s3BucketName,
      @Nullable String s3Mapping, @Nonnull OMResponse omResponse) {
    super(omResponse);
    this.omVolumeCreateResponse = omVolumeCreateResponse;
    this.omBucketCreateResponse = omBucketCreateResponse;
    this.s3Bucket = s3BucketName;
    this.s3Mapping = s3Mapping;
  }

  @Override
  public void addToDBBatch(OMMetadataManager omMetadataManager,
      BatchOperation batchOperation) throws IOException {

    if (getOMResponse().getStatus() == OzoneManagerProtocolProtos.Status.OK) {
      if (omVolumeCreateResponse != null) {
        omVolumeCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
      }

      Preconditions.checkState(omBucketCreateResponse != null);
      omBucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

      omMetadataManager.getS3Table().putWithBatch(batchOperation, s3Bucket,
          s3Mapping);
    }
  }

  @VisibleForTesting
  public String getS3Mapping() {
    return s3Mapping;
  }
}
