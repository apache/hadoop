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

package org.apache.hadoop.ozone.om.request.bucket;

import java.io.IOException;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

/**
 * Handles DeleteBucket Request.
 */
public class OMBucketDeleteRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketDeleteRequest.class);

  public OMBucketDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    // For Delete Bucket there are no preExecute steps
    return getOmRequest();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketDeletes();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    String volumeName = omRequest.getDeleteBucketRequest().getVolumeName();
    String bucketName = omRequest.getDeleteBucketRequest().getBucketName();

    // acquire lock
    omMetadataManager.getLock().acquireBucketLock(volumeName, bucketName);

    // Generate end user response
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(omRequest.getCmdType());

    try {
      // No need to check volume exists here, as bucket cannot be created
      // with out volume creation.
      //Check if bucket exists
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo omBucketInfo =
          omMetadataManager.getBucketTable().get(bucketKey);
      if (omBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      //Check if bucket is empty
      if (!omMetadataManager.isBucketEmpty(volumeName, bucketName)) {
        LOG.debug("bucket: {} is not empty ", bucketName);
        throw new OMException("Bucket is not empty",
            OMException.ResultCodes.BUCKET_NOT_EMPTY);
      }
      omMetrics.decNumBuckets();

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));
      // TODO: check acls.
    } catch (IOException ex) {
      omMetrics.incNumBucketDeleteFails();
      LOG.error("Delete bucket failed for bucket:{} in volume:{}", bucketName,
          volumeName, ex);
      omResponse.setSuccess(false).setMessage(ex.getMessage())
          .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    } finally {
      omMetadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
    return new OMBucketDeleteResponse(volumeName, bucketName,
        omResponse.build());
  }
}
