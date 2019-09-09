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
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketDeletes();
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    OMRequest omRequest = getOmRequest();
    String volumeName = omRequest.getDeleteBucketRequest().getVolumeName();
    String bucketName = omRequest.getDeleteBucketRequest().getBucketName();

    // Generate end user response
    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setCmdType(omRequest.getCmdType());


    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    Map<String, String> auditMap = buildVolumeAuditMap(volumeName);
    auditMap.put(OzoneConsts.BUCKET, bucketName);

    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    IOException exception = null;

    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, null);
      }


      // acquire lock
      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

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

      omResponse.setDeleteBucketResponse(
          DeleteBucketResponse.newBuilder().build());

      // Add to double buffer.
      omClientResponse = new OMBucketDeleteResponse(volumeName, bucketName,
          omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMBucketDeleteResponse(volumeName, bucketName,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_BUCKET,
        auditMap, exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.debug("Deleted bucket:{} in volume:{}", bucketName, volumeName);
      return omClientResponse;
    } else {
      omMetrics.incNumBucketDeleteFails();
      LOG.error("Delete bucket failed for bucket:{} in volume:{}", bucketName,
          volumeName, exception);
      return omClientResponse;
    }
  }
}
