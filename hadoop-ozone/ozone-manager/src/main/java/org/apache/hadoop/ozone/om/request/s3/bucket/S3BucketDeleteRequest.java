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

package org.apache.hadoop.ozone.om.request.s3.bucket;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.s3.bucket.S3BucketDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3DeleteBucketRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.OzoneConsts.S3_BUCKET_MAX_LENGTH;
import static org.apache.hadoop.ozone.OzoneConsts.S3_BUCKET_MIN_LENGTH;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_BUCKET_LOCK;

/**
 * Handle Create S3Bucket request.
 */
public class S3BucketDeleteRequest extends OMVolumeRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketDeleteRequest.class);

  public S3BucketDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    S3DeleteBucketRequest s3DeleteBucketRequest =
        getOmRequest().getDeleteS3BucketRequest();

    // TODO: Do we need to enforce the bucket rules in this code path?
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

    // For now only checked the length.
    int bucketLength = s3DeleteBucketRequest.getS3BucketName().length();
    if (bucketLength < S3_BUCKET_MIN_LENGTH ||
        bucketLength >= S3_BUCKET_MAX_LENGTH) {
      throw new OMException("S3BucketName must be at least 3 and not more " +
          "than 63 characters long",
          OMException.ResultCodes.S3_BUCKET_INVALID_LENGTH);
    }

    return getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {
    S3DeleteBucketRequest s3DeleteBucketRequest =
        getOmRequest().getDeleteS3BucketRequest();

    String s3BucketName = s3DeleteBucketRequest.getS3BucketName();

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.DeleteS3Bucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumS3BucketDeletes();
    IOException exception = null;
    boolean acquiredS3Lock = false;
    boolean acquiredBucketLock = false;
    String volumeName = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.S3, IAccessAuthorizer.ACLType.DELETE, null,
            s3BucketName, null);
      }

      acquiredS3Lock = omMetadataManager.getLock().acquireLock(S3_BUCKET_LOCK,
          s3BucketName);

      String s3Mapping = omMetadataManager.getS3Table().get(s3BucketName);

      if (s3Mapping == null) {
        throw new OMException("S3Bucket " + s3BucketName + " not found",
            OMException.ResultCodes.S3_BUCKET_NOT_FOUND);
      } else {
        volumeName = getOzoneVolumeName(s3Mapping);

        acquiredBucketLock =
            omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
                s3BucketName);

        String bucketKey = omMetadataManager.getBucketKey(volumeName,
            s3BucketName);

        // Update bucket table cache and s3 table cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(bucketKey),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
        omMetadataManager.getS3Table().addCacheEntry(
            new CacheKey<>(s3BucketName),
            new CacheValue<>(Optional.absent(), transactionLogIndex));
      }

      omResponse.setDeleteS3BucketResponse(
          OzoneManagerProtocolProtos.S3DeleteBucketResponse.newBuilder());

      omClientResponse = new S3BucketDeleteResponse(s3BucketName, volumeName,
          omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3BucketDeleteResponse(null, null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredBucketLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            s3BucketName);
      }
      if (acquiredS3Lock) {
        omMetadataManager.getLock().releaseLock(S3_BUCKET_LOCK, s3BucketName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.DELETE_S3_BUCKET,
            buildAuditMap(s3BucketName), exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      // Decrement s3 bucket and ozone bucket count. As S3 bucket is mapped to
      // ozonevolume/ozone bucket.
      LOG.debug("S3Bucket {} successfully deleted", s3BucketName);
      omMetrics.decNumS3Buckets();
      omMetrics.decNumBuckets();

      return omClientResponse;
    } else {
      LOG.error("S3Bucket Deletion failed for S3Bucket:{}", s3BucketName,
          exception);
      omMetrics.incNumS3BucketDeleteFails();
      return omClientResponse;
    }
  }

  /**
   * Extract volumeName from s3Mapping.
   * @param s3Mapping
   * @return volumeName
   * @throws IOException
   */
  private String getOzoneVolumeName(String s3Mapping) throws IOException {
    return s3Mapping.split("/")[0];
  }

  private Map<String, String> buildAuditMap(String s3BucketName) {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(s3BucketName, OzoneConsts.S3_BUCKET);
    return auditMap;
  }

}