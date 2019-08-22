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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketCreateResponse;
import org.apache.hadoop.ozone.om.response.s3.bucket.S3BucketCreateResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3CreateVolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;


import static org.apache.hadoop.ozone.OzoneConsts.OM_S3_VOLUME_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.S3_BUCKET_MAX_LENGTH;
import static org.apache.hadoop.ozone.OzoneConsts.S3_BUCKET_MIN_LENGTH;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.S3_BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Handles S3 Bucket create request.
 */
public class S3BucketCreateRequest extends OMVolumeRequest {

  private static final String S3_ADMIN_NAME = "OzoneS3Manager";

  private static final Logger LOG =
      LoggerFactory.getLogger(S3CreateBucketRequest.class);

  public S3BucketCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    S3CreateBucketRequest s3CreateBucketRequest =
        getOmRequest().getCreateS3BucketRequest();
    Preconditions.checkNotNull(s3CreateBucketRequest);

    S3CreateBucketRequest.Builder newS3CreateBucketRequest =
        s3CreateBucketRequest.toBuilder().setS3CreateVolumeInfo(
            S3CreateVolumeInfo.newBuilder().setCreationTime(Time.now()));

    // TODO: Do we need to enforce the bucket rules in this code path?
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html

    // For now only checked the length.
    int bucketLength = s3CreateBucketRequest.getS3Bucketname().length();
    if (bucketLength < S3_BUCKET_MIN_LENGTH ||
        bucketLength >= S3_BUCKET_MAX_LENGTH) {
      throw new OMException("S3BucketName must be at least 3 and not more " +
          "than 63 characters long",
          OMException.ResultCodes.S3_BUCKET_INVALID_LENGTH);
    }

    return getOmRequest().toBuilder()
        .setCreateS3BucketRequest(newS3CreateBucketRequest)
        .setUserInfo(getUserInfo()).build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    S3CreateBucketRequest s3CreateBucketRequest =
        getOmRequest().getCreateS3BucketRequest();

    String userName = s3CreateBucketRequest.getUserName();
    String s3BucketName = s3CreateBucketRequest.getS3Bucketname();

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.CreateS3Bucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumS3BucketCreates();

    // When s3 Bucket is created, we internally create ozone volume/ozone
    // bucket.

    // ozone volume name is generated from userName by calling
    // formatOzoneVolumeName.

    // ozone bucket name is same as s3 bucket name.
    // In S3 buckets are unique, so we create a mapping like s3BucketName ->
    // ozoneVolume/ozoneBucket and add it to s3 mapping table. If
    // s3BucketName exists in mapping table, bucket already exist or we go
    // ahead and create a bucket.
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;

    boolean volumeCreated = false;
    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    boolean acquiredS3Lock = false;
    String volumeName = formatOzoneVolumeName(userName);
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.S3, IAccessAuthorizer.ACLType.CREATE, null,
            s3BucketName, null);
      }

      acquiredS3Lock = omMetadataManager.getLock().acquireLock(S3_BUCKET_LOCK,
          s3BucketName);

      // First check if this s3Bucket exists
      if (omMetadataManager.getS3Table().isExist(s3BucketName)) {
        throw new OMException("S3Bucket " + s3BucketName + " already exists",
            OMException.ResultCodes.S3_BUCKET_ALREADY_EXISTS);
      }

      OMVolumeCreateResponse omVolumeCreateResponse = null;
      try {
        acquiredVolumeLock =
            omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volumeName);
        acquiredUserLock = omMetadataManager.getLock().acquireLock(USER_LOCK,
            userName);
        // Check if volume exists, if it does not exist create
        // ozone volume.
        String volumeKey = omMetadataManager.getVolumeKey(volumeName);
        if (!omMetadataManager.getVolumeTable().isExist(volumeKey)) {
          OmVolumeArgs omVolumeArgs = createOmVolumeArgs(volumeName, userName,
              s3CreateBucketRequest.getS3CreateVolumeInfo()
                  .getCreationTime());
          VolumeList volumeList = omMetadataManager.getUserTable().get(
              omMetadataManager.getUserKey(userName));
          volumeList = addVolumeToOwnerList(volumeList,
              volumeName, userName, ozoneManager.getMaxUserVolumeCount());
          createVolume(omMetadataManager, omVolumeArgs, volumeList, volumeKey,
              omMetadataManager.getUserKey(userName), transactionLogIndex);
          volumeCreated = true;
          omVolumeCreateResponse = new OMVolumeCreateResponse(omVolumeArgs,
              volumeList, omResponse.build());
        }
      } finally {
        if (acquiredUserLock) {
          omMetadataManager.getLock().releaseLock(USER_LOCK, userName);
        }
        if (acquiredVolumeLock) {
          omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volumeName);
        }
      }

      // check if ozone bucket exists, if it does not exist create ozone
      // bucket
      OmBucketInfo omBucketInfo = createBucket(omMetadataManager, volumeName,
          s3BucketName,
          s3CreateBucketRequest.getS3CreateVolumeInfo().getCreationTime(),
          transactionLogIndex);

      // Now finally add it to s3 table cache.
      omMetadataManager.getS3Table().addCacheEntry(
          new CacheKey<>(s3BucketName), new CacheValue<>(
              Optional.of(formatS3MappingName(volumeName, s3BucketName)),
              transactionLogIndex));

      OMBucketCreateResponse omBucketCreateResponse =
          new OMBucketCreateResponse(omBucketInfo, omResponse.build());

      omClientResponse = new S3BucketCreateResponse(omVolumeCreateResponse,
          omBucketCreateResponse, s3BucketName,
          formatS3MappingName(volumeName, s3BucketName),
          omResponse.setCreateS3BucketResponse(
              S3CreateBucketResponse.newBuilder()).build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new S3BucketCreateResponse(null, null, null, null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredS3Lock) {
        omMetadataManager.getLock().releaseLock(S3_BUCKET_LOCK, s3BucketName);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_S3_BUCKET,
            buildAuditMap(userName, s3BucketName), exception,
            getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("S3Bucket is successfully created for userName: {}, " +
          "s3BucketName {}, volumeName {}", userName, s3BucketName, volumeName);
      OMVolumeCreateResponse omVolumeCreateResponse = null;
      if (volumeCreated) {
        omMetrics.incNumVolumes();
      }
      omMetrics.incNumBuckets();
      omMetrics.incNumS3Buckets();

      return omClientResponse;
    } else {
      LOG.error("S3Bucket Creation Failed for userName: {}, s3BucketName {}, " +
          "VolumeName {}", userName, s3BucketName, volumeName);
      omMetrics.incNumS3BucketCreateFails();
      return omClientResponse;
    }
  }


  private OmBucketInfo createBucket(OMMetadataManager omMetadataManager,
      String volumeName, String s3BucketName, long creationTime,
      long transactionLogIndex) throws IOException {
    // check if ozone bucket exists, if it does not exist create ozone
    // bucket
    boolean acquireBucketLock = false;
    OmBucketInfo omBucketInfo = null;
    try {
      acquireBucketLock =
          omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
              s3BucketName);
      String bucketKey = omMetadataManager.getBucketKey(volumeName,
          s3BucketName);
      if (!omMetadataManager.getBucketTable().isExist(bucketKey)) {
        omBucketInfo = createOmBucketInfo(volumeName, s3BucketName,
            creationTime);
        // Add to bucket table cache.
        omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(bucketKey),
            new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));
      } else {
        // This can happen when a ozone bucket exists already in the
        // volume, but this is not a s3 bucket.
        throw new OMException("Bucket " + s3BucketName + " already exists",
            OMException.ResultCodes.BUCKET_ALREADY_EXISTS);
      }
    } finally {
      if (acquireBucketLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            s3BucketName);
      }
    }
    return omBucketInfo;
  }

  /**
   * Generate Ozone volume name from userName.
   * @param userName
   * @return volume name
   */
  @VisibleForTesting
  public static String formatOzoneVolumeName(String userName) {
    return String.format(OM_S3_VOLUME_PREFIX + "%s", userName);
  }

  /**
   * Generate S3Mapping for provided volume and bucket. This information will
   * be persisted in s3 table in OM DB.
   * @param volumeName
   * @param bucketName
   * @return s3Mapping
   */
  @VisibleForTesting
  public static String formatS3MappingName(String volumeName,
      String bucketName) {
    return String.format("%s" + OzoneConsts.OM_KEY_PREFIX + "%s", volumeName,
        bucketName);
  }

  /**
   * Create {@link OmVolumeArgs} which needs to be persisted in volume table
   * in OM DB.
   * @param volumeName
   * @param userName
   * @param creationTime
   * @return {@link OmVolumeArgs}
   */
  private OmVolumeArgs createOmVolumeArgs(String volumeName, String userName,
      long creationTime) {
    return OmVolumeArgs.newBuilder()
        .setAdminName(S3_ADMIN_NAME).setVolume(volumeName)
        .setQuotaInBytes(OzoneConsts.MAX_QUOTA_IN_BYTES)
        .setOwnerName(userName)
        .setCreationTime(creationTime).build();
  }

  /**
   * Create {@link OmBucketInfo} which needs to be persisted in to bucket table
   * in OM DB.
   * @param volumeName
   * @param s3BucketName
   * @param creationTime
   * @return {@link OmBucketInfo}
   */
  private OmBucketInfo createOmBucketInfo(String volumeName,
      String s3BucketName, long creationTime) {
    //TODO: Now S3Bucket API takes only bucketName as param. In future if we
    // support some configurable options we need to fix this.
    return OmBucketInfo.newBuilder().setVolumeName(volumeName)
        .setBucketName(s3BucketName).setIsVersionEnabled(Boolean.FALSE)
        .setStorageType(StorageType.DEFAULT).setCreationTime(creationTime)
        .build();
  }

  /**
   * Build auditMap.
   * @param userName
   * @param s3BucketName
   * @return auditMap
   */
  private Map<String, String> buildAuditMap(String userName,
      String s3BucketName) {
    Map<String, String> auditMap = new HashMap<>();
    auditMap.put(userName, OzoneConsts.USERNAME);
    auditMap.put(s3BucketName, OzoneConsts.S3_BUCKET);
    return auditMap;
  }
}

