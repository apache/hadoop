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
import java.util.List;

import com.google.common.base.Optional;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketSetPropertyResponse;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;

import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

/**
 * Handle SetBucketProperty Request.
 */
public class OMBucketSetPropertyRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMBucketSetPropertyRequest.class);

  public OMBucketSetPropertyRequest(OMRequest omRequest) {
    super(omRequest);
  }
  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    return getOmRequest();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {

    OMMetrics omMetrics = ozoneManager.getOmMetrics();

    // This will never be null, on a real Ozone cluster. For tests this might
    // be null. using mockito, to set omMetrics object, but still getting
    // null. For now added this not null check.
    //TODO: Removed not null check from here, once tests got fixed.
    if (omMetrics != null) {
      omMetrics.incNumBucketUpdates();
    }

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    BucketArgs bucketArgs =
        getOmRequest().getSetBucketPropertyRequest().getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);

    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();

    // acquire lock
    omMetadataManager.getLock().acquireBucketLock(volumeName, bucketName);

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.CreateBucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK);
    OmBucketInfo omBucketInfo = null;

    try {
      String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
      OmBucketInfo oldBucketInfo =
          omMetadataManager.getBucketTable().get(bucketKey);
      //Check if bucket exist
      if (oldBucketInfo == null) {
        LOG.debug("bucket: {} not found ", bucketName);
        throw new OMException("Bucket doesn't exist",
            OMException.ResultCodes.BUCKET_NOT_FOUND);
      }
      OmBucketInfo.Builder bucketInfoBuilder = OmBucketInfo.newBuilder();
      bucketInfoBuilder.setVolumeName(oldBucketInfo.getVolumeName())
          .setBucketName(oldBucketInfo.getBucketName());
      bucketInfoBuilder.addAllMetadata(KeyValueUtil
          .getFromProtobuf(bucketArgs.getMetadataList()));

      //Check ACLs to update
      if (omBucketArgs.getAddAcls() != null ||
          omBucketArgs.getRemoveAcls() != null) {
        bucketInfoBuilder.setAcls(getUpdatedAclList(oldBucketInfo.getAcls(),
            omBucketArgs.getRemoveAcls(), omBucketArgs.getAddAcls()));
        LOG.debug("Updating ACLs for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder.setAcls(oldBucketInfo.getAcls());
      }

      //Check StorageType to update
      StorageType storageType = omBucketArgs.getStorageType();
      if (storageType != null) {
        bucketInfoBuilder.setStorageType(storageType);
        LOG.debug("Updating bucket storage type for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder.setStorageType(oldBucketInfo.getStorageType());
      }

      //Check Versioning to update
      Boolean versioning = omBucketArgs.getIsVersionEnabled();
      if (versioning != null) {
        bucketInfoBuilder.setIsVersionEnabled(versioning);
        LOG.debug("Updating bucket versioning for bucket: {} in volume: {}",
            bucketName, volumeName);
      } else {
        bucketInfoBuilder
            .setIsVersionEnabled(oldBucketInfo.getIsVersionEnabled());
      }
      bucketInfoBuilder.setCreationTime(oldBucketInfo.getCreationTime());

      omBucketInfo = bucketInfoBuilder.build();

      // Update table cache.
      omMetadataManager.getBucketTable().addCacheEntry(
          new CacheKey<>(bucketKey),
          new CacheValue<>(Optional.of(omBucketInfo), transactionLogIndex));

      // TODO: check acls.
    } catch (IOException ex) {
      if (omMetrics != null) {
        omMetrics.incNumBucketUpdateFails();
      }
      LOG.error("Setting bucket property failed for bucket:{} in volume:{}",
          bucketName, volumeName, ex);
      omResponse.setSuccess(false).setMessage(ex.getMessage())
          .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    } finally {
      omMetadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
    return new OMBucketSetPropertyResponse(omBucketInfo, omResponse.build());
  }

  /**
   * Updates the existing ACL list with remove and add ACLs that are passed.
   * Remove is done before Add.
   *
   * @param existingAcls - old ACL list.
   * @param removeAcls - ACLs to be removed.
   * @param addAcls - ACLs to be added.
   * @return updated ACL list.
   */
  private List< OzoneAcl > getUpdatedAclList(List<OzoneAcl> existingAcls,
      List<OzoneAcl> removeAcls, List<OzoneAcl> addAcls) {
    if (removeAcls != null && !removeAcls.isEmpty()) {
      existingAcls.removeAll(removeAcls);
    }
    if (addAcls != null && !addAcls.isEmpty()) {
      addAcls.stream().filter(acl -> !existingAcls.contains(acl)).forEach(
          existingAcls::add);
    }
    return existingAcls;
  }
}
