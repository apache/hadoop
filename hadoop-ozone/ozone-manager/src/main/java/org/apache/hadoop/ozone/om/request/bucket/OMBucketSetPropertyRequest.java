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
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.om.request.OMClientRequest;

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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetBucketPropertyResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

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
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {


    SetBucketPropertyRequest setBucketPropertyRequest =
        getOmRequest().getSetBucketPropertyRequest();

    Preconditions.checkNotNull(setBucketPropertyRequest);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumBucketUpdates();


    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    BucketArgs bucketArgs = setBucketPropertyRequest.getBucketArgs();
    OmBucketArgs omBucketArgs = OmBucketArgs.getFromProtobuf(bucketArgs);

    String volumeName = bucketArgs.getVolumeName();
    String bucketName = bucketArgs.getBucketName();


    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.CreateBucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK);
    OmBucketInfo omBucketInfo = null;

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
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
      acquiredLock =  omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

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

      omResponse.setSetBucketPropertyResponse(
          SetBucketPropertyResponse.newBuilder().build());
      omClientResponse = new OMBucketSetPropertyResponse(omBucketInfo,
        omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMBucketSetPropertyResponse(omBucketInfo,
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
    auditLog(auditLogger, buildAuditMessage(OMAction.UPDATE_BUCKET,
        omBucketArgs.toAuditMap(), exception, userInfo));

    // return response.
    if (exception == null) {
      LOG.debug("Setting bucket property for bucket:{} in volume:{}",
          bucketName, volumeName);
      return omClientResponse;
    } else {
      LOG.error("Setting bucket property failed for bucket:{} in volume:{}",
          bucketName, volumeName, exception);
      omMetrics.incNumBucketUpdateFails();
      return omClientResponse;
    }
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
