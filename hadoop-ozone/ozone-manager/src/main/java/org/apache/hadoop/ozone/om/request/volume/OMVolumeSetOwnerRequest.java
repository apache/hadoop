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

package org.apache.hadoop.ozone.om.request.volume;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeSetOwnerResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Handle set owner request for volume.
 */
public class OMVolumeSetOwnerRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetOwnerRequest.class);

  public OMVolumeSetOwnerRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    SetVolumePropertyRequest setVolumePropertyRequest =
        getOmRequest().getSetVolumePropertyRequest();

    Preconditions.checkNotNull(setVolumePropertyRequest);

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.SetVolumeProperty).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    // In production this will never happen, this request will be called only
    // when we have ownerName in setVolumePropertyRequest.
    if (!setVolumePropertyRequest.hasOwnerName()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMVolumeSetOwnerResponse(null, null, null, null,
          omResponse.build());
    }

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    String volume = setVolumePropertyRequest.getVolumeName();
    String newOwner = setVolumePropertyRequest.getOwnerName();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildVolumeAuditMap(volume);
    auditMap.put(OzoneConsts.OWNER, newOwner);

    boolean acquiredUserLocks = false;
    boolean acquiredVolumeLock = false;
    IOException exception = null;
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String oldOwner = null;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE_ACL,
            volume, null, null);
      }


      long maxUserVolumeCount = ozoneManager.getMaxUserVolumeCount();

      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);

      OzoneManagerProtocolProtos.VolumeList oldOwnerVolumeList = null;
      OzoneManagerProtocolProtos.VolumeList newOwnerVolumeList = null;
      OmVolumeArgs omVolumeArgs = null;



      acquiredVolumeLock = omMetadataManager.getLock().acquireLock(VOLUME_LOCK,
          volume);

      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);

      if (omVolumeArgs == null) {
        LOG.debug("Changing volume ownership failed for user:{} volume:{}",
            newOwner, volume);
        throw new OMException("Volume " + volume + " is not found",
            OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      oldOwner = omVolumeArgs.getOwnerName();

      acquiredUserLocks =
          omMetadataManager.getLock().acquireMultiUserLock(newOwner, oldOwner);

      oldOwnerVolumeList =
          omMetadataManager.getUserTable().get(oldOwner);

      oldOwnerVolumeList = delVolumeFromOwnerList(
          oldOwnerVolumeList, volume, oldOwner);

      newOwnerVolumeList = omMetadataManager.getUserTable().get(newOwner);
      newOwnerVolumeList = addVolumeToOwnerList(
          newOwnerVolumeList, volume, newOwner, maxUserVolumeCount);

      // Set owner with new owner name.
      omVolumeArgs.setOwnerName(newOwner);

      // Update cache.
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(newOwner)),
              new CacheValue<>(Optional.of(newOwnerVolumeList),
                  transactionLogIndex));
      omMetadataManager.getUserTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getUserKey(oldOwner)),
          new CacheValue<>(Optional.of(oldOwnerVolumeList),
              transactionLogIndex));
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

      omResponse.setSetVolumePropertyResponse(
          SetVolumePropertyResponse.newBuilder().build());
      omClientResponse = new OMVolumeSetOwnerResponse(oldOwner,
          oldOwnerVolumeList, newOwnerVolumeList, omVolumeArgs,
          omResponse.build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMVolumeSetOwnerResponse(null, null, null, null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredUserLocks) {
        omMetadataManager.getLock().releaseMultiUserLock(newOwner, oldOwner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_OWNER, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Successfully changed Owner of Volume {} from {} -> {}", volume,
          oldOwner, newOwner);
    } else {
      LOG.error("Changing volume ownership failed for user:{} volume:{}",
          newOwner, volume, exception);
      omMetrics.incNumVolumeUpdateFails();
    }
    return omClientResponse;
  }
}

