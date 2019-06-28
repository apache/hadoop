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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.USER_LOCK;

/**
 * Handles volume create request.
 */
public class OMVolumeCreateRequest extends OMClientRequest
    implements OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeCreateRequest.class);

  public OMVolumeCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    VolumeInfo volumeInfo  =
        getOmRequest().getCreateVolumeRequest().getVolumeInfo();

    // Set creation time
    VolumeInfo updatedVolumeInfo =
        volumeInfo.toBuilder().setCreationTime(Time.now()).build();


    return getOmRequest().toBuilder().setCreateVolumeRequest(
        CreateVolumeRequest.newBuilder().setVolumeInfo(updatedVolumeInfo))
        .setUserInfo(getUserInfo())
        .build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {

    CreateVolumeRequest createVolumeRequest =
        getOmRequest().getCreateVolumeRequest();
    Preconditions.checkNotNull(createVolumeRequest);
    VolumeInfo volumeInfo = createVolumeRequest.getVolumeInfo();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeCreates();

    String volume = volumeInfo.getVolume();
    String owner = volumeInfo.getOwnerName();

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.CreateVolume).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    OmVolumeArgs omVolumeArgs = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    // Doing this here, so we can do protobuf conversion outside of lock.
    try {
      omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE, volume,
            null, null);
      }
    } catch (IOException ex) {
      omMetrics.incNumVolumeCreateFails();
      auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_VOLUME,
          buildVolumeAuditMap(volume), ex, userInfo));
      LOG.error("Volume creation failed for user:{} volume:{}", owner, volume,
          ex);
      return new OMVolumeCreateResponse(omVolumeArgs, null,
          createErrorOMResponse(omResponse, ex));
    }



    String dbUserKey = omMetadataManager.getUserKey(owner);
    String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
    VolumeList volumeList = null;
    boolean acquiredUserLock = false;
    IOException exception = null;

    // acquire lock.
    omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
    try {
      acquiredUserLock = omMetadataManager.getLock().acquireLock(USER_LOCK,
          owner);
      OmVolumeArgs dbVolumeArgs =
          omMetadataManager.getVolumeTable().get(dbVolumeKey);

      // Validation: Check if volume already exists
      if (dbVolumeArgs != null) {
        LOG.debug("volume:{} already exists", omVolumeArgs.getVolume());
        throw new OMException("Volume already exists",
            OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
      }

      volumeList = omMetadataManager.getUserTable().get(dbUserKey);
      volumeList = addVolumeToOwnerList(volumeList,
          volume, owner, ozoneManager.getMaxUserVolumeCount());

      // Update cache: Update user and volume cache.
      omMetadataManager.getUserTable().addCacheEntry(new CacheKey<>(dbUserKey),
          new CacheValue<>(Optional.of(volumeList), transactionLogIndex));

      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

    } catch (IOException ex) {
      exception = ex;
    } finally {
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseLock(USER_LOCK, owner);
      }
      omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_VOLUME,
        omVolumeArgs.toAuditMap(), exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("created volume:{} for user:{}", omVolumeArgs.getVolume(),
          owner);
      omMetrics.incNumVolumes();
      omResponse.setCreateVolumeResponse(CreateVolumeResponse.newBuilder()
          .build());
      return new OMVolumeCreateResponse(omVolumeArgs, volumeList,
          omResponse.build());
    } else {
      LOG.error("Volume creation failed for user:{} volume:{}", owner,
          volumeInfo.getVolume(), exception);
      omMetrics.incNumVolumeCreateFails();
      return new OMVolumeCreateResponse(omVolumeArgs, volumeList,
          createErrorOMResponse(omResponse, exception));
    }
  }


}
