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
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
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
public class OMVolumeCreateRequest extends OMVolumeRequest {
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
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

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

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    // Doing this here, so we can do protobuf conversion outside of lock.
    boolean acquiredVolumeLock = false;
    boolean acquiredUserLock = false;
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    OmVolumeArgs omVolumeArgs = null;
    Map<String, String> auditMap = new HashMap<>();
    try {
      omVolumeArgs = OmVolumeArgs.getFromProtobuf(volumeInfo);
      auditMap = omVolumeArgs.toAuditMap();
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.CREATE, volume,
            null, null);
      }

      VolumeList volumeList = null;

      // acquire lock.
      acquiredVolumeLock = omMetadataManager.getLock().acquireLock(VOLUME_LOCK,
          volume);

      acquiredUserLock = omMetadataManager.getLock().acquireLock(USER_LOCK,
          owner);

      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);

      OmVolumeArgs dbVolumeArgs =
          omMetadataManager.getVolumeTable().get(dbVolumeKey);

      if (dbVolumeArgs == null) {
        String dbUserKey = omMetadataManager.getUserKey(owner);
        volumeList = omMetadataManager.getUserTable().get(dbUserKey);
        volumeList = addVolumeToOwnerList(volumeList, volume, owner,
            ozoneManager.getMaxUserVolumeCount());
        createVolume(omMetadataManager, omVolumeArgs, volumeList, dbVolumeKey,
            dbUserKey, transactionLogIndex);

        omResponse.setCreateVolumeResponse(CreateVolumeResponse.newBuilder()
            .build());
        omClientResponse = new OMVolumeCreateResponse(omVolumeArgs, volumeList,
            omResponse.build());
        LOG.debug("volume:{} successfully created", omVolumeArgs.getVolume());
      } else {
        LOG.debug("volume:{} already exists", omVolumeArgs.getVolume());
        throw new OMException("Volume already exists",
            OMException.ResultCodes.VOLUME_ALREADY_EXISTS);
      }

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMVolumeCreateResponse(null, null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquiredUserLock) {
        omMetadataManager.getLock().releaseLock(USER_LOCK, owner);
      }
      if (acquiredVolumeLock) {
        omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(ozoneManager.getAuditLogger(),
        buildAuditMessage(OMAction.CREATE_VOLUME, auditMap, exception,
            getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("created volume:{} for user:{}", volume, owner);
      omMetrics.incNumVolumes();
    } else {
      LOG.error("Volume creation failed for user:{} volume:{}", owner,
          volume, exception);
      omMetrics.incNumVolumeCreateFails();
    }
    return omClientResponse;
  }
}


