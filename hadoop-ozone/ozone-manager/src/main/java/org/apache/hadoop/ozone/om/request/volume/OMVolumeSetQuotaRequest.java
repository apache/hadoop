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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeSetQuotaResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .SetVolumePropertyResponse;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;


/**
 * Handles set Quota request for volume.
 */
public class OMVolumeSetQuotaRequest extends OMVolumeRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeSetQuotaRequest.class);

  public OMVolumeSetQuotaRequest(OMRequest omRequest) {
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
    // when we have quota in bytes is set in setVolumePropertyRequest.
    if (!setVolumePropertyRequest.hasQuotaInBytes()) {
      omResponse.setStatus(OzoneManagerProtocolProtos.Status.INVALID_REQUEST)
          .setSuccess(false);
      return new OMVolumeSetQuotaResponse(null,
          omResponse.build());
    }

    String volume = setVolumePropertyRequest.getVolumeName();
    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();
    Map<String, String> auditMap = buildVolumeAuditMap(volume);
    auditMap.put(OzoneConsts.QUOTA,
        String.valueOf(setVolumePropertyRequest.getQuotaInBytes()));

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    IOException exception = null;
    boolean acquireVolumeLock = false;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE, volume,
            null, null);
      }

      OmVolumeArgs omVolumeArgs = null;

      acquireVolumeLock = omMetadataManager.getLock().acquireLock(VOLUME_LOCK,
          volume);
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);

      if (omVolumeArgs == null) {
        LOG.debug("volume:{} does not exist", volume);
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }

      omVolumeArgs.setQuotaInBytes(setVolumePropertyRequest.getQuotaInBytes());

      // update cache.
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));

      omResponse.setSetVolumePropertyResponse(
          SetVolumePropertyResponse.newBuilder().build());
      omClientResponse = new OMVolumeSetQuotaResponse(omVolumeArgs,
        omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMVolumeSetQuotaResponse(null,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      if (acquireVolumeLock) {
        omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
      }
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.SET_QUOTA, auditMap,
        exception, userInfo));

    // return response after releasing lock.
    if (exception == null) {
      LOG.debug("Changing volume quota is successfully completed for volume: " +
          "{} quota:{}", volume, setVolumePropertyRequest.getQuotaInBytes());
    } else {
      omMetrics.incNumVolumeUpdateFails();
      LOG.error("Changing volume quota failed for volume:{} quota:{}", volume,
          setVolumePropertyRequest.getQuotaInBytes(), exception);
    }
    return omClientResponse;
  }


}


