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
package org.apache.hadoop.ozone.om.request.volume.acl;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeAddAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * Handles add acl request.
 */
public class OMVolumeAddAclRequest extends OMClientRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMVolumeAddAclRequest.class);

  public OMVolumeAddAclRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    AddAclRequest addAclRequest = getOmRequest().getAddAclRequest();
    Preconditions.checkNotNull(addAclRequest);

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.AddAcl).setStatus(
            Status.OK).setSuccess(true);

    if (!addAclRequest.hasAcl()) {
      omResponse.setStatus(Status.INVALID_REQUEST).setSuccess(false);
      return new OMVolumeAddAclResponse(null, omResponse.build());
    }

    OzoneObjInfo ozoneObj = OzoneObjInfo.fromProtobuf(addAclRequest.getObj());
    OzoneAcl ozoneAcl = OzoneAcl.fromProtobuf(addAclRequest.getAcl());
    String volume = ozoneObj.getVolumeName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumVolumeUpdates();
    IOException exception = null;
    OmVolumeArgs omVolumeArgs = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.VOLUME,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE, volume,
            null, null);
      }

      omMetadataManager.getLock().acquireLock(VOLUME_LOCK, volume);
      String dbVolumeKey = omMetadataManager.getVolumeKey(volume);
      omVolumeArgs = omMetadataManager.getVolumeTable().get(dbVolumeKey);

      if (omVolumeArgs == null) {
        throw new OMException(OMException.ResultCodes.VOLUME_NOT_FOUND);
      }
      omVolumeArgs.addAcl(ozoneAcl);

      // update cache.
      omMetadataManager.getVolumeTable().addCacheEntry(
          new CacheKey<>(dbVolumeKey),
          new CacheValue<>(Optional.of(omVolumeArgs), transactionLogIndex));
    } catch (IOException ex) {
      exception = ex;
    } finally {
      omMetadataManager.getLock().releaseLock(VOLUME_LOCK, volume);
    }

    // TODO: audit log
    if (exception == null) {
      omResponse.setAddAclResponse(
          AddAclResponse.newBuilder().setResponse(true).build());
      return new OMVolumeAddAclResponse(omVolumeArgs, omResponse.build());
    } else {
      omMetrics.incNumVolumeUpdateFails();
      LOG.error("Add ACL {} for volume {} failed", ozoneAcl, volume, exception);
      return new OMVolumeAddAclResponse(null,
          createErrorOMResponse(omResponse, exception));
    }
  }
}
