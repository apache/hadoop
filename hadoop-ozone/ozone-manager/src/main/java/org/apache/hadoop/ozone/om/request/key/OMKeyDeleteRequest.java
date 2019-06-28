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

package org.apache.hadoop.ozone.om.request.key;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .DeleteKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles DeleteKey request.
 */
public class OMKeyDeleteRequest extends OMClientRequest
    implements OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyDeleteRequest.class);

  public OMKeyDeleteRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    DeleteKeyRequest deleteKeyRequest = getOmRequest().getDeleteKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs deleteKeyArgs =
        deleteKeyRequest.getKeyArgs();

    String volumeName = deleteKeyArgs.getVolumeName();
    String bucketName = deleteKeyArgs.getBucketName();
    String keyName = deleteKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyDeletes();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildKeyArgsAuditMap(deleteKeyArgs);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.DeleteKey).setStatus(
            OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.DELETE,
            volumeName, bucketName, keyName);
      }
    } catch (IOException ex) {
      LOG.error("Delete failed for Key: {} in volume/bucket:{}/{}",
          keyName, bucketName, volumeName, ex);
      omMetrics.incNumKeyDeleteFails();
      auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_KEY, auditMap,
          ex, userInfo));
      return new OMKeyCreateResponse(null, -1L,
          createErrorOMResponse(omResponse, ex));
    }

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    String objectKey = omMetadataManager.getOzoneKey(
        volumeName, bucketName, keyName);

    omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
        bucketName);
    IOException exception = null;
    OmKeyInfo omKeyInfo = null;
    try {

      // Not doing bucket/volume checks here. In this way we can avoid db
      // checks for them.
      // TODO: Once we have volume/bucket full cache, we can add
      // them back, as these checks will be inexpensive at that time.
      omKeyInfo = omMetadataManager.getKeyTable().get(objectKey);

      if (omKeyInfo == null) {
        throw new OMException("Key not found", KEY_NOT_FOUND);
      }

      // Update table cache.
      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
              keyName)),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      // No need to add cache entries to delete table. As delete table will
      // be used by DeleteKeyService only, not used for any client response
      // validation, so we don't need to add to cache.
      // TODO: Revisit if we need it later.

    } catch (IOException ex) {
      exception = ex;
    } finally {
      omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.DELETE_KEY, auditMap,
        exception, userInfo));

    // return response.
    if (exception == null) {
      omMetrics.decNumKeys();
      return new OMKeyDeleteResponse(omKeyInfo,
          omResponse.setDeleteKeyResponse(
              DeleteKeyResponse.newBuilder()).build());
    } else {
      omMetrics.incNumKeyDeleteFails();
      return new OMKeyDeleteResponse(null,
          createErrorOMResponse(omResponse, exception));
    }

  }
}
