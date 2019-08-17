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
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RenameKeyResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles rename key request.
 */
public class OMKeyRenameRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyRenameRequest.class);

  public OMKeyRenameRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();
    Preconditions.checkNotNull(renameKeyRequest);

    // Set modification time.
    KeyArgs.Builder newKeyArgs = renameKeyRequest.getKeyArgs().toBuilder()
            .setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setRenameKeyRequest(renameKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    RenameKeyRequest renameKeyRequest = getOmRequest().getRenameKeyRequest();

    OzoneManagerProtocolProtos.KeyArgs renameKeyArgs =
        renameKeyRequest.getKeyArgs();

    String volumeName = renameKeyArgs.getVolumeName();
    String bucketName = renameKeyArgs.getBucketName();
    String fromKeyName = renameKeyArgs.getKeyName();
    String toKeyName = renameKeyRequest.getToKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyRenames();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(renameKeyArgs);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.CommitKey).setStatus(
            OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    OMClientResponse omClientResponse = null;
    IOException exception = null;
    OmKeyInfo fromKeyValue = null;
    try {
      if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
        throw new OMException("Key name is empty",
            OMException.ResultCodes.INVALID_KEY_NAME);
      }
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, fromKeyName);
      }

      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

      // Not doing bucket/volume checks here. In this way we can avoid db
      // checks for them.
      // TODO: Once we have volume/bucket full cache, we can add
      // them back, as these checks will be inexpensive at that time.

      // fromKeyName should exist
      String fromKey = omMetadataManager.getOzoneKey(
          volumeName, bucketName, fromKeyName);
      fromKeyValue = omMetadataManager.getKeyTable().get(fromKey);
      if (fromKeyValue == null) {
        // TODO: Add support for renaming open key
        throw new OMException("Key not found " + fromKey, KEY_NOT_FOUND);
      }

      // toKeyName should not exist
      String toKey =
          omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
      OmKeyInfo toKeyValue = omMetadataManager.getKeyTable().get(toKey);
      if (toKeyValue != null) {
        throw new OMException("Key already exists " + toKeyName,
            OMException.ResultCodes.KEY_ALREADY_EXISTS);
      }

      fromKeyValue.setKeyName(toKeyName);

      //Set modification time
      fromKeyValue.setModificationTime(renameKeyArgs.getModificationTime());

      // Add to cache.
      // fromKey should be deleted, toKey should be added with newly updated
      // omKeyInfo.
      Table<String, OmKeyInfo> keyTable = omMetadataManager.getKeyTable();

      keyTable.addCacheEntry(new CacheKey<>(fromKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      keyTable.addCacheEntry(new CacheKey<>(toKey),
          new CacheValue<>(Optional.of(fromKeyValue), transactionLogIndex));

      omClientResponse = new OMKeyRenameResponse(fromKeyValue, toKeyName,
        fromKeyName, omResponse.setRenameKeyResponse(
            RenameKeyResponse.newBuilder()).build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMKeyRenameResponse(null, null, null,
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


    auditLog(auditLogger, buildAuditMessage(OMAction.RENAME_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    if (exception == null) {
      LOG.debug("Rename Key is successfully completed for volume:{} bucket:{}" +
          " fromKey:{} toKey:{}. ", volumeName, bucketName, fromKeyName,
          toKeyName);
      return omClientResponse;
    } else {
      LOG.error(
          "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
              + "Key: {} not found.", volumeName, bucketName, fromKeyName,
          toKeyName, fromKeyName);
      return omClientResponse;
    }
  }
}
