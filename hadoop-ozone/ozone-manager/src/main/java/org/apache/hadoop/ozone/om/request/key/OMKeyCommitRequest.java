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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CommitKey request.
 */
public class OMKeyCommitRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCommitRequest.class);

  public OMKeyCommitRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();
    Preconditions.checkNotNull(commitKeyRequest);

    KeyArgs keyArgs = commitKeyRequest.getKeyArgs();

    KeyArgs.Builder newKeyArgs =
        keyArgs.toBuilder().setModificationTime(Time.now());

    return getOmRequest().toBuilder()
        .setCommitKeyRequest(commitKeyRequest.toBuilder()
            .setKeyArgs(newKeyArgs)).setUserInfo(getUserInfo()).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    CommitKeyRequest commitKeyRequest = getOmRequest().getCommitKeyRequest();

    KeyArgs commitKeyArgs = commitKeyRequest.getKeyArgs();

    String volumeName = commitKeyArgs.getVolumeName();
    String bucketName = commitKeyArgs.getBucketName();
    String keyName = commitKeyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyCommits();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(commitKeyArgs);

    OzoneManagerProtocolProtos.OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.CommitKey).setStatus(
            OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    IOException exception = null;
    OmKeyInfo omKeyInfo = null;
    OMClientResponse omClientResponse = null;

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      List<OmKeyLocationInfo> locationInfoList = commitKeyArgs
          .getKeyLocationsList().stream()
          .map(OmKeyLocationInfo::getFromProtobuf)
          .collect(Collectors.toList());

      String dbOzoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      String dbOpenKey = omMetadataManager.getOpenKey(volumeName, bucketName,
          keyName, commitKeyRequest.getClientID());

      omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
          bucketName);

      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      omKeyInfo = omMetadataManager.getOpenKeyTable().get(dbOpenKey);
      if (omKeyInfo == null) {
        throw new OMException("Failed to commit key, as " + dbOpenKey +
            "entry is not found in the openKey table", KEY_NOT_FOUND);
      }
      omKeyInfo.setDataSize(commitKeyArgs.getDataSize());

      omKeyInfo.setModificationTime(commitKeyArgs.getModificationTime());

      //update the block length for each block
      omKeyInfo.updateLocationInfoList(locationInfoList);

      // Add to cache of open key table and key table.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(dbOpenKey),
          new CacheValue<>(Optional.absent(), transactionLogIndex));

      omMetadataManager.getKeyTable().addCacheEntry(
          new CacheKey<>(dbOzoneKey),
          new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));

      omResponse.setCommitKeyResponse(CommitKeyResponse.newBuilder().build());
      omClientResponse =
          new OMKeyCommitResponse(omKeyInfo, commitKeyRequest.getClientID(),
              omResponse.build());
    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMKeyCommitResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    } finally {
      if (omClientResponse != null) {
        omClientResponse.setFlushFuture(
            ozoneManagerDoubleBufferHelper.add(omClientResponse,
                transactionLogIndex));
      }
      omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
    }

    // Performing audit logging outside of the lock.
    auditLog(auditLogger, buildAuditMessage(OMAction.COMMIT_KEY, auditMap,
        exception, getOmRequest().getUserInfo()));

    // return response after releasing lock.
    if (exception == null) {
      omResponse.setCommitKeyResponse(CommitKeyResponse.newBuilder().build());

      // As when we commit the key, then it is visible in ozone, so we should
      // increment here.
      // As key also can have multiple versions, we need to increment keys
      // only if version is 0. Currently we have not complete support of
      // versioning of keys. So, this can be revisited later.

      if (omKeyInfo.getKeyLocationVersions().size() == 1) {
        omMetrics.incNumKeys();
      }
      return omClientResponse;
    } else {
      LOG.error("CommitKey failed for Key: {} in volume/bucket:{}/{}",
          keyName, bucketName, volumeName, exception);
      omMetrics.incNumKeyCommitFails();
      return omClientResponse;
    }

  }
}
