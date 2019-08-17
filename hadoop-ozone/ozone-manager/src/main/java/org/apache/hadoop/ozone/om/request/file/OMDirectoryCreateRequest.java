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

package org.apache.hadoop.ozone.om.request.file;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.key.OMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMDirectoryCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateDirectoryResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;


import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_ALREADY_EXISTS;
import static  org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.DIRECTORY_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS_IN_GIVENPATH;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.NONE;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.OMDirectoryResult.FILE_EXISTS;
/**
 * Handle create directory request.
 */
public class OMDirectoryCreateRequest extends OMKeyRequest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMDirectoryCreateRequest.class);

  public OMDirectoryCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) {
    CreateDirectoryRequest createDirectoryRequest =
        getOmRequest().getCreateDirectoryRequest();
    Preconditions.checkNotNull(createDirectoryRequest);

    KeyArgs.Builder newKeyArgs = createDirectoryRequest.getKeyArgs()
        .toBuilder().setModificationTime(Time.now());

    CreateDirectoryRequest.Builder newCreateDirectoryRequest =
        createDirectoryRequest.toBuilder().setKeyArgs(newKeyArgs);

    return getOmRequest().toBuilder().setCreateDirectoryRequest(
        newCreateDirectoryRequest).build();

  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper) {

    KeyArgs keyArgs = getOmRequest().getCreateDirectoryRequest().getKeyArgs();

    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMResponse.Builder omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.CreateDirectory).setStatus(
            OzoneManagerProtocolProtos.Status.OK);

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumCreateDirectory();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();
    OzoneManagerProtocolProtos.UserInfo userInfo = getOmRequest().getUserInfo();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    boolean acquiredLock = false;
    IOException exception = null;
    OMClientResponse omClientResponse = null;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.BUCKET,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      // Check if this is the root of the filesystem.
      if (keyName.length() == 0) {
        return new OMDirectoryCreateResponse(null,
            omResponse.setCreateDirectoryResponse(
                CreateDirectoryResponse.newBuilder()).build());
      }
      // acquire lock
      acquiredLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);

      // TODO: Not checking volume exist here, once we have full cache we can
      //  add volume exist check also.

      OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(volumeName, bucketName));

      if (omBucketInfo == null) {
        throw new OMException("Bucket not found " + bucketName,
            BUCKET_NOT_FOUND);
      }

      // Need to check if any files exist in the given path, if they exist we
      // cannot create a directory with the given key.
      OMFileRequest.OMDirectoryResult omDirectoryResult =
          OMFileRequest.verifyFilesInPath(omMetadataManager,
          volumeName, bucketName, keyName, Paths.get(keyName));

      OmKeyInfo dirKeyInfo = null;
      if (omDirectoryResult == FILE_EXISTS ||
          omDirectoryResult == FILE_EXISTS_IN_GIVENPATH) {
        throw new OMException("Unable to create directory: " +keyName
            + " in volume/bucket: " + volumeName + "/" + bucketName,
            FILE_ALREADY_EXISTS);
      } else if (omDirectoryResult == DIRECTORY_EXISTS_IN_GIVENPATH ||
          omDirectoryResult == NONE) {
        dirKeyInfo = createDirectoryKeyInfo(ozoneManager, omBucketInfo,
            volumeName, bucketName, keyName, keyArgs);

        omMetadataManager.getKeyTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getOzoneKey(volumeName, bucketName,
                dirKeyInfo.getKeyName())),
            new CacheValue<>(Optional.of(dirKeyInfo), transactionLogIndex));
      }
      // if directory already exists do nothing or do we need to throw
      // exception? Current KeyManagerImpl code does just return, following
      // similar approach.

      omResponse.setCreateDirectoryResponse(
          CreateDirectoryResponse.newBuilder());
      omClientResponse = new OMDirectoryCreateResponse(dirKeyInfo,
          omResponse.build());

    } catch (IOException ex) {
      exception = ex;
      omClientResponse = new OMDirectoryCreateResponse(null,
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

    auditLog(auditLogger, buildAuditMessage(OMAction.CREATE_DIRECTORY,
        auditMap, exception, userInfo));

    if (exception == null) {
      LOG.debug("Directory is successfully created for Key: {} in " +
              "volume/bucket:{}/{}", keyName, volumeName, bucketName);
      return omClientResponse;
    } else {
      LOG.error("CreateDirectory failed for Key: {} in volume/bucket:{}/{}",
          keyName, volumeName, bucketName, exception);
      omMetrics.incNumCreateDirectoryFails();
      return omClientResponse;
    }
  }

  private OmKeyInfo createDirectoryKeyInfo(OzoneManager ozoneManager,
      OmBucketInfo omBucketInfo, String volumeName, String bucketName,
      String keyName, KeyArgs keyArgs)
      throws IOException {
    Optional<FileEncryptionInfo> encryptionInfo =
        getFileEncryptionInfo(ozoneManager, omBucketInfo);
    String dirName = OzoneFSUtils.addTrailingSlashIfNeeded(keyName);

    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(dirName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, new ArrayList<>())))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(0)
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(HddsProtos.ReplicationFactor.ONE)
        .setFileEncryptionInfo(encryptionInfo.orNull())
        .setAcls(keyArgs.getAclsList())
        .build();
  }

}
