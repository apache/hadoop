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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.UniqueId;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;

/**
 * Handles CreateKey request.
 */

public class OMKeyCreateRequest extends OMClientRequest
    implements OMKeyRequest {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMKeyCreateRequest.class);

  public OMKeyCreateRequest(OMRequest omRequest) {
    super(omRequest);
  }

  @Override
  public OMRequest preExecute(OzoneManager ozoneManager) throws IOException {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();
    Preconditions.checkNotNull(createKeyRequest);

    KeyArgs keyArgs = createKeyRequest.getKeyArgs();

    // We cannot allocate block for multipart upload part when
    // createMultipartKey is called, as we will not know type and factor with
    // which initiateMultipartUpload has started for this key. When
    // allocateBlock call happen's we shall know type and factor, as we set
    // the type and factor read from multipart table, and set the KeyInfo in
    // validateAndUpdateCache and return to the client. TODO: See if we can fix
    //  this. We do not call allocateBlock in openKey for multipart upload.

    CreateKeyRequest.Builder newCreateKeyRequest = null;
    KeyArgs.Builder newKeyArgs = null;
    if (!keyArgs.getIsMultipartKey()) {

      long scmBlockSize = ozoneManager.getScmBlockSize();

      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets
      // a value, then this value is used, otherwise, we allocate a single
      // block which is the current size, if read by the client.
      final long requestedSize = keyArgs.getDataSize() > 0 ?
          keyArgs.getDataSize() : scmBlockSize;

      boolean useRatis = ozoneManager.shouldUseRatis();

      HddsProtos.ReplicationFactor factor = keyArgs.getFactor();
      if (factor == null) {
        factor = useRatis ? HddsProtos.ReplicationFactor.THREE :
            HddsProtos.ReplicationFactor.ONE;
      }

      HddsProtos.ReplicationType type = keyArgs.getType();
      if (type == null) {
        type = useRatis ? HddsProtos.ReplicationType.RATIS :
            HddsProtos.ReplicationType.STAND_ALONE;
      }

      // TODO: Here we are allocating block with out any check for
      //  bucket/key/volume or not and also with out any authorization checks.
      //  As for a client for the first time this can be executed on any OM,
      //  till leader is identified.

      List< OmKeyLocationInfo > omKeyLocationInfoList =
          allocateBlock(ozoneManager.getScmClient(),
              ozoneManager.getBlockTokenSecretManager(), type, factor,
              new ExcludeList(), requestedSize, scmBlockSize,
              ozoneManager.getPreallocateBlocksMax(),
              ozoneManager.isGrpcBlockTokenEnabled(),
              ozoneManager.getOMNodeId());

      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now())
              .setType(type).setFactor(factor)
              .setDataSize(requestedSize);

      newKeyArgs.addAllKeyLocations(omKeyLocationInfoList.stream()
          .map(OmKeyLocationInfo::getProtobuf).collect(Collectors.toList()));
    } else {
      newKeyArgs = keyArgs.toBuilder().setModificationTime(Time.now());
    }

    newCreateKeyRequest =
        createKeyRequest.toBuilder().setKeyArgs(newKeyArgs)
            .setClientID(UniqueId.next());

    return getOmRequest().toBuilder()
        .setCreateKeyRequest(newCreateKeyRequest).setUserInfo(getUserInfo())
        .build();
  }

  @Override
  public OMClientResponse validateAndUpdateCache(OzoneManager ozoneManager,
      long transactionLogIndex) {
    CreateKeyRequest createKeyRequest = getOmRequest().getCreateKeyRequest();


    KeyArgs keyArgs = createKeyRequest.getKeyArgs();


    String volumeName = keyArgs.getVolumeName();
    String bucketName = keyArgs.getBucketName();
    String keyName = keyArgs.getKeyName();

    OMMetrics omMetrics = ozoneManager.getMetrics();
    omMetrics.incNumKeyAllocates();

    AuditLogger auditLogger = ozoneManager.getAuditLogger();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    OMResponse.Builder omResponse = OMResponse.newBuilder().setCmdType(
            OzoneManagerProtocolProtos.Type.CreateKey).setStatus(
            OzoneManagerProtocolProtos.Status.OK).setSuccess(true);

    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }
    } catch (IOException ex) {
      LOG.error("Open failed for Key: {} in volume/bucket:{}/{}",
          keyName, bucketName, volumeName, ex);
      omMetrics.incNumKeyAllocateFails();
      auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_KEY, auditMap,
          ex, getOmRequest().getUserInfo()));
      return new OMKeyCreateResponse(null, -1L,
          createErrorOMResponse(omResponse, ex));
    }

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
        bucketName, keyName, createKeyRequest.getClientID());
    String dbKeyName = omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
    String dbBucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);

    OmKeyInfo omKeyInfo = null;
    final List< OmKeyLocationInfo > locations = new ArrayList<>();
    FileEncryptionInfo encryptionInfo = null;
    long openVersion = 0L;
    IOException exception = null;
    omMetadataManager.getLock().acquireLock(BUCKET_LOCK, volumeName,
        bucketName);
    try {
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // bucket encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from bucket table.
      OmBucketInfo bucketInfo =
          omMetadataManager.getBucketTable().get(dbBucketKey);
      encryptionInfo = getFileEncryptionInfo(ozoneManager, bucketInfo);
      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs, dbKeyName,
          keyArgs.getDataSize(), locations, encryptionInfo);
    } catch (IOException ex) {
      LOG.error("Key open failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      exception = ex;
    } finally {
      omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
          bucketName);
    }


    if (exception == null) {
      if (omKeyInfo == null) {
        // the key does not exist, create a new object, the new blocks are the
        // version 0
        omKeyInfo = createKeyInfo(keyArgs, locations, keyArgs.getFactor(),
            keyArgs.getType(), keyArgs.getDataSize(),
            encryptionInfo);
      }

      openVersion = omKeyInfo.getLatestVersionLocations().getVersion();

      try {
        omKeyInfo.appendNewBlocks(keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()), false);

      } catch (IOException ex) {
        LOG.error("Open failed for Key: {} in volume/bucket:{}/{}",
            keyName, bucketName, volumeName, ex);
        omMetrics.incNumKeyAllocateFails();
        auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_KEY, auditMap,
            ex, getOmRequest().getUserInfo()));
        return new OMKeyCreateResponse(null, -1L,
            createErrorOMResponse(omResponse, ex));
      }

      // Add to cache entry can be done outside of lock for this openKey.
      // Even if bucket gets deleted, when commitKey we shall identify if
      // bucket gets deleted.
      omMetadataManager.getOpenKeyTable().addCacheEntry(
          new CacheKey<>(dbOpenKeyName),
          new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));

      LOG.debug("Key {} allocated in volume/bucket: {}/{}", keyName, volumeName,
          bucketName);

      auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_KEY, auditMap,
          exception, getOmRequest().getUserInfo()));

      long clientID = createKeyRequest.getClientID();

      omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
          .setKeyInfo(omKeyInfo.getProtobuf())
          .setID(clientID).setOpenVersion(openVersion)
          .build());

      return new OMKeyCreateResponse(omKeyInfo, clientID, omResponse.build());

    } else {
      auditLog(auditLogger, buildAuditMessage(OMAction.ALLOCATE_KEY, auditMap,
          exception, getOmRequest().getUserInfo()));
      LOG.error("Open failed for Key: {} in volume/bucket:{}/{}",
          keyName, bucketName, volumeName, exception);
      omMetrics.incNumKeyAllocateFails();
      return new OMKeyCreateResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    }
  }

  private OmKeyInfo prepareKeyInfo(OMMetadataManager omMetadataManager,
      KeyArgs keyArgs, String dbKeyName, long size,
      List<OmKeyLocationInfo> locations, FileEncryptionInfo encInfo)
      throws IOException {
    OmKeyInfo keyInfo = null;
    if (keyArgs.getIsMultipartKey()) {
      keyInfo = prepareMultipartKeyInfo(omMetadataManager, keyArgs, size,
          locations, encInfo);
      //TODO args.getMetadata
    } else if (omMetadataManager.getKeyTable().isExist(dbKeyName)) {
      // TODO: Need to be fixed, as when key already exists, we are
      //  appending new blocks to existing key.
      keyInfo = omMetadataManager.getKeyTable().get(dbKeyName);
      // the key already exist, the new blocks will be added as new version
      // when locations.size = 0, the new version will have identical blocks
      // as its previous version
      keyInfo.addNewVersion(locations, false);
      keyInfo.setDataSize(size + keyInfo.getDataSize());
      // The modification time is set in preExecute, use the same as
      // modification time when key already exists.
      keyInfo.setModificationTime(keyArgs.getModificationTime());
    }
    return keyInfo;
  }

  private OmKeyInfo prepareMultipartKeyInfo(OMMetadataManager omMetadataManager,
      KeyArgs args, long size, List<OmKeyLocationInfo> locations,
      FileEncryptionInfo encInfo) throws IOException {
    HddsProtos.ReplicationFactor factor;
    HddsProtos.ReplicationType type;

    Preconditions.checkArgument(args.getMultipartNumber() > 0,
        "PartNumber Should be greater than zero");
    // When key is multipart upload part key, we should take replication
    // type and replication factor from original key which has done
    // initiate multipart upload. If we have not found any such, we throw
    // error no such multipart upload.
    String uploadID = args.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID);
    String multipartKey = omMetadataManager
        .getMultipartKey(args.getVolumeName(), args.getBucketName(),
            args.getKeyName(), uploadID);
    OmKeyInfo partKeyInfo = omMetadataManager.getOpenKeyTable().get(
        multipartKey);
    if (partKeyInfo == null) {
      throw new OMException("No such Multipart upload is with specified " +
          "uploadId " + uploadID,
          OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
    } else {
      factor = partKeyInfo.getFactor();
      type = partKeyInfo.getType();
    }
    // For this upload part we don't need to check in KeyTable. As this
    // is not an actual key, it is a part of the key.
    return createKeyInfo(args, locations, factor, type, size, encInfo);
  }

  /**
   * Create OmKeyInfo object.
   * @param keyArgs
   * @param locations
   * @param factor
   * @param type
   * @param size
   * @param encInfo
   * @return
   */
  private OmKeyInfo createKeyInfo(KeyArgs keyArgs,
      List<OmKeyLocationInfo> locations, HddsProtos.ReplicationFactor factor,
      HddsProtos.ReplicationType type, long size, FileEncryptionInfo encInfo) {
    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locations)))
        .setCreationTime(keyArgs.getModificationTime())
        .setModificationTime(keyArgs.getModificationTime())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setFileEncryptionInfo(encInfo);
    if(keyArgs.getAclsList() != null) {
      builder.setAcls(keyArgs.getAclsList());
    }
    return builder.build();
  }
}
