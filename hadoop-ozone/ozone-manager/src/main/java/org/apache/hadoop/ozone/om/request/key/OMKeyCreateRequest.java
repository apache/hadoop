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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
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
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateFile;
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

    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();
    OmKeyInfo omKeyInfo = null;
    final List< OmKeyLocationInfo > locations = new ArrayList<>();
    FileEncryptionInfo encryptionInfo = null;
    IOException exception = null;
    boolean acquireLock = false;
    try {
      // check Acl
      if (ozoneManager.getAclsEnabled()) {
        checkAcls(ozoneManager, OzoneObj.ResourceType.KEY,
            OzoneObj.StoreType.OZONE, IAccessAuthorizer.ACLType.WRITE,
            volumeName, bucketName, keyName);
      }

      acquireLock = omMetadataManager.getLock().acquireLock(BUCKET_LOCK,
          volumeName, bucketName);
      validateBucketAndVolume(omMetadataManager, volumeName, bucketName);
      //TODO: We can optimize this get here, if getKmsProvider is null, then
      // bucket encryptionInfo will be not set. If this assumption holds
      // true, we can avoid get from bucket table.

      OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
              omMetadataManager.getBucketKey(volumeName, bucketName));

      encryptionInfo = getFileEncryptionInfo(ozoneManager, bucketInfo);

      omKeyInfo = prepareKeyInfo(omMetadataManager, keyArgs,
          omMetadataManager.getOzoneKey(volumeName, bucketName, keyName),
          keyArgs.getDataSize(), locations, encryptionInfo);

    } catch (IOException ex) {
      LOG.error("Key open failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      exception = ex;
    } finally {
      if (acquireLock) {
        omMetadataManager.getLock().releaseLock(BUCKET_LOCK, volumeName,
            bucketName);
      }
    }

    return prepareCreateKeyResponse(keyArgs, omKeyInfo, locations,
        encryptionInfo, exception, createKeyRequest.getClientID(),
        transactionLogIndex, volumeName, bucketName, keyName, ozoneManager,
        OMAction.ALLOCATE_KEY);
  }

  /**
   * Prepare the response returned to the client.
   * @param keyArgs
   * @param omKeyInfo
   * @param locations
   * @param encryptionInfo
   * @param exception
   * @param clientID
   * @param transactionLogIndex
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param ozoneManager
   * @return OMClientResponse
   */
  @SuppressWarnings("parameternumber")
  protected OMClientResponse prepareCreateKeyResponse(@Nonnull KeyArgs keyArgs,
      OmKeyInfo omKeyInfo, @Nonnull List<OmKeyLocationInfo> locations,
      FileEncryptionInfo encryptionInfo, @Nullable IOException exception,
      long clientID, long transactionLogIndex, @Nonnull String volumeName,
      @Nonnull String bucketName, @Nonnull String keyName,
      @Nonnull OzoneManager ozoneManager, @Nonnull OMAction omAction) {

    OMResponse.Builder omResponse = OMResponse.newBuilder().setStatus(
        OzoneManagerProtocolProtos.Status.OK);
    OMMetadataManager omMetadataManager = ozoneManager.getMetadataManager();

    Map<String, String> auditMap = buildKeyArgsAuditMap(keyArgs);

    OMClientResponse omClientResponse = null;
    if (exception == null) {
      if (omKeyInfo == null) {
        // the key does not exist, create a new object, the new blocks are the
        // version 0
        omKeyInfo = createKeyInfo(keyArgs, locations, keyArgs.getFactor(),
            keyArgs.getType(), keyArgs.getDataSize(),
            encryptionInfo);
      }

      long openVersion = omKeyInfo.getLatestVersionLocations().getVersion();

      // Append blocks
      try {
        omKeyInfo.appendNewBlocks(keyArgs.getKeyLocationsList().stream()
            .map(OmKeyLocationInfo::getFromProtobuf)
            .collect(Collectors.toList()), false);

      } catch (IOException ex) {
        exception = ex;
      }

      if (exception != null) {
        LOG.error("{} failed for Key: {} in volume/bucket:{}/{}",
            omAction.getAction(), keyName, bucketName, volumeName, exception);
        omClientResponse = createKeyErrorResponse(ozoneManager.getMetrics(),
            omAction, exception, omResponse);
      } else {
        String dbOpenKeyName = omMetadataManager.getOpenKey(volumeName,
            bucketName, keyName, clientID);

        // Add to cache entry can be done outside of lock for this openKey.
        // Even if bucket gets deleted, when commitKey we shall identify if
        // bucket gets deleted.
        omMetadataManager.getOpenKeyTable().addCacheEntry(
            new CacheKey<>(dbOpenKeyName),
            new CacheValue<>(Optional.of(omKeyInfo), transactionLogIndex));

        LOG.debug("{} for Key: {} in volume/bucket: {}/{}",
            omAction.getAction(), keyName, volumeName, bucketName);


        if (omAction == OMAction.CREATE_FILE) {
          ozoneManager.getMetrics().incNumCreateFile();
          omResponse.setCreateFileResponse(
              OzoneManagerProtocolProtos.CreateFileResponse.newBuilder()
                  .setKeyInfo(omKeyInfo.getProtobuf())
                  .setID(clientID)
                  .setOpenVersion(openVersion).build());
          omResponse.setCmdType(OzoneManagerProtocolProtos.Type.CreateFile);
          omClientResponse = new OMFileCreateResponse(omKeyInfo, clientID,
              omResponse.build());
        } else {
          ozoneManager.getMetrics().incNumKeyAllocates();
          omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
              .setKeyInfo(omKeyInfo.getProtobuf())
              .setID(clientID).setOpenVersion(openVersion)
              .build());
          omResponse.setCmdType(OzoneManagerProtocolProtos.Type.CreateKey);
          omClientResponse = new OMKeyCreateResponse(omKeyInfo, clientID,
            omResponse.build());
        }
      }

    } else {
      LOG.error("{} failed for Key: {} in volume/bucket:{}/{}",
          omAction.getAction(), keyName, volumeName, bucketName, exception);
      omClientResponse = createKeyErrorResponse(ozoneManager.getMetrics(),
          omAction, exception, omResponse);
    }
    // audit log
    auditLog(ozoneManager.getAuditLogger(), buildAuditMessage(omAction,
        auditMap, exception, getOmRequest().getUserInfo()));
    return omClientResponse;
  }

  private OMClientResponse createKeyErrorResponse(@Nonnull OMMetrics omMetrics,
      @Nonnull OMAction omAction, @Nonnull IOException exception,
      @Nonnull OMResponse.Builder omResponse) {
    if (omAction == OMAction.CREATE_FILE) {
      omMetrics.incNumCreateFileFails();
      omResponse.setCmdType(CreateFile);
      return new OMFileCreateResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    } else {
      omMetrics.incNumKeyAllocateFails();
      omResponse.setCmdType(CreateKey);
      return new OMKeyCreateResponse(null, -1L,
          createErrorOMResponse(omResponse, exception));
    }
  }

  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @param omMetadataManager
   * @param keyArgs
   * @param dbKeyName
   * @param size
   * @param locations
   * @param encInfo
   * @return OmKeyInfo
   * @throws IOException
   */
  protected OmKeyInfo prepareKeyInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull KeyArgs keyArgs, @Nonnull String dbKeyName, long size,
      @Nonnull List<OmKeyLocationInfo> locations, FileEncryptionInfo encInfo)
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

  /**
   * Prepare OmKeyInfo for multi-part upload part key which will be persisted
   * to openKeyTable.
   * @param omMetadataManager
   * @param args
   * @param size
   * @param locations
   * @param encInfo
   * @return OmKeyInfo
   * @throws IOException
   */
  private OmKeyInfo prepareMultipartKeyInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull KeyArgs args, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
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
   * @return OmKeyInfo
   */
  private OmKeyInfo createKeyInfo(@Nonnull KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull HddsProtos.ReplicationFactor factor,
      @Nonnull HddsProtos.ReplicationType type, long size,
      FileEncryptionInfo encInfo) {
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
