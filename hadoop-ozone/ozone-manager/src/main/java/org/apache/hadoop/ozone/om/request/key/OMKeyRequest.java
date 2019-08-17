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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
    .EncryptedKeyVersion;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmClient;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.utils.db.cache.CacheKey;
import org.apache.hadoop.utils.db.cache.CacheValue;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes
    .VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateFile;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.CreateKey;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Interface for key write requests.
 */
public abstract class OMKeyRequest extends OMClientRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OMKeyRequest.class);

  public OMKeyRequest(OMRequest omRequest) {
    super(omRequest);
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  protected List< OmKeyLocationInfo > allocateBlock(ScmClient scmClient,
      OzoneBlockTokenSecretManager secretManager,
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor replicationFactor,
      ExcludeList excludeList, long requestedSize, long scmBlockSize,
      int preallocateBlocksMax, boolean grpcBlockTokenEnabled, String omID)
      throws IOException {

    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
        preallocateBlocksMax);

    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmClient.getBlockClient()
          .allocateBlock(scmBlockSize, numBlocks, replicationType,
              replicationFactor, omID, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.SAFE_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(),
            OMException.ResultCodes.SCM_IN_SAFE_MODE);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(allocatedBlock.getBlockID()))
          .setLength(scmBlockSize)
          .setOffset(0)
          .setPipeline(allocatedBlock.getPipeline());
      if (grpcBlockTokenEnabled) {
        builder.setToken(secretManager
            .generateToken(remoteUser, allocatedBlock.getBlockID().toString(),
                getAclForUser(remoteUser), scmBlockSize));
      }
      locationInfos.add(builder.build());
    }
    return locationInfos;
  }

  /* Optimize ugi lookup for RPC operations to avoid a trip through
   * UGI.getCurrentUser which is synch'ed.
   */
  private UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Return acl for user.
   * @param user
   *
   * */
  private EnumSet< HddsProtos.BlockTokenSecretProto.AccessModeProto>
      getAclForUser(String user) {
    // TODO: Return correct acl for user.
    return EnumSet.allOf(
        HddsProtos.BlockTokenSecretProto.AccessModeProto.class);
  }

  /**
   * Validate bucket and volume exists or not.
   * @param omMetadataManager
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  public void validateBucketAndVolume(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName)
      throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    // Check if bucket exists
    if (!omMetadataManager.getBucketTable().isExist(bucketKey)) {
      String volumeKey = omMetadataManager.getVolumeKey(volumeName);
      // If the volume also does not exist, we should throw volume not found
      // exception
      if (!omMetadataManager.getVolumeTable().isExist(volumeKey)) {
        throw new OMException("Volume not found " + volumeName,
            VOLUME_NOT_FOUND);
      }

      // if the volume exists but bucket does not exist, throw bucket not found
      // exception
      throw new OMException("Bucket not found " + bucketName, BUCKET_NOT_FOUND);
    }
  }

  protected Optional<FileEncryptionInfo> getFileEncryptionInfo(
      OzoneManager ozoneManager, OmBucketInfo bucketInfo) throws IOException {
    Optional<FileEncryptionInfo> encInfo = Optional.absent();
    BucketEncryptionKeyInfo ezInfo = bucketInfo.getEncryptionKeyInfo();
    if (ezInfo != null) {
      if (ozoneManager.getKmsProvider() == null) {
        throw new OMException("Invalid KMS provider, check configuration " +
            CommonConfigurationKeys.HADOOP_SECURITY_KEY_PROVIDER_PATH,
            OMException.ResultCodes.INVALID_KMS_PROVIDER);
      }

      final String ezKeyName = ezInfo.getKeyName();
      EncryptedKeyVersion edek = generateEDEK(ozoneManager, ezKeyName);
      encInfo = Optional.of(new FileEncryptionInfo(ezInfo.getSuite(),
        ezInfo.getVersion(),
          edek.getEncryptedKeyVersion().getMaterial(),
          edek.getEncryptedKeyIv(), ezKeyName,
          edek.getEncryptionKeyVersionName()));
    }
    return encInfo;
  }

  private EncryptedKeyVersion generateEDEK(OzoneManager ozoneManager,
      String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion >() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return ozoneManager.getKmsProvider()
                  .generateEncryptedKey(ezKeyName);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        });
    long generateEDEKTime = monotonicNow() - generateEDEKStartTime;
    LOG.debug("generateEDEK takes {} ms", generateEDEKTime);
    Preconditions.checkNotNull(edek);
    return edek;
  }

  /**
   * Prepare the response returned to the client.
   * @return OMClientResponse
   */
  @SuppressWarnings("parameternumber")
  protected OMClientResponse prepareCreateKeyResponse(@Nonnull KeyArgs keyArgs,
      OmKeyInfo omKeyInfo, @Nonnull List<OmKeyLocationInfo> locations,
      FileEncryptionInfo encryptionInfo, @Nullable IOException exception,
      long clientID, long transactionLogIndex, @Nonnull String volumeName,
      @Nonnull String bucketName, @Nonnull String keyName,
      @Nonnull OzoneManager ozoneManager, @Nonnull OMAction omAction) {

    OMResponse.Builder omResponse = OMResponse.newBuilder()
        .setStatus(OzoneManagerProtocolProtos.Status.OK);
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
          omResponse.setCreateFileResponse(CreateFileResponse.newBuilder()
                  .setKeyInfo(omKeyInfo.getProtobuf())
                  .setID(clientID)
                  .setOpenVersion(openVersion).build());
          omResponse.setCmdType(CreateFile);
          omClientResponse = new OMFileCreateResponse(omKeyInfo, clientID,
              omResponse.build());
        } else {
          ozoneManager.getMetrics().incNumKeyAllocates();
          omResponse.setCreateKeyResponse(CreateKeyResponse.newBuilder()
              .setKeyInfo(omKeyInfo.getProtobuf())
              .setID(clientID).setOpenVersion(openVersion)
              .build());
          omResponse.setCmdType(CreateKey);
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

  /**
   * Create OmKeyInfo object.
   * @return OmKeyInfo
   */
  protected OmKeyInfo createKeyInfo(@Nonnull KeyArgs keyArgs,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nonnull HddsProtos.ReplicationFactor factor,
      @Nonnull HddsProtos.ReplicationType type, long size,
      @Nullable FileEncryptionInfo encInfo) {
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

  /**
   * Prepare OmKeyInfo which will be persisted to openKeyTable.
   * @return OmKeyInfo
   * @throws IOException
   */
  protected OmKeyInfo prepareKeyInfo(
      @Nonnull OMMetadataManager omMetadataManager,
      @Nonnull KeyArgs keyArgs, @Nonnull String dbKeyName, long size,
      @Nonnull List<OmKeyLocationInfo> locations,
      @Nullable FileEncryptionInfo encInfo)
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

}
