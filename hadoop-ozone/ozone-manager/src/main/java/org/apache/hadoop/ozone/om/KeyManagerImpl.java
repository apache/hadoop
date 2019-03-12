/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.ozone.om.helpers.BucketEncryptionKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmPartInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.db.BatchOperation;
import org.apache.hadoop.utils.db.DBStore;

import com.google.common.base.Preconditions;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_MULTIPART_MIN_SIZE;
import static org.apache.hadoop.util.Time.monotonicNow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of keyManager.
 */
public class KeyManagerImpl implements KeyManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyManagerImpl.class);

  /**
   * A SCM block client, used to talk to SCM to allocate block during putKey.
   */
  private final ScmBlockLocationProtocol scmBlockClient;
  private final OMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final boolean useRatis;

  private final int preallocateBlocksMax;
  private final String omId;
  private final OzoneBlockTokenSecretManager secretManager;
  private final boolean grpcBlockTokenEnabled;

  private BackgroundService keyDeletingService;

  private final KeyProviderCryptoExtension kmsProvider;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager) {
    this(scmBlockClient, metadataManager, conf, omId, secretManager, null);
  }

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager, OzoneConfiguration conf, String omId,
      OzoneBlockTokenSecretManager secretManager,
      KeyProviderCryptoExtension kmsProvider) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
    this.scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.preallocateBlocksMax = conf.getInt(
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX,
        OZONE_KEY_PREALLOCATION_BLOCKS_MAX_DEFAULT);
    this.omId = omId;
    start(conf);
    this.secretManager = secretManager;
    this.grpcBlockTokenEnabled = conf.getBoolean(
        HDDS_BLOCK_TOKEN_ENABLED,
        HDDS_BLOCK_TOKEN_ENABLED_DEFAULT);
    this.kmsProvider = kmsProvider;
  }

  @Override
  public void start(OzoneConfiguration configuration) {
    if (keyDeletingService == null) {
      long blockDeleteInterval = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
          OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS);
      long serviceTimeout = configuration.getTimeDuration(
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
          OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
          TimeUnit.MILLISECONDS);
      keyDeletingService = new KeyDeletingService(scmBlockClient, this,
          blockDeleteInterval, serviceTimeout, configuration);
      keyDeletingService.start();
    }
  }

  KeyProviderCryptoExtension getKMSProvider() {
    return kmsProvider;
  }

  @Override
  public void stop() throws IOException {
    if (keyDeletingService != null) {
      keyDeletingService.shutdown();
      keyDeletingService = null;
    }
  }

  private OmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    return metadataManager.getBucketTable().get(bucketKey);
  }

  private void validateBucket(String volumeName, String bucketName)
      throws IOException {
    String volumeKey = metadataManager.getVolumeKey(volumeName);
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

    //Check if the volume exists
    if (metadataManager.getVolumeTable().get(volumeKey) == null) {
      LOG.error("volume not found: {}", volumeName);
      throw new OMException("Volume not found",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }
    //Check if bucket already exists
    if (metadataManager.getBucketTable().get(bucketKey) == null) {
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  /**
   * Check S3 bucket exists or not.
   * @param volumeName
   * @param bucketName
   * @throws IOException
   */
  private void validateS3Bucket(String volumeName, String bucketName)
      throws IOException {

    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
    //Check if bucket already exists
    if (metadataManager.getBucketTable().get(bucketKey) == null) {
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID,
      ExcludeList excludeList)
      throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    validateBucket(volumeName, bucketName);
    String openKey = metadataManager.getOpenKey(
        volumeName, bucketName, keyName, clientID);

    OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(openKey);
    if (keyInfo == null) {
      LOG.error("Allocate block for a key not in open status in meta store" +
          " /{}/{}/{} with ID {}", volumeName, bucketName, keyName, clientID);
      throw new OMException("Open Key not found",
          OMException.ResultCodes.KEY_NOT_FOUND);
    }

    // current version not committed, so new blocks coming now are added to
    // the same version
    List<OmKeyLocationInfo> locationInfos =
        allocateBlock(keyInfo, excludeList, scmBlockSize);
    keyInfo.appendNewBlocks(locationInfos);
    keyInfo.updateModifcationTime();
    metadataManager.getOpenKeyTable().put(openKey,
        keyInfo);
    return locationInfos.get(0);
  }

  /**
   * This methods avoids multiple rpc calls to SCM by allocating multiple blocks
   * in one rpc call.
   * @param keyInfo - key info for key to be allocated.
   * @param requestedSize requested length for allocation.
   * @param excludeList exclude list while allocating blocks.
   * @param requestedSize requested size to be allocated.
   * @return
   * @throws IOException
   */
  private List<OmKeyLocationInfo> allocateBlock(OmKeyInfo keyInfo,
      ExcludeList excludeList, long requestedSize) throws IOException {
    int numBlocks = Math.min((int) ((requestedSize - 1) / scmBlockSize + 1),
        preallocateBlocksMax);
    List<OmKeyLocationInfo> locationInfos = new ArrayList<>(numBlocks);
    String remoteUser = getRemoteUser().getShortUserName();
    List<AllocatedBlock> allocatedBlocks;
    try {
      allocatedBlocks = scmBlockClient
          .allocateBlock(scmBlockSize, numBlocks, keyInfo.getType(),
              keyInfo.getFactor(), omId, excludeList);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.CHILL_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(), ResultCodes.SCM_IN_CHILL_MODE);
      }
      throw ex;
    }
    for (AllocatedBlock allocatedBlock : allocatedBlocks) {
      OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(allocatedBlock.getBlockID()))
          .setLength(scmBlockSize)
          .setOffset(0);
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
  public static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  /**
   * Return acl for user.
   * @param user
   *
   * */
  private EnumSet<AccessModeProto> getAclForUser(String user) {
    // TODO: Return correct acl for user.
    return EnumSet.allOf(AccessModeProto.class);
  }

  private EncryptedKeyVersion generateEDEK(
      final String ezKeyName) throws IOException {
    if (ezKeyName == null) {
      return null;
    }
    long generateEDEKStartTime = monotonicNow();
    EncryptedKeyVersion edek = SecurityUtil.doAsLoginUser(
        new PrivilegedExceptionAction<EncryptedKeyVersion>() {
          @Override
          public EncryptedKeyVersion run() throws IOException {
            try {
              return getKMSProvider().generateEncryptedKey(ezKeyName);
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

  @SuppressWarnings("checkstyle:methodlength")
  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    validateBucket(volumeName, bucketName);

    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    String keyName = args.getKeyName();
    ReplicationFactor factor = args.getFactor();
    ReplicationType type = args.getType();
    long currentTime = Time.monotonicNowNanos();
    OmKeyInfo keyInfo;
    String openKey;
    long openVersion;

    FileEncryptionInfo encInfo = null;
    OmBucketInfo bucketInfo = getBucketInfo(volumeName, bucketName);
    BucketEncryptionKeyInfo ezInfo = bucketInfo.getEncryptionKeyInfo();
    if (ezInfo != null) {
      if (getKMSProvider() == null) {
        throw new OMException("Invalid KMS provider, check configuration " +
            CommonConfigurationKeys.HADOOP_SECURITY_KEY_PROVIDER_PATH,
            OMException.ResultCodes.INVALID_KMS_PROVIDER);
      }

      final String ezKeyName = ezInfo.getKeyName();
      EncryptedKeyVersion edek = generateEDEK(ezKeyName);
      encInfo = new FileEncryptionInfo(ezInfo.getSuite(), ezInfo.getVersion(),
            edek.getEncryptedKeyVersion().getMaterial(),
            edek.getEncryptedKeyIv(),
            ezKeyName, edek.getEncryptionKeyVersionName());
    }

    try {
      if (args.getIsMultipartKey()) {
        Preconditions.checkArgument(args.getMultipartUploadPartNumber() > 0,
            "PartNumber Should be greater than zero");
        // When key is multipart upload part key, we should take replication
        // type and replication factor from original key which has done
        // initiate multipart upload. If we have not found any such, we throw
        // error no such multipart upload.
        String uploadID = args.getMultipartUploadID();
        Preconditions.checkNotNull(uploadID);
        String multipartKey = metadataManager.getMultipartKey(volumeName,
            bucketName, keyName, uploadID);
        OmKeyInfo partKeyInfo = metadataManager.getOpenKeyTable().get(
            multipartKey);
        if (partKeyInfo == null) {
          throw new OMException("No such Multipart upload is with specified " +
              "uploadId " + uploadID,
              ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
        } else {
          factor = partKeyInfo.getFactor();
          type = partKeyInfo.getType();
        }
      } else {
        // If user does not specify a replication strategy or
        // replication factor, OM will use defaults.
        if (factor == null) {
          factor = useRatis ? ReplicationFactor.THREE : ReplicationFactor.ONE;
        }
        if (type == null) {
          type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
        }
      }
      List<OmKeyLocationInfo> locations = new ArrayList<>();
      String objectKey = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets a
      // value, then this value is used, otherwise, we allocate a single block
      // which is the current size, if read by the client.
      long size = args.getDataSize() >= 0 ? args.getDataSize() : scmBlockSize;

      if (args.getIsMultipartKey()) {
        // For this upload part we don't need to check in KeyTable. As this
        // is not an actual key, it is a part of the key.
        keyInfo = createKeyInfo(args, locations, factor, type, size, encInfo);
        //TODO args.getMetadata
        openVersion = 0;
      } else {
        keyInfo = metadataManager.getKeyTable().get(objectKey);
        if (keyInfo != null) {
          // the key already exist, the new blocks will be added as new version
          // when locations.size = 0, the new version will have identical blocks
          // as its previous version
          openVersion = keyInfo.addNewVersion(locations);
          keyInfo.setDataSize(size + keyInfo.getDataSize());
        } else {
          // the key does not exist, create a new object, the new blocks are the
          // version 0
          keyInfo = createKeyInfo(args, locations, factor, type, size, encInfo);
          openVersion = 0;
        }
      }
      openKey = metadataManager.getOpenKey(
          volumeName, bucketName, keyName, currentTime);
      if (metadataManager.getOpenKeyTable().get(openKey) != null) {
        // This should not happen. If this condition is satisfied, it means
        // that we have generated a same openKeyId (i.e. currentTime) for two
        // different client who are trying to write the same key at the same
        // time. The chance of this happening is very, very minimal.

        // Do we really need this check? Can we avoid this to gain some
        // minor performance improvement?
        LOG.warn("Cannot allocate key. The generated open key id is already" +
            "used for the same key which is currently being written.");
        throw new OMException("Cannot allocate key. Not able to get a valid" +
            "open key id.", ResultCodes.KEY_ALLOCATION_ERROR);
      }
      LOG.debug("Key {} allocated in volume {} bucket {}",
          keyName, volumeName, bucketName);
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key open failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.KEY_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }

    // requested size is not required but more like a optimization:
    // SCM looks at the requested, if it 0, no block will be allocated at
    // the point, if client needs more blocks, client can always call
    // allocateBlock. But if requested size is not 0, OM will preallocate
    // some blocks and piggyback to client, to save RPC calls.
    if (args.getDataSize() > 0) {
      List<OmKeyLocationInfo> locationInfos =
          allocateBlock(keyInfo, new ExcludeList(), args.getDataSize());
      keyInfo.appendNewBlocks(locationInfos);
    }
    metadataManager.getOpenKeyTable().put(openKey, keyInfo);
    return new OpenKeySession(currentTime, keyInfo, openVersion);
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
  private OmKeyInfo createKeyInfo(OmKeyArgs keyArgs,
                                  List<OmKeyLocationInfo> locations,
                                  ReplicationFactor factor,
                                  ReplicationType type, long size,
                                  FileEncryptionInfo encInfo) {
    return new OmKeyInfo.Builder()
        .setVolumeName(keyArgs.getVolumeName())
        .setBucketName(keyArgs.getBucketName())
        .setKeyName(keyArgs.getKeyName())
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locations)))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(size)
        .setReplicationType(type)
        .setReplicationFactor(factor)
        .setFileEncryptionInfo(encInfo)
        .build();
  }

  @Override
  public void commitKey(OmKeyArgs args, long clientID) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      validateBucket(volumeName, bucketName);
      String openKey = metadataManager.getOpenKey(volumeName, bucketName,
          keyName, clientID);
      String objectKey = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(openKey);
      if (keyInfo == null) {
        throw new OMException("Commit a key without corresponding entry " +
            objectKey, ResultCodes.KEY_NOT_FOUND);
      }
      keyInfo.setDataSize(args.getDataSize());
      keyInfo.setModificationTime(Time.now());
      List<OmKeyLocationInfo> locationInfoList = args.getLocationInfoList();
      Preconditions.checkNotNull(locationInfoList);

      //update the block length for each block
      keyInfo.updateLocationInfoList(locationInfoList);
      metadataManager.getStore().move(
          openKey,
          objectKey,
          keyInfo,
          metadataManager.getOpenKeyTable(),
          metadataManager.getKeyTable());
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key commit failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.KEY_ALLOCATION_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String keyBytes = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      OmKeyInfo value = metadataManager.getKeyTable().get(keyBytes);
      if (value == null) {
        LOG.debug("volume:{} bucket:{} Key:{} not found",
            volumeName, bucketName, keyName);
        throw new OMException("Key not found",
            OMException.ResultCodes.KEY_NOT_FOUND);
      }
      if (grpcBlockTokenEnabled) {
        String remoteUser = getRemoteUser().getShortUserName();
        for (OmKeyLocationInfoGroup key : value.getKeyLocationVersions()) {
          key.getLocationList().forEach(k -> {
            k.setToken(secretManager.generateToken(remoteUser,
                k.getBlockID().getContainerBlockID().toString(),
                getAclForUser(remoteUser),
                k.getLength()));
          });
        }
      }
      return value;
    } catch (IOException ex) {
      LOG.debug("Get key failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          OMException.ResultCodes.KEY_NOT_FOUND);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    Preconditions.checkNotNull(args);
    Preconditions.checkNotNull(toKeyName);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String fromKeyName = args.getKeyName();
    if (toKeyName.length() == 0 || fromKeyName.length() == 0) {
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}",
          volumeName, bucketName, fromKeyName, toKeyName);
      throw new OMException("Key name is empty",
          ResultCodes.INVALID_KEY_NAME);
    }

    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      // fromKeyName should exist
      String fromKey = metadataManager.getOzoneKey(
          volumeName, bucketName, fromKeyName);
      OmKeyInfo fromKeyValue = metadataManager.getKeyTable().get(fromKey);
      if (fromKeyValue == null) {
        // TODO: Add support for renaming open key
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} not found.", volumeName, bucketName, fromKeyName,
            toKeyName, fromKeyName);
        throw new OMException("Key not found",
            OMException.ResultCodes.KEY_NOT_FOUND);
      }

      // A rename is a no-op if the target and source name is same.
      // TODO: Discuss if we need to throw?.
      if (fromKeyName.equals(toKeyName)) {
        return;
      }

      // toKeyName should not exist
      String toKey =
          metadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
      OmKeyInfo toKeyValue = metadataManager.getKeyTable().get(toKey);
      if (toKeyValue != null) {
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} already exists.", volumeName, bucketName,
            fromKeyName, toKeyName, toKeyName);
        throw new OMException("Key already exists",
            OMException.ResultCodes.KEY_ALREADY_EXISTS);
      }

      fromKeyValue.setKeyName(toKeyName);
      fromKeyValue.updateModifcationTime();
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        metadataManager.getKeyTable().deleteWithBatch(batch, fromKey);
        metadataManager.getKeyTable().putWithBatch(batch, toKey,
            fromKeyValue);
        store.commitBatchOperation(batch);
      }
    } catch (IOException ex) {
      if (ex instanceof OMException) {
        throw ex;
      }
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}",
          volumeName, bucketName, fromKeyName, toKeyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.KEY_RENAME_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String objectKey = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(objectKey);
      if (keyInfo == null) {
        throw new OMException("Key not found",
            OMException.ResultCodes.KEY_NOT_FOUND);
      } else {
        // directly delete key with no blocks from db. This key need not be
        // moved to deleted table.
        if (isKeyEmpty(keyInfo)) {
          metadataManager.getKeyTable().delete(objectKey);
          LOG.debug("Key {} deleted from OM DB", keyName);
          return;
        }
      }
      metadataManager.getStore().move(objectKey,
          metadataManager.getKeyTable(),
          metadataManager.getDeletedTable());
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error(String.format("Delete key failed for volume:%s "
          + "bucket:%s key:%s", volumeName, bucketName, keyName), ex);
      throw new OMException(ex.getMessage(), ex,
          ResultCodes.KEY_DELETION_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  private boolean isKeyEmpty(OmKeyInfo keyInfo) {
    for (OmKeyLocationInfoGroup keyLocationList : keyInfo
        .getKeyLocationVersions()) {
      if (keyLocationList.getLocationList().size() != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix,
      int maxKeys) throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    // We don't take a lock in this path, since we walk the
    // underlying table using an iterator. That automatically creates a
    // snapshot of the data, so we don't need these locks at a higher level
    // when we iterate.
    return metadataManager.listKeys(volumeName, bucketName,
        startKey, keyPrefix, maxKeys);
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int count)
      throws IOException {
    return  metadataManager.getPendingDeletionKeys(count);
  }

  @Override
  public List<BlockGroup> getExpiredOpenKeys() throws IOException {
    return metadataManager.getExpiredOpenKeys();

  }

  @Override
  public void deleteExpiredOpenKey(String objectKeyName) throws IOException {
    Preconditions.checkNotNull(objectKeyName);
    // TODO: Fix this in later patches.
  }

  @Override
  public OMMetadataManager getMetadataManager() {
    return metadataManager;
  }

  @Override
  public BackgroundService getDeletingService() {
    return keyDeletingService;
  }

  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs omKeyArgs) throws
      IOException {
    Preconditions.checkNotNull(omKeyArgs);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();

    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    validateS3Bucket(volumeName, bucketName);
    try {
      long time = Time.monotonicNowNanos();
      String uploadID = UUID.randomUUID().toString() + "-" + Long.toString(
          time);

      // We are adding uploadId to key, because if multiple users try to
      // perform multipart upload on the same key, each will try to upload, who
      // ever finally commit the key, we see that key in ozone. Suppose if we
      // don't add id, and use the same key /volume/bucket/key, when multiple
      // users try to upload the key, we update the parts of the key's from
      // multiple users to same key, and the key output can be a mix of the
      // parts from multiple users.

      // So on same key if multiple time multipart upload is initiated we
      // store multiple entries in the openKey Table.
      // Checked AWS S3, when we try to run multipart upload, each time a
      // new uploadId is returned.

      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      // Not checking if there is an already key for this in the keyTable, as
      // during final complete multipart upload we take care of this.


      Map<Integer, PartKeyInfo> partKeyInfoMap = new HashMap<>();
      OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo(uploadID,
          partKeyInfoMap);
      List<OmKeyLocationInfo> locations = new ArrayList<>();
      OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
          .setVolumeName(omKeyArgs.getVolumeName())
          .setBucketName(omKeyArgs.getBucketName())
          .setKeyName(omKeyArgs.getKeyName())
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setReplicationType(omKeyArgs.getType())
          .setReplicationFactor(omKeyArgs.getFactor())
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, locations)))
          .build();
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        // Create an entry in open key table and multipart info table for
        // this key.
        metadataManager.getMultipartInfoTable().putWithBatch(batch,
            multipartKey, multipartKeyInfo);
        metadataManager.getOpenKeyTable().putWithBatch(batch,
            multipartKey, omKeyInfo);
        store.commitBatchOperation(batch);
        return new OmMultipartInfo(volumeName, bucketName, keyName, uploadID);
      }
    } catch (IOException ex) {
      LOG.error("Initiate Multipart upload Failed for volume:{} bucket:{} " +
              "key:{}", volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.INITIATE_MULTIPART_UPLOAD_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientID) throws IOException {
    Preconditions.checkNotNull(omKeyArgs);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    int partNumber = omKeyArgs.getMultipartUploadPartNumber();

    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    validateS3Bucket(volumeName, bucketName);
    String partName;
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);

      String openKey = metadataManager.getOpenKey(
          volumeName, bucketName, keyName, clientID);
      OmKeyInfo keyInfo = metadataManager.getOpenKeyTable().get(
          openKey);

      // set the data size and location info list
      keyInfo.setDataSize(omKeyArgs.getDataSize());
      keyInfo.updateLocationInfoList(omKeyArgs.getLocationInfoList());

      partName = metadataManager.getOzoneKey(volumeName, bucketName, keyName)
          + clientID;
      if (multipartKeyInfo == null) {
        // This can occur when user started uploading part by the time commit
        // of that part happens, in between the user might have requested
        // abort multipart upload. If we just throw exception, then the data
        // will not be garbage collected, so move this part to delete table
        // and throw error
        // Move this part to delete table.
        metadataManager.getDeletedTable().put(partName, keyInfo);
        throw new OMException("No such Multipart upload is with specified " +
            "uploadId " + uploadID, ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        PartKeyInfo oldPartKeyInfo =
            multipartKeyInfo.getPartKeyInfo(partNumber);
        PartKeyInfo.Builder partKeyInfo = PartKeyInfo.newBuilder();
        partKeyInfo.setPartName(partName);
        partKeyInfo.setPartNumber(partNumber);
        partKeyInfo.setPartKeyInfo(keyInfo.getProtobuf());
        multipartKeyInfo.addPartKeyInfo(partNumber, partKeyInfo.build());
        if (oldPartKeyInfo == null) {
          // This is the first time part is being added.
          DBStore store = metadataManager.getStore();
          try (BatchOperation batch = store.initBatchOperation()) {
            metadataManager.getOpenKeyTable().deleteWithBatch(batch, openKey);
            metadataManager.getMultipartInfoTable().putWithBatch(batch,
                multipartKey, multipartKeyInfo);
            store.commitBatchOperation(batch);
          }
        } else {
          // If we have this part already, that means we are overriding it.
          // We need to 3 steps.
          // Add the old entry to delete table.
          // Remove the new entry from openKey table.
          // Add the new entry in to the list of part keys.
          DBStore store = metadataManager.getStore();
          try (BatchOperation batch = store.initBatchOperation()) {
            metadataManager.getDeletedTable().putWithBatch(batch,
                oldPartKeyInfo.getPartName(),
                OmKeyInfo.getFromProtobuf(oldPartKeyInfo.getPartKeyInfo()));
            metadataManager.getOpenKeyTable().deleteWithBatch(batch, openKey);
            metadataManager.getMultipartInfoTable().putWithBatch(batch,
                multipartKey, multipartKeyInfo);
            store.commitBatchOperation(batch);
          }
        }
      }
    } catch (IOException ex) {
      LOG.error("Upload part Failed: volume:{} bucket:{} " +
          "key:{} PartNumber: {}", volumeName, bucketName, keyName,
          partNumber, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.MULTIPART_UPLOAD_PARTFILE_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }

    return new OmMultipartCommitUploadPartInfo(partName);

  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadList multipartUploadList)
      throws IOException {
    Preconditions.checkNotNull(omKeyArgs);
    Preconditions.checkNotNull(multipartUploadList);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    validateS3Bucket(volumeName, bucketName);
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      String ozoneKey = metadataManager.getOzoneKey(volumeName, bucketName,
          keyName);
      OmKeyInfo keyInfo = metadataManager.getKeyTable().get(ozoneKey);

      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);
      if (multipartKeyInfo == null) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }
      TreeMap<Integer, PartKeyInfo> partKeyInfoMap = multipartKeyInfo
          .getPartKeyInfoMap();

      TreeMap<Integer, String> multipartMap = multipartUploadList
          .getMultipartMap();

      // Last key in the map should be having key value as size, as map's
      // are sorted. Last entry in both maps should have partNumber as size
      // of the map. As we have part entries 1, 2, 3, 4 and then we get
      // complete multipart upload request so the map last entry should have 4,
      // if it is having value greater or less than map size, then there is
      // some thing wrong throw error.

      Map.Entry<Integer, String> multipartMapLastEntry = multipartMap
          .lastEntry();
      Map.Entry<Integer, PartKeyInfo> partKeyInfoLastEntry = partKeyInfoMap
          .lastEntry();
      if (partKeyInfoMap.size() != multipartMap.size()) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.MISMATCH_MULTIPART_LIST);
      }

      // Last entry part Number should be the size of the map, otherwise this
      // means we have missing some parts but we got a complete request.
      if (multipartMapLastEntry.getKey() != partKeyInfoMap.size() ||
          partKeyInfoLastEntry.getKey() != partKeyInfoMap.size()) {
        throw new OMException("Complete Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.MISSING_UPLOAD_PARTS);
      }
      ReplicationType type = partKeyInfoLastEntry.getValue().getPartKeyInfo()
          .getType();
      ReplicationFactor factor = partKeyInfoLastEntry.getValue()
          .getPartKeyInfo().getFactor();
      List<OmKeyLocationInfo> locations = new ArrayList<>();
      long size = 0;
      int partsCount =1;
      int partsMapSize = partKeyInfoMap.size();
      for(Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry : partKeyInfoMap
          .entrySet()) {
        int partNumber = partKeyInfoEntry.getKey();
        PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
        // Check we have all parts to complete multipart upload and also
        // check partNames provided match with actual part names
        String providedPartName = multipartMap.get(partNumber);
        String actualPartName = partKeyInfo.getPartName();
        if (partNumber == partsCount) {
          if (!actualPartName.equals(providedPartName)) {
            throw new OMException("Complete Multipart Upload Failed: volume: " +
                volumeName + "bucket: " + bucketName + "key: " + keyName,
                ResultCodes.MISMATCH_MULTIPART_LIST);
          }
          OmKeyInfo currentPartKeyInfo = OmKeyInfo
              .getFromProtobuf(partKeyInfo.getPartKeyInfo());
          // Check if any part size is less than 5mb, last part can be less
          // than 5 mb.
          if (partsCount != partsMapSize &&
              currentPartKeyInfo.getDataSize() < OM_MULTIPART_MIN_SIZE) {
            LOG.error("MultipartUpload: " + ozoneKey + "Part number: " +
                partKeyInfo.getPartNumber() + "size " + currentPartKeyInfo
                    .getDataSize() + " is less than minimum part size " +
                OzoneConsts.OM_MULTIPART_MIN_SIZE);
            throw new OMException("Complete Multipart Upload Failed: Entity " +
                "too small: volume: " + volumeName + "bucket: " + bucketName
                + "key: " + keyName, ResultCodes.ENTITY_TOO_SMALL);
          }
          // As all part keys will have only one version.
          OmKeyLocationInfoGroup currentKeyInfoGroup = currentPartKeyInfo
              .getKeyLocationVersions().get(0);
          locations.addAll(currentKeyInfoGroup.getLocationList());
          size += currentPartKeyInfo.getDataSize();
        } else {
          throw new OMException("Complete Multipart Upload Failed: volume: " +
              volumeName + "bucket: " + bucketName + "key: " + keyName,
              ResultCodes.MISSING_UPLOAD_PARTS);
        }
        partsCount++;
      }
      if (keyInfo == null) {
        // This is a newly added key, it does not have any versions.
        OmKeyLocationInfoGroup keyLocationInfoGroup = new
            OmKeyLocationInfoGroup(0, locations);
        // A newly created key, this is the first version.
        keyInfo = new OmKeyInfo.Builder()
            .setVolumeName(omKeyArgs.getVolumeName())
            .setBucketName(omKeyArgs.getBucketName())
            .setKeyName(omKeyArgs.getKeyName())
            .setReplicationFactor(factor)
            .setReplicationType(type)
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setDataSize(size)
            .setOmKeyLocationInfos(
                Collections.singletonList(keyLocationInfoGroup))
            .build();
      } else {
        // Already a version exists, so we should add it as a new version.
        // But now as versioning is not supported, just following the commit
        // key approach.
        // When versioning support comes, then we can uncomment below code
        // keyInfo.addNewVersion(locations);
        keyInfo.updateLocationInfoList(locations);
      }
      DBStore store = metadataManager.getStore();
      try (BatchOperation batch = store.initBatchOperation()) {
        //Remove entry in multipart table and add a entry in to key table
        metadataManager.getMultipartInfoTable().deleteWithBatch(batch,
            multipartKey);
        metadataManager.getKeyTable().putWithBatch(batch,
            ozoneKey, keyInfo);
        store.commitBatchOperation(batch);
      }
      return new OmMultipartUploadCompleteInfo(omKeyArgs.getVolumeName(),
          omKeyArgs.getBucketName(), omKeyArgs.getKeyName(), DigestUtils
              .sha256Hex(keyName));
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Complete Multipart Upload Failed: volume: " + volumeName +
          "bucket: " + bucketName + "key: " + keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
          .COMPLETE_MULTIPART_UPLOAD_ERROR);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {

    Preconditions.checkNotNull(omKeyArgs);
    String volumeName = omKeyArgs.getVolumeName();
    String bucketName = omKeyArgs.getBucketName();
    String keyName = omKeyArgs.getKeyName();
    String uploadID = omKeyArgs.getMultipartUploadID();
    Preconditions.checkNotNull(uploadID, "uploadID cannot be null");
    validateS3Bucket(volumeName, bucketName);
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);

    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);
      OmMultipartKeyInfo multipartKeyInfo = metadataManager
          .getMultipartInfoTable().get(multipartKey);
      OmKeyInfo openKeyInfo = metadataManager.getOpenKeyTable().get(
          multipartKey);

      // If there is no entry in openKeyTable, then there is no multipart
      // upload initiated for this key.
      if (openKeyInfo == null) {
        LOG.error("Abort Multipart Upload Failed: volume: " + volumeName +
            "bucket: " + bucketName + "key: " + keyName + "with error no " +
            "such uploadID:" + uploadID);
        throw new OMException("Abort Multipart Upload Failed: volume: " +
            volumeName + "bucket: " + bucketName + "key: " + keyName,
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        // Move all the parts to delete table
        TreeMap<Integer, PartKeyInfo> partKeyInfoMap = multipartKeyInfo
            .getPartKeyInfoMap();
        DBStore store = metadataManager.getStore();
        try (BatchOperation batch = store.initBatchOperation()) {
          for (Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry : partKeyInfoMap
              .entrySet()) {
            PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
            OmKeyInfo currentKeyPartInfo = OmKeyInfo.getFromProtobuf(
                partKeyInfo.getPartKeyInfo());
            metadataManager.getDeletedTable().putWithBatch(batch,
                partKeyInfo.getPartName(), currentKeyPartInfo);
          }
          // Finally delete the entry from the multipart info table and open
          // key table
          metadataManager.getMultipartInfoTable().deleteWithBatch(batch,
              multipartKey);
          metadataManager.getOpenKeyTable().deleteWithBatch(batch,
              multipartKey);
          store.commitBatchOperation(batch);
        }
      }
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.error("Abort Multipart Upload Failed: volume: " + volumeName +
          "bucket: " + bucketName + "key: " + keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
          .ABORT_MULTIPART_UPLOAD_FAILED);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }

  }


  @Override
  public OmMultipartUploadListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts)  throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(keyName);
    Preconditions.checkNotNull(uploadID);
    boolean isTruncated = false;
    int nextPartNumberMarker = 0;

    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      String multipartKey = metadataManager.getMultipartKey(volumeName,
          bucketName, keyName, uploadID);

      OmMultipartKeyInfo multipartKeyInfo =
          metadataManager.getMultipartInfoTable().get(multipartKey);

      if (multipartKeyInfo == null) {
        throw new OMException("No Such Multipart upload exists for this key.",
            ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR);
      } else {
        TreeMap<Integer, PartKeyInfo> partKeyInfoMap =
            multipartKeyInfo.getPartKeyInfoMap();
        Iterator<Map.Entry<Integer, PartKeyInfo>> partKeyInfoMapIterator =
            partKeyInfoMap.entrySet().iterator();
        HddsProtos.ReplicationType replicationType =
            partKeyInfoMap.firstEntry().getValue().getPartKeyInfo().getType();
        int count = 0;
        List<OmPartInfo> omPartInfoList = new ArrayList<>();

        while (count < maxParts && partKeyInfoMapIterator.hasNext()) {
          Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry =
              partKeyInfoMapIterator.next();
          nextPartNumberMarker = partKeyInfoEntry.getKey();
          // As we should return only parts with part number greater
          // than part number marker
          if (partKeyInfoEntry.getKey() > partNumberMarker) {
            PartKeyInfo partKeyInfo = partKeyInfoEntry.getValue();
            OmPartInfo omPartInfo = new OmPartInfo(partKeyInfo.getPartNumber(),
                partKeyInfo.getPartName(),
                partKeyInfo.getPartKeyInfo().getModificationTime(),
                partKeyInfo.getPartKeyInfo().getDataSize());
            omPartInfoList.add(omPartInfo);
            replicationType = partKeyInfo.getPartKeyInfo().getType();
            count++;
          }
        }

        if (partKeyInfoMapIterator.hasNext()) {
          Map.Entry<Integer, PartKeyInfo> partKeyInfoEntry =
              partKeyInfoMapIterator.next();
          isTruncated = true;
        } else {
          isTruncated = false;
          nextPartNumberMarker = 0;
        }
        OmMultipartUploadListParts omMultipartUploadListParts =
            new OmMultipartUploadListParts(replicationType,
                nextPartNumberMarker, isTruncated);
        omMultipartUploadListParts.addPartList(omPartInfoList);
        return omMultipartUploadListParts;
      }
    } catch (OMException ex) {
      throw ex;
    } catch (IOException ex){
      LOG.error("List Multipart Upload Parts Failed: volume: " + volumeName +
              "bucket: " + bucketName + "key: " + keyName, ex);
      throw new OMException(ex.getMessage(), ResultCodes
              .LIST_MULTIPART_UPLOAD_PARTS_FAILED);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

}
