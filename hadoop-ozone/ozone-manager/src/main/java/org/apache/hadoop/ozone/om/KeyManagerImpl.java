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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.db.BatchOperation;
import org.apache.hadoop.utils.db.DBStore;

import com.google.common.base.Preconditions;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

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

  private final long preallocateMax;
  private final String omId;

  private BackgroundService keyDeletingService;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager,
      OzoneConfiguration conf,
      String omId) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
    this.scmBlockSize = (long) conf
        .getStorageSize(OZONE_SCM_BLOCK_SIZE, OZONE_SCM_BLOCK_SIZE_DEFAULT,
            StorageUnit.BYTES);
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.preallocateMax = conf.getLong(
        OZONE_KEY_PREALLOCATION_MAXSIZE,
        OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT);
    this.omId = omId;
    start(conf);
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

  @Override
  public void stop() throws IOException {
    if (keyDeletingService != null) {
      keyDeletingService.shutdown();
      keyDeletingService = null;
    }
  }

  private void validateBucket(String volumeName, String bucketName)
      throws IOException {
    String volumeKey = metadataManager.getVolumeKey(volumeName);
    String bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

    //Check if the volume exists
    if (metadataManager.getVolumeTable().get(volumeKey) == null) {
      LOG.error("volume not found: {}", volumeName);
      throw new OMException("Volume not found",
          OMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }
    //Check if bucket already exists
    if (metadataManager.getBucketTable().get(bucketKey) == null) {
      LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
      throw new OMException("Bucket not found",
          OMException.ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientID)
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
          OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
    }

    AllocatedBlock allocatedBlock;
    try {
      allocatedBlock =
          scmBlockClient.allocateBlock(scmBlockSize, keyInfo.getType(),
              keyInfo.getFactor(), omId);
    } catch (SCMException ex) {
      if (ex.getResult()
          .equals(SCMException.ResultCodes.CHILL_MODE_EXCEPTION)) {
        throw new OMException(ex.getMessage(), ResultCodes.SCM_IN_CHILL_MODE);
      }
      throw ex;
    }
    OmKeyLocationInfo info = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(allocatedBlock.getBlockID()))
        .setLength(scmBlockSize)
        .setOffset(0)
        .build();
    // current version not committed, so new blocks coming now are added to
    // the same version
    keyInfo.appendNewBlocks(Collections.singletonList(info));
    keyInfo.updateModifcationTime();
    metadataManager.getOpenKeyTable().put(openKey,
        keyInfo);
    return info;
  }

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

    try {
      if (args.getIsMultipartKey()) {
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
              "uploadId " + uploadID, ResultCodes.NO_SUCH_MULTIPART_UPLOAD);
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
      long requestedSize = Math.min(preallocateMax, args.getDataSize());
      List<OmKeyLocationInfo> locations = new ArrayList<>();
      String objectKey = metadataManager.getOzoneKey(
          volumeName, bucketName, keyName);
      // requested size is not required but more like a optimization:
      // SCM looks at the requested, if it 0, no block will be allocated at
      // the point, if client needs more blocks, client can always call
      // allocateBlock. But if requested size is not 0, OM will preallocate
      // some blocks and piggyback to client, to save RPC calls.
      while (requestedSize > 0) {
        long allocateSize = Math.min(scmBlockSize, requestedSize);
        AllocatedBlock allocatedBlock;
        try {
          allocatedBlock = scmBlockClient
              .allocateBlock(allocateSize, type, factor, omId);
        } catch (IOException ex) {
          if (ex instanceof SCMException) {
            if (((SCMException) ex).getResult()
                .equals(SCMException.ResultCodes.CHILL_MODE_EXCEPTION)) {
              throw new OMException(ex.getMessage(),
                  ResultCodes.SCM_IN_CHILL_MODE);
            }
          }
          throw ex;
        }
        OmKeyLocationInfo subKeyInfo = new OmKeyLocationInfo.Builder()
            .setBlockID(new BlockID(allocatedBlock.getBlockID()))
            .setLength(allocateSize)
            .setOffset(0)
            .build();
        locations.add(subKeyInfo);
        requestedSize -= allocateSize;
      }
      // NOTE size of a key is not a hard limit on anything, it is a value that
      // client should expect, in terms of current size of key. If client sets a
      // value, then this value is used, otherwise, we allocate a single block
      // which is the current size, if read by the client.
      long size = args.getDataSize() >= 0 ? args.getDataSize() : scmBlockSize;
      OmKeyInfo keyInfo;
      long openVersion;

      if (args.getIsMultipartKey()) {
        // For this upload part we don't need to check in KeyTable. As this
        // is not an actual key, it is a part of the key.
        keyInfo = createKeyInfo(args, locations, factor, type, size);
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
          keyInfo = createKeyInfo(args, locations, factor, type, size);
          openVersion = 0;
        }
      }
      String openKey = metadataManager.getOpenKey(
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
            "open key id.", OMException.ResultCodes.FAILED_KEY_ALLOCATION);
      }
      metadataManager.getOpenKeyTable().put(openKey, keyInfo);
      LOG.debug("Key {} allocated in volume {} bucket {}",
          keyName, volumeName, bucketName);
      return new OpenKeySession(currentTime, keyInfo, openVersion);
    } catch (OMException e) {
      throw e;
    } catch (IOException ex) {
      LOG.error("Key open failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          OMException.ResultCodes.FAILED_KEY_ALLOCATION);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }
  }

  /**
   * Create OmKeyInfo object.
   * @param keyArgs
   * @param locations
   * @param factor
   * @param type
   * @param size
   * @return
   */
  private OmKeyInfo createKeyInfo(OmKeyArgs keyArgs,
                                  List<OmKeyLocationInfo> locations,
                                  ReplicationFactor factor,
                                  ReplicationType type, long size) {
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
            objectKey, ResultCodes.FAILED_KEY_NOT_FOUND);
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
          OMException.ResultCodes.FAILED_KEY_ALLOCATION);
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
            OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      return value;
    } catch (IOException ex) {
      LOG.debug("Get key failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new OMException(ex.getMessage(),
          OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
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
          ResultCodes.FAILED_INVALID_KEY_NAME);
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
            OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
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
        throw new OMException("Key not found",
            OMException.ResultCodes.FAILED_KEY_ALREADY_EXISTS);
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
      LOG.error("Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}",
          volumeName, bucketName, fromKeyName, toKeyName, ex);
      throw new OMException(ex.getMessage(),
          ResultCodes.FAILED_KEY_RENAME);
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
            OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
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
          ResultCodes.FAILED_KEY_DELETION);
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
          OMException.ResultCodes.INITIATE_MULTIPART_UPLOAD_FAILED);
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

      partName = keyName + clientID;
      if (multipartKeyInfo == null) {
        throw new OMException("No such Multipart upload is with specified " +
            "uploadId " + uploadID, ResultCodes.NO_SUCH_MULTIPART_UPLOAD);
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
      throw new OMException(ex.getMessage(), ResultCodes.UPLOAD_PART_FAILED);
    } finally {
      metadataManager.getLock().releaseBucketLock(volumeName, bucketName);
    }

    return new OmMultipartCommitUploadPartInfo(partName);

  }

}
