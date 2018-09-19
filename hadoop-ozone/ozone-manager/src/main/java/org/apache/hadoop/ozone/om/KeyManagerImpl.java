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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.protocol.proto
    .OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_IN_MB;

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

  private final BackgroundService keyDeletingService;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      OMMetadataManager metadataManager,
      OzoneConfiguration conf,
      String omId) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
    this.scmBlockSize = conf.getLong(OZONE_SCM_BLOCK_SIZE_IN_MB,
        OZONE_SCM_BLOCK_SIZE_DEFAULT) * OzoneConsts.MB;
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    this.preallocateMax = conf.getLong(
        OZONE_KEY_PREALLOCATION_MAXSIZE,
        OZONE_KEY_PREALLOCATION_MAXSIZE_DEFAULT);
    long blockDeleteInterval = conf.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    long serviceTimeout = conf.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);
    keyDeletingService = new KeyDeletingService(
        scmBlockClient, this, blockDeleteInterval, serviceTimeout, conf);

    this.omId = omId;
  }

  @Override
  public void start() {
    keyDeletingService.start();
  }

  @Override
  public void stop() throws IOException {
    keyDeletingService.shutdown();
  }

  private void validateBucket(String volumeName, String bucketName)
      throws IOException {
    byte[] volumeKey = metadataManager.getVolumeKey(volumeName);
    byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

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
    byte[] openKey = metadataManager.getOpenKeyBytes(
        volumeName, bucketName, keyName, clientID);

    byte[] keyData = metadataManager.getOpenKeyTable().get(openKey);
    if (keyData == null) {
      LOG.error("Allocate block for a key not in open status in meta store" +
          " /{}/{}/{} with ID {}", volumeName, bucketName, keyName, clientID);
      throw new OMException("Open Key not found",
          OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
    }
    OmKeyInfo keyInfo =
        OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(keyData));
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
        .setBlockID(allocatedBlock.getBlockID())
        .setShouldCreateContainer(allocatedBlock.getCreateContainer())
        .setLength(scmBlockSize)
        .setOffset(0)
        .build();
    // current version not committed, so new blocks coming now are added to
    // the same version
    keyInfo.appendNewBlocks(Collections.singletonList(info));
    keyInfo.updateModifcationTime();
    metadataManager.getOpenKeyTable().put(openKey,
        keyInfo.getProtobuf().toByteArray());
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

    // If user does not specify a replication strategy or
    // replication factor, OM will use defaults.
    if (factor == null) {
      factor = useRatis ? ReplicationFactor.THREE : ReplicationFactor.ONE;
    }

    if (type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    try {
      long requestedSize = Math.min(preallocateMax, args.getDataSize());
      List<OmKeyLocationInfo> locations = new ArrayList<>();
      byte[] objectKey = metadataManager.getOzoneKeyBytes(
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
            .setBlockID(allocatedBlock.getBlockID())
            .setShouldCreateContainer(allocatedBlock.getCreateContainer())
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
      byte[] value = metadataManager.getKeyTable().get(objectKey);
      OmKeyInfo keyInfo;
      long openVersion;
      if (value != null) {
        // the key already exist, the new blocks will be added as new version
        keyInfo = OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(value));
        // when locations.size = 0, the new version will have identical blocks
        // as its previous version
        openVersion = keyInfo.addNewVersion(locations);
        keyInfo.setDataSize(size + keyInfo.getDataSize());
      } else {
        // the key does not exist, create a new object, the new blocks are the
        // version 0

        keyInfo = new OmKeyInfo.Builder()
            .setVolumeName(args.getVolumeName())
            .setBucketName(args.getBucketName())
            .setKeyName(args.getKeyName())
            .setOmKeyLocationInfos(Collections.singletonList(
                new OmKeyLocationInfoGroup(0, locations)))
            .setCreationTime(Time.now())
            .setModificationTime(Time.now())
            .setDataSize(size)
            .setReplicationType(type)
            .setReplicationFactor(factor)
            .build();
        openVersion = 0;
      }
      byte[] openKey = metadataManager.getOpenKeyBytes(
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
      metadataManager.getOpenKeyTable().put(openKey,
          keyInfo.getProtobuf().toByteArray());
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

  @Override
  public void commitKey(OmKeyArgs args, long clientID) throws IOException {
    Preconditions.checkNotNull(args);
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    metadataManager.getLock().acquireBucketLock(volumeName, bucketName);
    try {
      validateBucket(volumeName, bucketName);
      byte[] openKey = metadataManager.getOpenKeyBytes(volumeName, bucketName,
          keyName, clientID);
      byte[] objectKey = metadataManager.getOzoneKeyBytes(
          volumeName, bucketName, keyName);
      byte[] openKeyData = metadataManager.getOpenKeyTable().get(openKey);
      if (openKeyData == null) {
        throw new OMException("Commit a key without corresponding entry " +
            DFSUtil.bytes2String(objectKey), ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      OmKeyInfo keyInfo =
          OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(openKeyData));
      keyInfo.setDataSize(args.getDataSize());
      keyInfo.setModificationTime(Time.now());
      List<OmKeyLocationInfo> locationInfoList = args.getLocationInfoList();
      Preconditions.checkNotNull(locationInfoList);

      //update the block length for each block
      keyInfo.updateLocationInfoList(locationInfoList);
      metadataManager.getStore().move(openKey, objectKey,
          keyInfo.getProtobuf().toByteArray(),
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
      byte[] keyBytes = metadataManager.getOzoneKeyBytes(
          volumeName, bucketName, keyName);
      byte[] value = metadataManager.getKeyTable().get(keyBytes);
      if (value == null) {
        LOG.debug("volume:{} bucket:{} Key:{} not found",
            volumeName, bucketName, keyName);
        throw new OMException("Key not found",
            OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      return OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(value));
    } catch (IOException ex) {
      LOG.error("Get key failed for volume:{} bucket:{} key:{}",
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
      byte[] fromKey = metadataManager.getOzoneKeyBytes(
          volumeName, bucketName, fromKeyName);
      byte[] fromKeyValue = metadataManager.getKeyTable().get(fromKey);
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
      // TODO: Define the semantics of rename more clearly. Today this code
      // will allow rename of a Key across volumes. This should *not* be
      // allowed. The documentation of Ozone says that rename is permitted only
      // within a volume.
      if (fromKeyName.equals(toKeyName)) {
        return;
      }

      // toKeyName should not exist
      byte[] toKey =
          metadataManager.getOzoneKeyBytes(volumeName, bucketName, toKeyName);
      byte[] toKeyValue = metadataManager.getKeyTable().get(toKey);
      if (toKeyValue != null) {
        LOG.error(
            "Rename key failed for volume:{} bucket:{} fromKey:{} toKey:{}. "
                + "Key: {} already exists.", volumeName, bucketName,
            fromKeyName, toKeyName, toKeyName);
        throw new OMException("Key not found",
            OMException.ResultCodes.FAILED_KEY_ALREADY_EXISTS);
      }


      OmKeyInfo newKeyInfo =
          OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(fromKeyValue));
      newKeyInfo.setKeyName(toKeyName);
      newKeyInfo.updateModifcationTime();
      try (WriteBatch batch = new WriteBatch()) {
        batch.delete(metadataManager.getKeyTable().getHandle(), fromKey);
        batch.put(metadataManager.getKeyTable().getHandle(), toKey,
            newKeyInfo.getProtobuf().toByteArray());
        metadataManager.getStore().write(batch);
      }
    } catch (RocksDBException | IOException ex) {
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
      byte[] objectKey = metadataManager.getOzoneKeyBytes(
          volumeName, bucketName, keyName);
      byte[] objectValue = metadataManager.getKeyTable().get(objectKey);
      if (objectValue == null) {
        throw new OMException("Key not found",
            OMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      } else {
        // directly delete key with no blocks from db. This key need not be
        // moved to deleted table.
        KeyInfo keyInfo = KeyInfo.parseFrom(objectValue);
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

  private boolean isKeyEmpty(KeyInfo keyInfo) {
    for (KeyLocationList keyLocationList : keyInfo.getKeyLocationListList()) {
      if (keyLocationList.getKeyLocationsCount() != 0) {
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
}
