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
package org.apache.hadoop.ozone.ksm;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BatchOperation;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_IN_MB;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationType;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos.ReplicationFactor;


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
  private final KSMMetadataManager metadataManager;
  private final long scmBlockSize;
  private final boolean useRatis;
  private final BackgroundService keyDeletingService;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      KSMMetadataManager metadataManager, OzoneConfiguration conf) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
    this.scmBlockSize = conf.getLong(OZONE_SCM_BLOCK_SIZE_IN_MB,
        OZONE_SCM_BLOCK_SIZE_DEFAULT) * OzoneConsts.MB;
    this.useRatis = conf.getBoolean(DFS_CONTAINER_RATIS_ENABLED_KEY,
        DFS_CONTAINER_RATIS_ENABLED_DEFAULT);
    int svcInterval = conf.getInt(
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS,
        OZONE_BLOCK_DELETING_SERVICE_INTERVAL_MS_DEFAULT);
    long serviceTimeout = conf.getTimeDuration(
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT,
        OZONE_BLOCK_DELETING_SERVICE_TIMEOUT_DEFAULT, TimeUnit.MILLISECONDS);
    keyDeletingService = new KeyDeletingService(
        scmBlockClient, this, svcInterval, serviceTimeout, conf);
  }

  @Override
  public void start() {
    keyDeletingService.start();
  }

  @Override
  public void stop() throws IOException {
    keyDeletingService.shutdown();
  }

  @Override
  public KsmKeyInfo allocateKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    ReplicationFactor factor = args.getFactor();
    ReplicationType type = args.getType();

    // If user does not specify a replication strategy or
    // replication factor, KSM will use defaults.
    if(factor == null) {
      factor = useRatis ? ReplicationFactor.THREE: ReplicationFactor.ONE;
    }

    if(type == null) {
      type = useRatis ? ReplicationType.RATIS : ReplicationType.STAND_ALONE;
    }

    try {
      byte[] volumeKey = metadataManager.getVolumeKey(volumeName);
      byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      byte[] keyKey =
          metadataManager.getDBKeyBytes(volumeName, bucketName, keyName);

      //Check if the volume exists
      if (metadataManager.get(volumeKey) == null) {
        LOG.debug("volume not found: {}", volumeName);
        throw new KSMException("Volume not found",
            KSMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }
      //Check if bucket already exists
      if (metadataManager.get(bucketKey) == null) {
        LOG.debug("bucket not found: {}/{} ", volumeName, bucketName);
        throw new KSMException("Bucket not found",
            KSMException.ResultCodes.FAILED_BUCKET_NOT_FOUND);
      }

      // TODO: Garbage collect deleted blocks due to overwrite of a key.
      // FIXME: BUG: Please see HDFS-11922.
      // If user overwrites a key, then we are letting it pass without
      // corresponding process.
      // In reality we need to garbage collect those blocks by telling SCM to
      // clean up those blocks when it can. Right now making this change
      // allows us to pass tests that expect ozone can overwrite a key.

      // When we talk to SCM make sure that we ask for at least a byte in the
      // block. This way even if the call is for a zero length key, we back it
      // with a actual SCM block.
      // TODO : Review this decision later. We can get away with only a
      // metadata entry in case of 0 length key.
      long targetSize = args.getDataSize();
      List<KsmKeyLocationInfo> subKeyInfos = new ArrayList<>();
      int idx = 0;
      long offset = 0;

      // in case targetSize == 0, subKeyInfos will be an empty list
      while (targetSize > 0) {
        long allocateSize = Math.min(targetSize, scmBlockSize);
        AllocatedBlock allocatedBlock =
            scmBlockClient.allocateBlock(allocateSize, type, factor);
        KsmKeyLocationInfo subKeyInfo = new KsmKeyLocationInfo.Builder()
            .setContainerName(allocatedBlock.getPipeline().getContainerName())
            .setBlockID(allocatedBlock.getKey())
            .setShouldCreateContainer(allocatedBlock.getCreateContainer())
            .setIndex(idx)
            .setLength(allocateSize)
            .setOffset(offset)
            .build();
        idx += 1;
        offset += allocateSize;
        targetSize -= allocateSize;
        subKeyInfos.add(subKeyInfo);
      }

      long currentTime = Time.now();
      KsmKeyInfo keyBlock = new KsmKeyInfo.Builder()
          .setVolumeName(args.getVolumeName())
          .setBucketName(args.getBucketName())
          .setKeyName(args.getKeyName())
          .setDataSize(args.getDataSize())
          .setKsmKeyLocationInfos(subKeyInfos)
          .setCreationTime(currentTime)
          .setModificationTime(currentTime)
          .build();
      metadataManager.put(keyKey, keyBlock.getProtobuf().toByteArray());
      LOG.debug("Key {} allocated in volume {} bucket {}", keyName, volumeName,
          bucketName);
      return keyBlock;
    } catch (KSMException e) {
      throw e;
    } catch (IOException ex) {
      if (!(ex instanceof KSMException)) {
        LOG.error("Key allocation failed for volume:{} bucket:{} key:{}",
            volumeName, bucketName, keyName, ex);
      }
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_KEY_ALLOCATION);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public KsmKeyInfo lookupKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      byte[] keyKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, keyName);
      byte[] value = metadataManager.get(keyKey);
      if (value == null) {
        LOG.debug("volume:{} bucket:{} Key:{} not found",
            volumeName, bucketName, keyName);
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      return KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(value));
    } catch (DBException ex) {
      LOG.error("Get key failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public void deleteKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      byte[] objectKey = metadataManager.getDBKeyBytes(
          volumeName, bucketName, keyName);
      byte[] objectValue = metadataManager.get(objectKey);
      if (objectValue == null) {
        throw new KSMException("Key not found",
            KSMException.ResultCodes.FAILED_KEY_NOT_FOUND);
      }
      byte[] deletingKey = metadataManager.getDeletedKeyName(objectKey);
      BatchOperation batch = new BatchOperation();
      batch.put(deletingKey, objectValue);
      batch.delete(objectKey);
      metadataManager.writeBatch(batch);
    } catch (DBException ex) {
      LOG.error(String.format("Delete key failed for volume:%s "
          + "bucket:%s key:%s", volumeName, bucketName, keyName), ex);
      throw new KSMException(ex.getMessage(), ex,
          ResultCodes.FAILED_KEY_DELETION);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  @Override
  public List<KsmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);

    metadataManager.readLock().lock();
    try {
      return metadataManager.listKeys(volumeName, bucketName,
          startKey, keyPrefix, maxKeys);
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int count)
      throws IOException {
    metadataManager.readLock().lock();
    try {
      return metadataManager.getPendingDeletionKeys(count);
    } finally {
      metadataManager.readLock().unlock();
    }
  }

  @Override
  public void deletePendingDeletionKey(String objectKeyName)
      throws IOException{
    Preconditions.checkNotNull(objectKeyName);
    if (!objectKeyName.startsWith(OzoneConsts.DELETING_KEY_PREFIX)) {
      throw new IllegalArgumentException("Invalid key name,"
          + " the name should be the key name with deleting prefix");
    }

    // Simply removes the entry from KSM DB.
    metadataManager.writeLock().lock();
    try {
      byte[] pendingDelKey = DFSUtil.string2Bytes(objectKeyName);
      byte[] delKeyValue = metadataManager.get(pendingDelKey);
      if (delKeyValue == null) {
        throw new IOException("Failed to delete key " + objectKeyName
            + " because it is not found in DB");
      }
      metadataManager.delete(pendingDelKey);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }
}
