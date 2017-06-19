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
import org.apache.hadoop.ksm.helpers.KsmKeyArgs;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmBlockResult.Result;
import org.apache.hadoop.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.scm.container.common.helpers.DeleteBlockResult;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
  private final MetadataManager metadataManager;

  public KeyManagerImpl(ScmBlockLocationProtocol scmBlockClient,
      MetadataManager metadataManager) {
    this.scmBlockClient = scmBlockClient;
    this.metadataManager = metadataManager;
  }

  @Override
  public KsmKeyInfo allocateKey(KsmKeyArgs args) throws IOException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      byte[] volumeKey = metadataManager.getVolumeKey(volumeName);
      byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      byte[] keyKey = metadataManager.getDBKeyForKey(
          volumeName, bucketName, keyName);

      //Check if the volume exists
      if(metadataManager.get(volumeKey) == null) {
        LOG.error("volume not found: {}", volumeName);
        throw new KSMException("Volume not found",
            KSMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }
      //Check if bucket already exists
      if(metadataManager.get(bucketKey) == null) {
        LOG.error("bucket not found: {}/{} ", volumeName, bucketName);
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
      AllocatedBlock allocatedBlock =
          scmBlockClient.allocateBlock(Math.max(args.getDataSize(), 1));
      KsmKeyInfo keyBlock = new KsmKeyInfo.Builder()
          .setVolumeName(args.getVolumeName())
          .setBucketName(args.getBucketName())
          .setKeyName(args.getKeyName())
          .setDataSize(args.getDataSize())
          .setBlockID(allocatedBlock.getKey())
          .setContainerName(allocatedBlock.getPipeline().getContainerName())
          .setShouldCreateContainer(allocatedBlock.getCreateContainer())
          .build();
      metadataManager.put(keyKey, keyBlock.getProtobuf().toByteArray());
      LOG.debug("Key {} allocated in volume {} bucket {}",
          keyName, volumeName, bucketName);
      return keyBlock;
    } catch (Exception ex) {
      LOG.error("Key allocation failed for volume:{} bucket:{} key:{}",
          volumeName, bucketName, keyName, ex);
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_INTERNAL_ERROR);
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
      byte[] keyKey = metadataManager.getDBKeyForKey(
          volumeName, bucketName, keyName);
      byte[] value = metadataManager.get(keyKey);
      if (value == null) {
        LOG.error("Key: {} not found", keyKey);
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
    KsmKeyInfo keyInfo = lookupKey(args);

    metadataManager.writeLock().lock();
    String volumeName = args.getVolumeName();
    String bucketName = args.getBucketName();
    String keyName = args.getKeyName();
    try {
      List<DeleteBlockResult> resultList =
          scmBlockClient.deleteBlocks(
              Collections.singleton(keyInfo.getBlockID()));
      if (resultList.size() != 1) {
        throw new KSMException("Delete result size from SCM is wrong",
            ResultCodes.FAILED_INTERNAL_ERROR);
      }

      if (resultList.get(0).getResult() == Result.success) {
        byte[] objectKey = metadataManager.getDBKeyForKey(
            volumeName, bucketName, keyName);
        metadataManager.deleteKey(objectKey);
      } else {
        throw new KSMException("Cannot delete key from SCM",
                ResultCodes.FAILED_INTERNAL_ERROR);
      }
    } catch (DBException ex) {
      LOG.error(String.format("Delete key failed for volume:%s "
          + "bucket:%s key:%s", volumeName, bucketName, keyName), ex);
      throw new KSMException(ex.getMessage(), ex,
          ResultCodes.FAILED_INTERNAL_ERROR);
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
}
