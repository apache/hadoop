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
import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.protocol.proto
    .KeySpaceManagerProtocolProtos.BucketInfo;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * KSM bucket manager.
 */
public class BucketManagerImpl implements BucketManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(BucketManagerImpl.class);

  /**
   * MetadataManager is used for accessing KSM MetadataDB and ReadWriteLock.
   */
  private final MetadataManager metadataManager;

  /**
   * Constructs BucketManager.
   * @param metadataManager
   */
  public BucketManagerImpl(MetadataManager metadataManager){
    this.metadataManager = metadataManager;
  }

  /**
   * MetadataDB is maintained in MetadataManager and shared between
   * BucketManager and VolumeManager. (and also by KeyManager)
   *
   * BucketManager uses MetadataDB to store bucket level information.
   *
   * Keys used in BucketManager for storing data into MetadataDB
   * for BucketInfo:
   * {volume/bucket} -> bucketInfo
   *
   * Work flow of create bucket:
   *
   * -> Check if the Volume exists in metadataDB, if not throw
   * VolumeNotFoundException.
   * -> Else check if the Bucket exists in metadataDB, if so throw
   * BucketExistException
   * -> Else update MetadataDB with VolumeInfo.
   */

  /**
   * Creates a bucket.
   * @param bucketInfo - KsmBucketInfo.
   */
  @Override
  public void createBucket(KsmBucketInfo bucketInfo) throws IOException {
    Preconditions.checkNotNull(bucketInfo);
    metadataManager.writeLock().lock();
    String volumeName = bucketInfo.getVolumeName();
    String bucketName = bucketInfo.getBucketName();
    try {
      byte[] volumeKey = metadataManager.getVolumeKey(volumeName);
      byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);

      //Check if the volume exists
      if(metadataManager.get(volumeKey) == null) {
        LOG.error("volume: {} not found ", volumeName);
        throw new KSMException("Volume doesn't exist",
            KSMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }
      //Check if bucket already exists
      if(metadataManager.get(bucketKey) != null) {
        LOG.error("bucket: {} already exists ", bucketName);
        throw new KSMException("Bucket already exist",
            KSMException.ResultCodes.FAILED_BUCKET_ALREADY_EXISTS);
      }
      metadataManager.put(bucketKey, bucketInfo.getProtobuf().toByteArray());

      LOG.debug("created bucket: {} in volume: {}", bucketName, volumeName);
    } catch (IOException | DBException ex) {
      LOG.error("Bucket creation failed for bucket:{} in volume:{}",
          bucketName, volumeName, ex);
      throw ex;
    } finally {
      metadataManager.writeLock().unlock();
    }
  }

  /**
   * Returns Bucket Information.
   *
   * @param volumeName - Name of the Volume.
   * @param bucketName - Name of the Bucket.
   */
  @Override
  public KsmBucketInfo getBucketInfo(String volumeName, String bucketName)
      throws IOException {
    Preconditions.checkNotNull(volumeName);
    Preconditions.checkNotNull(bucketName);
    metadataManager.readLock().lock();
    try {
      byte[] bucketKey = metadataManager.getBucketKey(volumeName, bucketName);
      byte[] value = metadataManager.get(bucketKey);
      if(value == null) {
        LOG.error("bucket: {} not found in volume: {}.",
            bucketName, volumeName);
        throw new KSMException("Bucket not found",
            KSMException.ResultCodes.FAILED_BUCKET_NOT_FOUND);
      }
      return KsmBucketInfo.getFromProtobuf(BucketInfo.parseFrom(value));
    } catch (IOException | DBException ex) {
      LOG.error("Exception while getting bucket info for bucket: {}",
          bucketName, ex);
      throw ex;
    } finally {
      metadataManager.readLock().unlock();
    }
  }
}
