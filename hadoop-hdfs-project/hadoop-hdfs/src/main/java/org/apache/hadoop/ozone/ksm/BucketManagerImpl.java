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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ksm.helpers.KsmBucketArgs;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.iq80.leveldb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.OzoneConsts.DB_KEY_DELIMITER;

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
   * @param args - KsmBucketArgs.
   */
  @Override
  public void createBucket(KsmBucketArgs args) throws KSMException {
    Preconditions.checkNotNull(args);
    metadataManager.writeLock().lock();
    String volumeNameString = args.getVolumeName();
    String bucketNameString = args.getBucketName();
    try {
      //bucket key: {volume/bucket}
      String bucketKeyString = volumeNameString +
          DB_KEY_DELIMITER + bucketNameString;

      byte[] volumeName = DFSUtil.string2Bytes(volumeNameString);
      byte[] bucketKey = DFSUtil.string2Bytes(bucketKeyString);

      //Check if the volume exists
      if(metadataManager.get(volumeName) == null) {
        LOG.error("volume: {} not found ", volumeNameString);
        throw new KSMException("Volume doesn't exist",
            KSMException.ResultCodes.FAILED_VOLUME_NOT_FOUND);
      }
      //Check if bucket already exists
      if(metadataManager.get(bucketKey) != null) {
        LOG.error("bucket: {} already exists ", bucketNameString);
        throw new KSMException("Bucket already exist",
            KSMException.ResultCodes.FAILED_BUCKET_ALREADY_EXISTS);
      }
      metadataManager.put(bucketKey, args.getProtobuf().toByteArray());

      LOG.info("created bucket: {} in volume: {}", bucketNameString,
          volumeNameString);
    } catch (DBException ex) {
      LOG.error("Bucket creation failed for bucket:{} in volume:{}",
          volumeNameString, bucketNameString, ex);
      throw new KSMException(ex.getMessage(),
          KSMException.ResultCodes.FAILED_INTERNAL_ERROR);
    } finally {
      metadataManager.writeLock().unlock();
    }
  }
}