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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * KSM metadata manager interface.
 */
public interface KSMMetadataManager {
  /**
   * Start metadata manager.
   */
  void start();

  /**
   * Stop metadata manager.
   */
  void stop() throws IOException;

  /**
   * Get metadata store.
   * @return metadata store.
   */
  @VisibleForTesting
  MetadataStore getStore();

  /**
   * Returns the read lock used on Metadata DB.
   * @return readLock
   */
  Lock readLock();

  /**
   * Returns the write lock used on Metadata DB.
   * @return writeLock
   */
  Lock writeLock();

  /**
   * Returns the value associated with this key.
   * @param key - key
   * @return value
   */
  byte[] get(byte[] key) throws IOException;

  /**
   * Puts a Key into Metadata DB.
   * @param key   - key
   * @param value - value
   */
  void put(byte[] key, byte[] value) throws IOException;

  /**
   * Deletes a Key from Metadata DB.
   * @param key   - key
   */
  void delete(byte[] key) throws IOException;

  /**
   * Atomic write a batch of operations.
   * @param batch
   * @throws IOException
   */
  void writeBatch(BatchOperation batch) throws IOException;

  /**
   * Given a volume return the corresponding DB key.
   * @param volume - Volume name
   */
  byte[] getVolumeKey(String volume);

  /**
   * Given a user return the corresponding DB key.
   * @param user - User name
   */
  byte[] getUserKey(String user);

  /**
   * Given a volume and bucket, return the corresponding DB key.
   * @param volume - User name
   * @param bucket - Bucket name
   */
  byte[] getBucketKey(String volume, String bucket);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @return bytes of DB key.
   */
  byte[] getDBKeyBytes(String volume, String bucket, String key);

  /**
   * Returns the DB key name of a deleted key in KSM metadata store.
   * The name for a deleted key has prefix #deleting# followed by
   * the actual key name.
   * @param keyName - key name
   * @return bytes of DB key.
   */
  byte[] getDeletedKeyName(byte[] keyName);

  /**
   * Returns the DB key name of a open key in KSM metadata store.
   * Should be #open# prefix followed by actual key name.
   * @param keyName - key name
   * @param id - the id for this open
   * @return bytes of DB key.
   */
  byte[] getOpenKeyNameBytes(String keyName, int id);

  /**
   * Returns the full name of a key given volume name, bucket name and key name.
   * Generally done by padding certain delimiters.
   *
   * @param volumeName - volume name
   * @param bucketName - bucket name
   * @param keyName - key name
   * @return the full key name.
   */
  String getKeyWithDBPrefix(String volumeName, String bucketName,
      String keyName);

  /**
   * Given a volume, check if it is empty,
   * i.e there are no buckets inside it.
   * @param volume - Volume name
   */
  boolean isVolumeEmpty(String volume) throws IOException;

  /**
   * Given a volume/bucket, check if it is empty,
   * i.e there are no keys inside it.
   * @param volume - Volume name
   * @param  bucket - Bucket name
   * @return true if the bucket is empty
   */
  boolean isBucketEmpty(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link KsmBucketInfo}
   * in the given volume.
   *
   * @param volumeName
   *   the name of the volume. This argument is required,
   *   this method returns buckets in this given volume.
   * @param startBucket
   *   the start bucket name. Only the buckets whose name is
   *   after this value will be included in the result.
   *   This key is excluded from the result.
   * @param bucketPrefix
   *   bucket name prefix. Only the buckets whose name has
   *   this prefix will be included in the result.
   * @param maxNumOfBuckets
   *   the maximum number of buckets to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<KsmBucketInfo> listBuckets(String volumeName, String startBucket,
      String bucketPrefix, int maxNumOfBuckets) throws IOException;

  /**
   * Returns a list of keys represented by {@link KsmKeyInfo}
   * in the given bucket.
   *
   * @param volumeName
   *   the name of the volume.
   * @param bucketName
   *   the name of the bucket.
   * @param startKey
   *   the start key name, only the keys whose name is
   *   after this value will be included in the result.
   *   This key is excluded from the result.
   * @param keyPrefix
   *   key name prefix, only the keys whose name has
   *   this prefix will be included in the result.
   * @param maxKeys
   *   the maximum number of keys to return. It ensures
   *   the size of the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<KsmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns a list of volumes owned by a given user; if user is null,
   * returns all volumes.
   *
   * @param userName
   *   volume owner
   * @param prefix
   *   the volume prefix used to filter the listing result.
   * @param startKey
   *   the start volume name determines where to start listing from,
   *   this key is excluded from the result.
   * @param maxKeys
   *   the maximum number of volumes to return.
   * @return a list of {@link KsmVolumeArgs}
   * @throws IOException
   */
  List<KsmVolumeArgs> listVolumes(String userName, String prefix,
      String startKey, int maxKeys) throws IOException;

  /**
   * Returns a list of pending deletion key info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the
   * key name and all its associated block IDs. A pending deletion key is
   * stored with #deleting# prefix in KSM DB.
   *
   * @param count max number of keys to return.
   * @return a list of {@link BlockGroup} represent keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionKeys(int count) throws IOException;
}
