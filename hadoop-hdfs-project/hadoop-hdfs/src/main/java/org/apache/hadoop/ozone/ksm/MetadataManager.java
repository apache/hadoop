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

import org.apache.hadoop.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ksm.helpers.KsmKeyInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

/**
 * KSM metadata manager interface.
 */
public interface MetadataManager {
  /**
   * Start metadata manager.
   */
  void start();

  /**
   * Stop metadata manager.
   */
  void stop() throws IOException;

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
  byte[] get(byte[] key);

  /**
   * Puts a Key into Metadata DB.
   * @param key   - key
   * @param value - value
   */
  void put(byte[] key, byte[] value);

  /**
   * Deletes a Key from Metadata DB.
   * @param key   - key
   */
  void delete(byte[] key);

  /**
   * Performs batch Put and Delete to Metadata DB.
   * Can be used to do multiple puts and deletes atomically.
   * @param putList - list of Key/Value to put into DB
   * @param delList - list of Key to delete from DB
   */
  void batchPutDelete(List<Map.Entry<byte[], byte[]>> putList,
                      List<byte[]> delList) throws IOException;

  /**
   * Performs a batch Put to Metadata DB.
   * Can be used to do multiple puts atomically.
   * @param putList - list of Key/Value to put into DB
   */
  void batchPut(List<Map.Entry<byte[], byte[]>> putList) throws IOException;

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
  byte[] getDBKeyForKey(String volume, String bucket, String key);

  /**
   * Deletes the key from DB.
   *
   * @param key - key name
   */
  void deleteKey(byte[] key);

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
}
