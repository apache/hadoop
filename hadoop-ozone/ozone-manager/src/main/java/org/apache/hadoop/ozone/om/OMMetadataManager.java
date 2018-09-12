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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.Table;

import java.io.IOException;
import java.util.List;

/**
 * OM metadata manager interface.
 */
public interface OMMetadataManager {
  /**
   * Start metadata manager.
   */
  void start();

  /**
   * Stop metadata manager.
   */
  void stop() throws Exception;

  /**
   * Get metadata store.
   *
   * @return metadata store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  OzoneManagerLock getLock();

  /**
   * Given a volume return the corresponding DB key.
   *
   * @param volume - Volume name
   */
  byte[] getVolumeKey(String volume);

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  byte[] getUserKey(String user);

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  byte[] getBucketKey(String volume, String bucket);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @return bytes of DB key.
   */
  byte[] getOzoneKeyBytes(String volume, String bucket, String key);

  /**
   * Returns the DB key name of a open key in OM metadata store. Should be
   * #open# prefix followed by actual key name.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param id - the id for this open
   * @return bytes of DB key.
   */
  byte[] getOpenKeyBytes(String volume, String bucket, String key, long id);

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   *
   * @param volume - Volume name
   */
  boolean isVolumeEmpty(String volume) throws IOException;

  /**
   * Given a volume/bucket, check if it is empty, i.e there are no keys inside
   * it.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  boolean isBucketEmpty(String volume, String bucket) throws IOException;

  /**
   * Returns a list of buckets represented by {@link OmBucketInfo} in the given
   * volume.
   *
   * @param volumeName the name of the volume. This argument is required, this
   * method returns buckets in this given volume.
   * @param startBucket the start bucket name. Only the buckets whose name is
   * after this value will be included in the result. This key is excluded from
   * the result.
   * @param bucketPrefix bucket name prefix. Only the buckets whose name has
   * this prefix will be included in the result.
   * @param maxNumOfBuckets the maximum number of buckets to return. It ensures
   * the size of the result will not exceed this limit.
   * @return a list of buckets.
   * @throws IOException
   */
  List<OmBucketInfo> listBuckets(String volumeName, String startBucket,
      String bucketPrefix, int maxNumOfBuckets)
      throws IOException;

  /**
   * Returns a list of keys represented by {@link OmKeyInfo} in the given
   * bucket.
   *
   * @param volumeName the name of the volume.
   * @param bucketName the name of the bucket.
   * @param startKey the start key name, only the keys whose name is after this
   * value will be included in the result. This key is excluded from the
   * result.
   * @param keyPrefix key name prefix, only the keys whose name has this prefix
   * will be included in the result.
   * @param maxKeys the maximum number of keys to return. It ensures the size of
   * the result will not exceed this limit.
   * @return a list of keys.
   * @throws IOException
   */
  List<OmKeyInfo> listKeys(String volumeName,
      String bucketName, String startKey, String keyPrefix, int maxKeys)
      throws IOException;

  /**
   * Returns a list of volumes owned by a given user; if user is null, returns
   * all volumes.
   *
   * @param userName volume owner
   * @param prefix the volume prefix used to filter the listing result.
   * @param startKey the start volume name determines where to start listing
   * from, this key is excluded from the result.
   * @param maxKeys the maximum number of volumes to return.
   * @return a list of {@link OmVolumeArgs}
   * @throws IOException
   */
  List<OmVolumeArgs> listVolumes(String userName, String prefix,
      String startKey, int maxKeys) throws IOException;

  /**
   * Returns a list of pending deletion key info that ups to the given count.
   * Each entry is a {@link BlockGroup}, which contains the info about the key
   * name and all its associated block IDs. A pending deletion key is stored
   * with #deleting# prefix in OM DB.
   *
   * @param count max number of keys to return.
   * @return a list of {@link BlockGroup} represent keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getPendingDeletionKeys(int count) throws IOException;

  /**
   * Returns a list of all still open key info. Which contains the info about
   * the key name and all its associated block IDs. A pending open key has
   * prefix #open# in OM DB.
   *
   * @return a list of {@link BlockGroup} representing keys and blocks.
   * @throws IOException
   */
  List<BlockGroup> getExpiredOpenKeys() throws IOException;

  /**
   * Returns the user Table.
   *
   * @return UserTable.
   */
  Table getUserTable();

  /**
   * Returns the Volume Table.
   *
   * @return VolumeTable.
   */
  Table getVolumeTable();

  /**
   * Returns the BucketTable.
   *
   * @return BucketTable.
   */
  Table getBucketTable();

  /**
   * Returns the KeyTable.
   *
   * @return KeyTable.
   */
  Table getKeyTable();

  /**
   * Get Deleted Table.
   *
   * @return Deleted Table.
   */
  Table getDeletedTable();

  /**
   * Gets the OpenKeyTable.
   *
   * @return Table.
   */
  Table getOpenKeyTable();

}
