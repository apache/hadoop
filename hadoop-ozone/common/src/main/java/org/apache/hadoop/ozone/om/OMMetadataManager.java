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
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.Table;

import com.google.common.annotations.VisibleForTesting;

/**
 * OM metadata manager interface.
 */
public interface OMMetadataManager {
  /**
   * Start metadata manager.
   *
   * @param configuration
   * @throws IOException
   */
  void start(OzoneConfiguration configuration) throws IOException;

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
  String getVolumeKey(String volume);

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  String getUserKey(String user);

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  String getBucketKey(String volume, String bucket);

  /**
   * Given a volume, bucket and a key, return the corresponding DB key.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key    - key name
   * @return DB key as String.
   */

  String getOzoneKey(String volume, String bucket, String key);


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
  String getOpenKey(String volume, String bucket, String key, long id);

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
  Table<String, VolumeList> getUserTable();

  /**
   * Returns the Volume Table.
   *
   * @return VolumeTable.
   */
  Table<String, OmVolumeArgs> getVolumeTable();

  /**
   * Returns the BucketTable.
   *
   * @return BucketTable.
   */
  Table<String, OmBucketInfo> getBucketTable();

  /**
   * Returns the KeyTable.
   *
   * @return KeyTable.
   */
  Table<String, OmKeyInfo> getKeyTable();

  /**
   * Get Deleted Table.
   *
   * @return Deleted Table.
   */
  Table<String, OmKeyInfo> getDeletedTable();

  /**
   * Gets the OpenKeyTable.
   *
   * @return Table.
   */
  Table<String, OmKeyInfo> getOpenKeyTable();

  /**
   * Gets the DelegationTokenTable.
   *
   * @return Table.
   */
  Table<OzoneTokenIdentifier, Long> getDelegationTokenTable();

  /**
   * Gets the S3Bucket to Ozone Volume/bucket mapping table.
   *
   * @return Table.
   */

  Table<byte[], byte[]> getS3Table();

  /**
<<<<<<< HEAD
   * Returns the DB key name of a multipart upload key in OM metadata store.
   *
   * @param volume - volume name
   * @param bucket - bucket name
   * @param key - key name
   * @param uploadId - the upload id for this key
   * @return bytes of DB key.
   */
  String getMultipartKey(String volume, String bucket, String key, String
      uploadId);


  /**
   * Gets the multipart info table which holds the information about
   * multipart upload information of the keys.
   * @return Table
   */
  Table<String, OmMultipartKeyInfo> getMultipartInfoTable();

  /**
   * Gets the S3 Secrets table.
   * @return Table
   */
  Table<byte[], byte[]> getS3SecretTable();

  /**
   * Returns number of rows in a table.  This should not be used for very
   * large tables.
   * @param table
   * @return long
   * @throws IOException
   */
  <KEY, VALUE> long countRowsInTable(Table<KEY, VALUE> table)
      throws IOException;
}
