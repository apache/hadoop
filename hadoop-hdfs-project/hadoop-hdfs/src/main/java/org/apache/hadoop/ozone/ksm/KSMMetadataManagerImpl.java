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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmBucketInfo;
import org.apache.hadoop.ozone.ksm.helpers.KsmVolumeArgs;
import org.apache.hadoop.ozone.ksm.helpers.KsmKeyLocationInfo;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException;
import org.apache.hadoop.ozone.ksm.exceptions.KSMException.ResultCodes;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.KeySpaceManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataKeyFilters.KeyPrefixFilter;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.OzoneConsts.DELETING_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.KSM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OPEN_KEY_ID_DELIMINATOR;
import static org.apache.hadoop.ozone.OzoneConsts.OPEN_KEY_PREFIX;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_DB_CACHE_SIZE_MB;

/**
 * KSM metadata manager interface.
 */
public class KSMMetadataManagerImpl implements KSMMetadataManager {

  private final MetadataStore store;
  private final ReadWriteLock lock;


  public KSMMetadataManagerImpl(OzoneConfiguration conf) throws IOException {
    File metaDir = OzoneUtils.getScmMetadirPath(conf);
    final int cacheSize = conf.getInt(OZONE_KSM_DB_CACHE_SIZE_MB,
        OZONE_KSM_DB_CACHE_SIZE_DEFAULT);
    File ksmDBFile = new File(metaDir.getPath(), KSM_DB_NAME);
    this.store = MetadataStoreBuilder.newBuilder()
        .setConf(conf)
        .setDbFile(ksmDBFile)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Start metadata manager.
   */
  @Override
  public void start() {

  }

  /**
   * Stop metadata manager.
   */
  @Override
  public void stop() throws IOException {
    if (store != null) {
      store.close();
    }
  }

  /**
   * Get metadata store.
   * @return store - metadata store.
   */
  @VisibleForTesting
  @Override
  public MetadataStore getStore() {
    return store;
  }

  /**
   * Given a volume return the corresponding DB key.
   * @param volume - Volume name
   */
  public byte[] getVolumeKey(String volume) {
    String dbVolumeName = OzoneConsts.KSM_VOLUME_PREFIX + volume;
    return DFSUtil.string2Bytes(dbVolumeName);
  }

  /**
   * Given a user return the corresponding DB key.
   * @param user - User name
   */
  public byte[] getUserKey(String user) {
    String dbUserName = OzoneConsts.KSM_USER_PREFIX + user;
    return DFSUtil.string2Bytes(dbUserName);
  }

  /**
   * Given a volume and bucket, return the corresponding DB key.
   * @param volume - User name
   * @param bucket - Bucket name
   */
  public byte[] getBucketKey(String volume, String bucket) {
    String bucketKeyString = OzoneConsts.KSM_VOLUME_PREFIX + volume
        + OzoneConsts.KSM_BUCKET_PREFIX + bucket;
    return DFSUtil.string2Bytes(bucketKeyString);
  }

  /**
   * @param volume
   * @param bucket
   * @return
   */
  private String getBucketWithDBPrefix(String volume, String bucket) {
    StringBuffer sb = new StringBuffer();
    sb.append(OzoneConsts.KSM_VOLUME_PREFIX)
        .append(volume)
        .append(OzoneConsts.KSM_BUCKET_PREFIX);
    if (!Strings.isNullOrEmpty(bucket)) {
      sb.append(bucket);
    }
    return sb.toString();
  }

  @Override
  public String getKeyWithDBPrefix(String volume, String bucket, String key) {
    String keyVB = OzoneConsts.KSM_KEY_PREFIX + volume
        + OzoneConsts.KSM_KEY_PREFIX + bucket
        + OzoneConsts.KSM_KEY_PREFIX;
    return Strings.isNullOrEmpty(key) ? keyVB : keyVB + key;
  }

  @Override
  public byte[] getDBKeyBytes(String volume, String bucket, String key) {
    return DFSUtil.string2Bytes(getKeyWithDBPrefix(volume, bucket, key));
  }

  @Override
  public byte[] getDeletedKeyName(byte[] keyName) {
    return DFSUtil.string2Bytes(
        DELETING_KEY_PREFIX + DFSUtil.bytes2String(keyName));
  }

  @Override
  public byte[] getOpenKeyNameBytes(String keyName, int id) {
    return DFSUtil.string2Bytes(OPEN_KEY_PREFIX + id +
        OPEN_KEY_ID_DELIMINATOR + keyName);
  }

  /**
   * Returns the read lock used on Metadata DB.
   * @return readLock
   */
  @Override
  public Lock readLock() {
    return lock.readLock();
  }

  /**
   * Returns the write lock used on Metadata DB.
   * @return writeLock
   */
  @Override
  public Lock writeLock() {
    return lock.writeLock();
  }

  /**
   * Returns the value associated with this key.
   * @param key - key
   * @return value
   */
  @Override
  public byte[] get(byte[] key) throws IOException {
    return store.get(key);
  }

  /**
   * Puts a Key into Metadata DB.
   * @param key   - key
   * @param value - value
   */
  @Override
  public void put(byte[] key, byte[] value) throws IOException {
    store.put(key, value);
  }

  /**
   * Deletes a Key from Metadata DB.
   * @param key   - key
   */
  public void delete(byte[] key) throws IOException {
    store.delete(key);
  }

  @Override
  public void writeBatch(BatchOperation batch) throws IOException {
    this.store.writeBatch(batch);
  }

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   * @param volume - Volume name
   * @return true if the volume is empty
   */
  public boolean isVolumeEmpty(String volume) throws IOException {
    String dbVolumeRootName = OzoneConsts.KSM_VOLUME_PREFIX + volume
        + OzoneConsts.KSM_BUCKET_PREFIX;
    byte[] dbVolumeRootKey = DFSUtil.string2Bytes(dbVolumeRootName);
    ImmutablePair<byte[], byte[]> volumeRoot =
        store.peekAround(0, dbVolumeRootKey);
    if (volumeRoot != null) {
      return !DFSUtil.bytes2String(volumeRoot.getKey())
          .startsWith(dbVolumeRootName);
    }
    return true;
  }

  /**
   * Given a volume/bucket, check if it is empty,
   * i.e there are no keys inside it.
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  public boolean isBucketEmpty(String volume, String bucket)
      throws IOException {
    String keyRootName = getKeyWithDBPrefix(volume, bucket, null);
    byte[] keyRoot = DFSUtil.string2Bytes(keyRootName);
    ImmutablePair<byte[], byte[]> firstKey = store.peekAround(0, keyRoot);
    if (firstKey != null) {
      return !DFSUtil.bytes2String(firstKey.getKey())
          .startsWith(keyRootName);
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<KsmBucketInfo> listBuckets(final String volumeName,
      final String startBucket, final String bucketPrefix,
      final int maxNumOfBuckets) throws IOException {
    List<KsmBucketInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new KSMException("Volume name is required.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }

    byte[] volumeNameBytes = getVolumeKey(volumeName);
    if (store.get(volumeNameBytes) == null) {
      throw new KSMException("Volume " + volumeName + " not found.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }


    // A bucket starts with /#volume/#bucket_prefix
    MetadataKeyFilter filter = (preKey, currentKey, nextKey) -> {
      if (currentKey != null) {
        String bucketNamePrefix =
                getBucketWithDBPrefix(volumeName, bucketPrefix);
        String bucket = DFSUtil.bytes2String(currentKey);
        return bucket.startsWith(bucketNamePrefix);
      }
      return false;
    };

    List<Map.Entry<byte[], byte[]>> rangeResult;
    if (!Strings.isNullOrEmpty(startBucket)) {
      // Since we are excluding start key from the result,
      // the maxNumOfBuckets is incremented.
      rangeResult = store.getSequentialRangeKVs(
          getBucketKey(volumeName, startBucket),
          maxNumOfBuckets + 1, filter);
      //Remove start key from result.
      rangeResult.remove(0);
    } else {
      rangeResult = store.getSequentialRangeKVs(null, maxNumOfBuckets, filter);
    }

    for (Map.Entry<byte[], byte[]> entry : rangeResult) {
      KsmBucketInfo info = KsmBucketInfo.getFromProtobuf(
          BucketInfo.parseFrom(entry.getValue()));
      result.add(info);
    }
    return result;
  }

  @Override
  public List<KsmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    List<KsmKeyInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new KSMException("Volume name is required.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new KSMException("Bucket name is required.",
          ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }

    byte[] bucketNameBytes = getBucketKey(volumeName, bucketName);
    if (store.get(bucketNameBytes) == null) {
      throw new KSMException("Bucket " + bucketName + " not found.",
          ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }

    MetadataKeyFilter filter = new KeyPrefixFilter(
                getKeyWithDBPrefix(volumeName, bucketName, keyPrefix));

    List<Map.Entry<byte[], byte[]>> rangeResult;
    if (!Strings.isNullOrEmpty(startKey)) {
      //Since we are excluding start key from the result,
      // the maxNumOfBuckets is incremented.
      rangeResult = store.getSequentialRangeKVs(
          getDBKeyBytes(volumeName, bucketName, startKey),
          maxKeys + 1, filter);
      //Remove start key from result.
      rangeResult.remove(0);
    } else {
      rangeResult = store.getSequentialRangeKVs(null, maxKeys, filter);
    }

    for (Map.Entry<byte[], byte[]> entry : rangeResult) {
      KsmKeyInfo info = KsmKeyInfo.getFromProtobuf(
          KeyInfo.parseFrom(entry.getValue()));
      result.add(info);
    }
    return result;
  }

  @Override
  public List<KsmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    List<KsmVolumeArgs> result = Lists.newArrayList();
    VolumeList volumes;
    if (Strings.isNullOrEmpty(userName)) {
      volumes = getAllVolumes();
    } else {
      volumes = getVolumesByUser(userName);
    }

    if (volumes == null || volumes.getVolumeNamesCount() == 0) {
      return result;
    }

    boolean startKeyFound = Strings.isNullOrEmpty(startKey);
    for (String volumeName : volumes.getVolumeNamesList()) {
      if (!Strings.isNullOrEmpty(prefix)) {
        if (!volumeName.startsWith(prefix)) {
          continue;
        }
      }

      if (!startKeyFound && volumeName.equals(startKey)) {
        startKeyFound = true;
        continue;
      }
      if (startKeyFound && result.size() < maxKeys) {
        byte[] volumeInfo = store.get(this.getVolumeKey(volumeName));
        if (volumeInfo == null) {
          // Could not get volume info by given volume name,
          // since the volume name is loaded from db,
          // this probably means ksm db is corrupted or some entries are
          // accidentally removed.
          throw new KSMException("Volume info not found for " + volumeName,
              ResultCodes.FAILED_VOLUME_NOT_FOUND);
        }
        VolumeInfo info = VolumeInfo.parseFrom(volumeInfo);
        KsmVolumeArgs volumeArgs = KsmVolumeArgs.getFromProtobuf(info);
        result.add(volumeArgs);
      }
    }

    return result;
  }

  private VolumeList getVolumesByUser(String userName)
      throws KSMException {
    return getVolumesByUser(getUserKey(userName));
  }

  private VolumeList getVolumesByUser(byte[] userNameKey)
      throws KSMException {
    VolumeList volumes = null;
    try {
      byte[] volumesInBytes = store.get(userNameKey);
      if (volumesInBytes == null) {
        // No volume found for this user, return an empty list
        return VolumeList.newBuilder().build();
      }
      volumes = VolumeList.parseFrom(volumesInBytes);
    } catch (IOException e) {
      throw new KSMException("Unable to get volumes info by the given user, "
          + "metadata might be corrupted", e,
          ResultCodes.FAILED_METADATA_ERROR);
    }
    return volumes;
  }

  private VolumeList getAllVolumes() throws IOException {
    // Scan all users in database
    KeyPrefixFilter filter = new KeyPrefixFilter(OzoneConsts.KSM_USER_PREFIX);
    // We are not expecting a huge number of users per cluster,
    // it should be fine to scan all users in db and return us a
    // list of volume names in string per user.
    List<Map.Entry<byte[], byte[]>> rangeKVs = store
        .getSequentialRangeKVs(null, Integer.MAX_VALUE, filter);

    VolumeList.Builder builder = VolumeList.newBuilder();
    for (Map.Entry<byte[], byte[]> entry : rangeKVs) {
      VolumeList volumes = this.getVolumesByUser(entry.getKey());
      builder.addAllVolumeNames(volumes.getVolumeNamesList());
    }

    return builder.build();
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int count)
      throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    final MetadataKeyFilter deletingKeyFilter =
        new KeyPrefixFilter(DELETING_KEY_PREFIX);
    List<Map.Entry<byte[], byte[]>> rangeResult =
        store.getSequentialRangeKVs(null, count, deletingKeyFilter);
    for (Map.Entry<byte[], byte[]> entry : rangeResult) {
      KsmKeyInfo info =
          KsmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(entry.getValue()));
      // Get block keys as a list.
      List<String> item = info.getKeyLocationList().stream()
          .map(KsmKeyLocationInfo::getBlockID)
          .collect(Collectors.toList());
      BlockGroup keyBlocks = BlockGroup.newBuilder()
          .setKeyName(DFSUtil.bytes2String(entry.getKey()))
          .addAllBlockIDs(item)
          .build();
      keyBlocksList.add(keyBlocks);
    }
    return keyBlocksList;
  }
}
