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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBStoreBuilder;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

/**
 * Ozone metadata manager interface.
 */
public class OmMetadataManagerImpl implements OMMetadataManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmMetadataManagerImpl.class);

  /**
   * OM RocksDB Structure .
   * <p>
   * OM DB stores metadata as KV pairs in different column families.
   * <p>
   * OM DB Schema:
   * |-------------------------------------------------------------------|
   * |  Column Family     |        VALUE                                 |
   * |-------------------------------------------------------------------|
   * | userTable          |     user->VolumeList                         |
   * |-------------------------------------------------------------------|
   * | volumeTable        |     /volume->VolumeInfo                      |
   * |-------------------------------------------------------------------|
   * | bucketTable        |     /volume/bucket-> BucketInfo              |
   * |-------------------------------------------------------------------|
   * | keyTable           | /volumeName/bucketName/keyName->KeyInfo      |
   * |-------------------------------------------------------------------|
   * | deletedTable       | /volumeName/bucketName/keyName->KeyInfo      |
   * |-------------------------------------------------------------------|
   * | openKey            | /volumeName/bucketName/keyName/id->KeyInfo   |
   * |-------------------------------------------------------------------|
   * | s3Table            | s3BucketName -> /volumeName/bucketName       |
   * |-------------------------------------------------------------------|
   */

  private static final String USER_TABLE = "userTable";
  private static final String VOLUME_TABLE = "volumeTable";
  private static final String BUCKET_TABLE = "bucketTable";
  private static final String KEY_TABLE = "keyTable";
  private static final String DELETED_TABLE = "deletedTable";
  private static final String OPEN_KEY_TABLE = "openKeyTable";
  private static final String S3_TABLE = "s3Table";

  private final DBStore store;

  private final OzoneManagerLock lock;
  private final long openKeyExpireThresholdMS;

  private final Table userTable;
  private final Table volumeTable;
  private final Table bucketTable;
  private final Table keyTable;
  private final Table deletedTable;
  private final Table openKeyTable;
  private final Table s3Table;

  public OmMetadataManagerImpl(OzoneConfiguration conf) throws IOException {
    File metaDir = getOzoneMetaDirPath(conf);
    this.lock = new OzoneManagerLock(conf);
    this.openKeyExpireThresholdMS = 1000 * conf.getInt(
        OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS,
        OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT);

    this.store = DBStoreBuilder.newBuilder(conf)
        .setName(OM_DB_NAME)
        .setPath(Paths.get(metaDir.getPath()))
        .addTable(USER_TABLE)
        .addTable(VOLUME_TABLE)
        .addTable(BUCKET_TABLE)
        .addTable(KEY_TABLE)
        .addTable(DELETED_TABLE)
        .addTable(OPEN_KEY_TABLE)
        .addTable(S3_TABLE)
        .build();

    userTable = this.store.getTable(USER_TABLE);
    checkTableStatus(userTable, USER_TABLE);

    volumeTable = this.store.getTable(VOLUME_TABLE);
    checkTableStatus(volumeTable, VOLUME_TABLE);

    bucketTable = this.store.getTable(BUCKET_TABLE);
    checkTableStatus(bucketTable, BUCKET_TABLE);

    keyTable = this.store.getTable(KEY_TABLE);
    checkTableStatus(keyTable, KEY_TABLE);

    deletedTable = this.store.getTable(DELETED_TABLE);
    checkTableStatus(deletedTable, DELETED_TABLE);

    openKeyTable = this.store.getTable(OPEN_KEY_TABLE);
    checkTableStatus(openKeyTable, OPEN_KEY_TABLE);

    s3Table = this.store.getTable(S3_TABLE);
    checkTableStatus(s3Table, S3_TABLE);

  }

  @Override
  public Table getUserTable() {
    return userTable;
  }

  @Override
  public Table getVolumeTable() {
    return volumeTable;
  }

  @Override
  public Table getBucketTable() {
    return bucketTable;
  }

  @Override
  public Table getKeyTable() {
    return keyTable;
  }

  @Override
  public Table getDeletedTable() {
    return deletedTable;
  }

  @Override
  public Table getOpenKeyTable() {
    return openKeyTable;
  }

  @Override
  public Table getS3Table() {
    return s3Table;
  }


  private void checkTableStatus(Table table, String name) throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the logs" +
        "for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
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
  public void stop() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  /**
   * Get metadata store.
   *
   * @return store - metadata store.
   */
  @VisibleForTesting
  @Override
  public DBStore getStore() {
    return store;
  }

  /**
   * Given a volume return the corresponding DB key.
   *
   * @param volume - Volume name
   */
  @Override
  public byte[] getVolumeKey(String volume) {
    return DFSUtil.string2Bytes(OzoneConsts.OM_KEY_PREFIX + volume);
  }

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  @Override
  public byte[] getUserKey(String user) {
    return DFSUtil.string2Bytes(user);
  }

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  @Override
  public byte[] getBucketKey(String volume, String bucket) {
    StringBuilder builder =
        new StringBuilder().append(OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OM_KEY_PREFIX).append(bucket);
    }
    return DFSUtil.string2Bytes(builder.toString());
  }

  @Override
  public byte[] getOzoneKeyBytes(String volume, String bucket, String key) {
    StringBuilder builder = new StringBuilder()
        .append(OM_KEY_PREFIX).append(volume);
    // TODO : Throw if the Bucket is null?
    builder.append(OM_KEY_PREFIX).append(bucket);
    if (StringUtil.isNotBlank(key)) {
      builder.append(OM_KEY_PREFIX).append(key);
    }
    return DFSUtil.string2Bytes(builder.toString());
  }

  @Override
  public byte[] getOpenKeyBytes(String volume, String bucket,
      String key, long id) {
    String openKey = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key + OM_KEY_PREFIX + id;
    return DFSUtil.string2Bytes(openKey);
  }

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  @Override
  public OzoneManagerLock getLock() {
    return lock;
  }

  /**
   * Returns true if the firstArray startsWith the bytes of secondArray.
   *
   * @param firstArray - Byte array
   * @param secondArray - Byte array
   * @return true if the first array bytes match the bytes in the second array.
   */
  private boolean startsWith(byte[] firstArray, byte[] secondArray) {

    if (firstArray == null) {
      // if both are null, then the arrays match, else if first is null and
      // second is not, then this function returns false.
      return secondArray == null;
    }


    if (secondArray != null) {
      // If the second array is longer then first array cannot be starting with
      // the bytes of second array.
      if (secondArray.length > firstArray.length) {
        return false;
      }

      for (int ndx = 0; ndx < secondArray.length; ndx++) {
        if (firstArray[ndx] != secondArray[ndx]) {
          return false;
        }
      }
      return true; //match, return true.
    }
    return false; // if first is not null and second is null, we define that
    // array does not start with same chars.
  }

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   * We iterate in the bucket table and see if there is any key that starts with
   * the volume prefix. We actually look for /volume/, since if we don't have
   * the trailing slash it is possible that we might match some other volume.
   * <p>
   * For example, vol1 and vol122 might match, to avoid that we look for /vol1/
   *
   * @param volume - Volume name
   * @return true if the volume is empty
   */
  @Override
  public boolean isVolumeEmpty(String volume) throws IOException {
    byte[] volumePrefix = getVolumeKey(volume + OM_KEY_PREFIX);
    try (TableIterator<Table.KeyValue> bucketIter = bucketTable.iterator()) {
      Table.KeyValue kv = bucketIter.seek(volumePrefix);
      if (kv != null && startsWith(kv.getKey(), volumePrefix)) {
        return false; // we found at least one bucket with this volume prefix.
      }
    }
    return true;
  }

  /**
   * Given a volume/bucket, check if it is empty, i.e there are no keys inside
   * it. Prefix is /volume/bucket/, and we lookup the keyTable.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  @Override
  public boolean isBucketEmpty(String volume, String bucket)
      throws IOException {
    byte[] keyPrefix = getBucketKey(volume, bucket + OM_KEY_PREFIX);
    try (TableIterator<Table.KeyValue> keyIter = keyTable.iterator()) {
      Table.KeyValue kv = keyIter.seek(keyPrefix);
      if (kv != null && startsWith(kv.getKey(), keyPrefix)) {
        return false; // we found at least one key with this vol/bucket prefix.
      }
    }
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBuckets(final String volumeName,
      final String startBucket, final String bucketPrefix,
      final int maxNumOfBuckets) throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }

    byte[] volumeNameBytes = getVolumeKey(volumeName);
    if (volumeTable.get(volumeNameBytes) == null) {
      throw new OMException("Volume " + volumeName + " not found.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }


    byte[] startKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startBucket)) {
      // if the user has specified a start key, we need to seek to that key
      // and avoid that key in the response set.
      startKey = getBucketKey(volumeName, startBucket);
      skipStartKey = true;
    } else {
      // If the user has specified a prefix key, we need to get to the first
      // of the keys with the prefix match. We can leverage RocksDB to do that.
      // However, if the user has specified only a prefix, we cannot skip
      // the first prefix key we see, the boolean skipStartKey allows us to
      // skip the startkey or not depending on what patterns are specified.
      startKey = getBucketKey(volumeName, bucketPrefix);
    }

    byte[] seekPrefix;
    if (StringUtil.isNotBlank(bucketPrefix)) {
      seekPrefix = getBucketKey(volumeName, bucketPrefix);
    } else {
      seekPrefix = getVolumeKey(volumeName + OM_KEY_PREFIX);
    }
    int currentCount = 0;
    try (TableIterator<Table.KeyValue> bucketIter = bucketTable.iterator()) {
      Table.KeyValue kv = bucketIter.seek(startKey);
      while (currentCount < maxNumOfBuckets && bucketIter.hasNext()) {
        kv = bucketIter.next();
        // Skip the Start Bucket if needed.
        if (kv != null && skipStartKey &&
            Arrays.equals(kv.getKey(), startKey)) {
          continue;
        }
        if (kv != null && startsWith(kv.getKey(), seekPrefix)) {
          result.add(OmBucketInfo.getFromProtobuf(
              BucketInfo.parseFrom(kv.getValue())));
          currentCount++;
        } else {
          // The SeekPrefix does not match any more, we can break out of the
          // loop.
          break;
        }
      }
    }
    return result;
  }

  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {
    List<OmKeyInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.",
          ResultCodes.FAILED_VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new OMException("Bucket name is required.",
          ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }

    byte[] bucketNameBytes = getBucketKey(volumeName, bucketName);
    if (getBucketTable().get(bucketNameBytes) == null) {
      throw new OMException("Bucket " + bucketName + " not found.",
          ResultCodes.FAILED_BUCKET_NOT_FOUND);
    }

    byte[] seekKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startKey)) {
      // Seek to the specified key.
      seekKey = getOzoneKeyBytes(volumeName, bucketName, startKey);
      skipStartKey = true;
    } else {
      // This allows us to seek directly to the first key with the right prefix.
      seekKey = getOzoneKeyBytes(volumeName, bucketName, keyPrefix);
    }

    byte[] seekPrefix;
    if (StringUtil.isNotBlank(keyPrefix)) {
      seekPrefix = getOzoneKeyBytes(volumeName, bucketName, keyPrefix);
    } else {
      seekPrefix = getBucketKey(volumeName, bucketName + OM_KEY_PREFIX);
    }
    int currentCount = 0;
    try (TableIterator<Table.KeyValue> keyIter = getKeyTable().iterator()) {
      Table.KeyValue kv = keyIter.seek(seekKey);
      while (currentCount < maxKeys && keyIter.hasNext()) {
        kv = keyIter.next();
        // Skip the Start key if needed.
        if (kv != null && skipStartKey && Arrays.equals(kv.getKey(), seekKey)) {
          continue;
        }
        if (kv != null && startsWith(kv.getKey(), seekPrefix)) {
          result.add(OmKeyInfo.getFromProtobuf(
              KeyInfo.parseFrom(kv.getValue())));
          currentCount++;
        } else {
          // The SeekPrefix does not match any more, we can break out of the
          // loop.
          break;
        }
      }
    }
    return result;
  }

  @Override
  public List<OmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {
    List<OmVolumeArgs> result = Lists.newArrayList();
    VolumeList volumes;
    if (StringUtil.isBlank(userName)) {
      throw new OMException("User name is required to list Volumes.",
          ResultCodes.FAILED_USER_NOT_FOUND);
    }
    volumes = getVolumesByUser(userName);

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
        byte[] volumeInfo = getVolumeTable().get(this.getVolumeKey(volumeName));
        if (volumeInfo == null) {
          // Could not get volume info by given volume name,
          // since the volume name is loaded from db,
          // this probably means om db is corrupted or some entries are
          // accidentally removed.
          throw new OMException("Volume info not found for " + volumeName,
              ResultCodes.FAILED_VOLUME_NOT_FOUND);
        }
        VolumeInfo info = VolumeInfo.parseFrom(volumeInfo);
        OmVolumeArgs volumeArgs = OmVolumeArgs.getFromProtobuf(info);
        result.add(volumeArgs);
      }
    }

    return result;
  }

  private VolumeList getVolumesByUser(String userName)
      throws OMException {
    return getVolumesByUser(getUserKey(userName));
  }

  private VolumeList getVolumesByUser(byte[] userNameKey)
      throws OMException {
    VolumeList volumes = null;
    try {
      byte[] volumesInBytes = getUserTable().get(userNameKey);
      if (volumesInBytes == null) {
        // No volume found for this user, return an empty list
        return VolumeList.newBuilder().build();
      }
      volumes = VolumeList.parseFrom(volumesInBytes);
    } catch (IOException e) {
      throw new OMException("Unable to get volumes info by the given user, "
          + "metadata might be corrupted", e,
          ResultCodes.FAILED_METADATA_ERROR);
    }
    return volumes;
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int keyCount)
      throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    try (TableIterator<Table.KeyValue> keyIter = getDeletedTable().iterator()) {
      int currentCount = 0;
      while (keyIter.hasNext() && currentCount < keyCount) {
        Table.KeyValue kv = keyIter.next();
        if (kv != null) {
          OmKeyInfo info =
              OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(kv.getValue()));
          // Get block keys as a list.
          OmKeyLocationInfoGroup latest = info.getLatestVersionLocations();
          List<BlockID> item = latest.getLocationList().stream()
              .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
              .collect(Collectors.toList());
          BlockGroup keyBlocks = BlockGroup.newBuilder()
              .setKeyName(DFSUtil.bytes2String(kv.getKey()))
              .addAllBlockIDs(item)
              .build();
          keyBlocksList.add(keyBlocks);
          currentCount++;
        }
      }
    }
    return keyBlocksList;
  }

  @Override
  public List<BlockGroup> getExpiredOpenKeys() throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    long now = Time.now();
    // TODO: Fix the getExpiredOpenKeys, Not part of this patch.
    List<Map.Entry<byte[], byte[]>> rangeResult = Collections.emptyList();

    for (Map.Entry<byte[], byte[]> entry : rangeResult) {
      OmKeyInfo info =
          OmKeyInfo.getFromProtobuf(KeyInfo.parseFrom(entry.getValue()));
      long lastModify = info.getModificationTime();
      if (now - lastModify < this.openKeyExpireThresholdMS) {
        // consider as may still be active, not hanging.
        continue;
      }
      // Get block keys as a list.
      List<BlockID> item = info.getLatestVersionLocations()
          .getBlocksLatestVersionOnly().stream()
          .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
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
