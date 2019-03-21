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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.codec.OmBucketInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmMultipartKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmVolumeArgsCodec;
import org.apache.hadoop.ozone.om.codec.TokenIdentifierCodec;
import org.apache.hadoop.ozone.om.codec.VolumeListCodec;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeList;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.DBStoreBuilder;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.Table.KeyValue;
import org.apache.hadoop.utils.db.TableIterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * | s3SecretTable      | s3g_access_key_id -> s3Secret                |
   * |-------------------------------------------------------------------|
   * | dTokenTable        | s3g_access_key_id -> s3Secret                |
   * |-------------------------------------------------------------------|
   */

  private static final String USER_TABLE = "userTable";
  private static final String VOLUME_TABLE = "volumeTable";
  private static final String BUCKET_TABLE = "bucketTable";
  private static final String KEY_TABLE = "keyTable";
  private static final String DELETED_TABLE = "deletedTable";
  private static final String OPEN_KEY_TABLE = "openKeyTable";
  private static final String S3_TABLE = "s3Table";
  private static final String MULTIPARTINFO_TABLE = "multipartInfoTable";
  private static final String S3_SECRET_TABLE = "s3SecretTable";
  private static final String DELEGATION_TOKEN_TABLE = "dTokenTable";

  private DBStore store;

  private final OzoneManagerLock lock;
  private final long openKeyExpireThresholdMS;

  private Table userTable;
  private Table volumeTable;
  private Table bucketTable;
  private Table keyTable;
  private Table deletedTable;
  private Table openKeyTable;
  private Table s3Table;
  private Table<String, OmMultipartKeyInfo> multipartInfoTable;
  private Table s3SecretTable;
  private Table dTokenTable;

  public OmMetadataManagerImpl(OzoneConfiguration conf) throws IOException {
    this.lock = new OzoneManagerLock(conf);
    this.openKeyExpireThresholdMS = 1000L * conf.getInt(
        OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS,
        OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT);
    start(conf);
  }

  /**
   * For subclass overriding.
   */
  protected OmMetadataManagerImpl() {
    this.lock = new OzoneManagerLock(new OzoneConfiguration());
    this.openKeyExpireThresholdMS =
        OZONE_OPEN_KEY_EXPIRE_THRESHOLD_SECONDS_DEFAULT;
  }

  @Override
  public Table<String, VolumeList> getUserTable() {
    return userTable;
  }

  public Table<OzoneTokenIdentifier, Long> getDelegationTokenTable() {
    return dTokenTable;
  }

  @Override
  public Table<String, OmVolumeArgs> getVolumeTable() {
    return volumeTable;
  }

  @Override
  public Table<String, OmBucketInfo> getBucketTable() {
    return bucketTable;
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable() {
    return keyTable;
  }

  @Override
  public Table<String, OmKeyInfo> getDeletedTable() {
    return deletedTable;
  }

  @Override
  public Table<String, OmKeyInfo> getOpenKeyTable() {
    return openKeyTable;
  }

  @Override
  public Table<byte[], byte[]> getS3Table() {
    return s3Table;
  }

  @Override
  public Table<String, OmMultipartKeyInfo> getMultipartInfoTable() {
    return multipartInfoTable;
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
  public void start(OzoneConfiguration configuration) throws IOException {
    // We need to create the DB here, as when during restart, stop closes the
    // db, so we need to create the store object and initialize DB.
    if (store == null) {
      File metaDir = OmUtils.getOmDbDir(configuration);

      DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(configuration)
          .setName(OM_DB_NAME)
          .setPath(Paths.get(metaDir.getPath()));
      this.store = addOMTablesAndCodecs(dbStoreBuilder).build();
      initializeOmTables();
    }
  }

  protected DBStoreBuilder addOMTablesAndCodecs(DBStoreBuilder builder) {

    return builder.addTable(USER_TABLE)
        .addTable(VOLUME_TABLE)
        .addTable(BUCKET_TABLE)
        .addTable(KEY_TABLE)
        .addTable(DELETED_TABLE)
        .addTable(OPEN_KEY_TABLE)
        .addTable(S3_TABLE)
        .addTable(MULTIPARTINFO_TABLE)
        .addTable(DELEGATION_TOKEN_TABLE)
        .addTable(S3_SECRET_TABLE)
        .addCodec(OzoneTokenIdentifier.class, new TokenIdentifierCodec())
        .addCodec(OmKeyInfo.class, new OmKeyInfoCodec())
        .addCodec(OmBucketInfo.class, new OmBucketInfoCodec())
        .addCodec(OmVolumeArgs.class, new OmVolumeArgsCodec())
        .addCodec(VolumeList.class, new VolumeListCodec())
        .addCodec(OmMultipartKeyInfo.class, new OmMultipartKeyInfoCodec());
  }

  /**
   * Initialize OM Tables.
   *
   * @throws IOException
   */
  protected void initializeOmTables() throws IOException {
    userTable =
        this.store.getTable(USER_TABLE, String.class, VolumeList.class);
    checkTableStatus(userTable, USER_TABLE);
    this.store.getTable(VOLUME_TABLE, String.class,
        String.class);
    volumeTable =
        this.store.getTable(VOLUME_TABLE, String.class, OmVolumeArgs.class);
    checkTableStatus(volumeTable, VOLUME_TABLE);

    bucketTable =
        this.store.getTable(BUCKET_TABLE, String.class, OmBucketInfo.class);
    checkTableStatus(bucketTable, BUCKET_TABLE);

    keyTable = this.store.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
    checkTableStatus(keyTable, KEY_TABLE);

    deletedTable =
        this.store.getTable(DELETED_TABLE, String.class, OmKeyInfo.class);
    checkTableStatus(deletedTable, DELETED_TABLE);

    openKeyTable =
        this.store.getTable(OPEN_KEY_TABLE, String.class, OmKeyInfo.class);
    checkTableStatus(openKeyTable, OPEN_KEY_TABLE);

    s3Table = this.store.getTable(S3_TABLE);
    checkTableStatus(s3Table, S3_TABLE);

    multipartInfoTable = this.store.getTable(MULTIPARTINFO_TABLE,
        String.class, OmMultipartKeyInfo.class);
    checkTableStatus(multipartInfoTable, MULTIPARTINFO_TABLE);

    dTokenTable = this.store.getTable(DELEGATION_TOKEN_TABLE,
        OzoneTokenIdentifier.class, Long.class);
    checkTableStatus(dTokenTable, DELEGATION_TOKEN_TABLE);

    s3SecretTable = this.store.getTable(S3_SECRET_TABLE);
    checkTableStatus(s3SecretTable, S3_SECRET_TABLE);
  }

  /**
   * Stop metadata manager.
   */
  @Override
  public void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
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
  public String getVolumeKey(String volume) {
    return OzoneConsts.OM_KEY_PREFIX + volume;
  }

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  @Override
  public String getUserKey(String user) {
    return user;
  }

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - User name
   * @param bucket - Bucket name
   */
  @Override
  public String getBucketKey(String volume, String bucket) {
    StringBuilder builder =
        new StringBuilder().append(OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OM_KEY_PREFIX).append(bucket);
    }
    return builder.toString();
  }

  @Override
  public String getOzoneKey(String volume, String bucket, String key) {
    StringBuilder builder = new StringBuilder()
        .append(OM_KEY_PREFIX).append(volume);
    // TODO : Throw if the Bucket is null?
    builder.append(OM_KEY_PREFIX).append(bucket);
    if (StringUtil.isNotBlank(key)) {
      builder.append(OM_KEY_PREFIX).append(key);
    }
    return builder.toString();
  }

  @Override
  public String getOpenKey(String volume, String bucket,
                           String key, long id) {
    String openKey = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key + OM_KEY_PREFIX + id;
    return openKey;
  }

  @Override
  public String getMultipartKey(String volume, String bucket, String key,
                                String
                                    uploadId) {
    String multipartKey =  OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key + OM_KEY_PREFIX + uploadId;
    return multipartKey;
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
    String volumePrefix = getVolumeKey(volume + OM_KEY_PREFIX);

    try (TableIterator<String, ? extends KeyValue<String, OmBucketInfo>>
        bucketIter = bucketTable.iterator()) {
      KeyValue<String, OmBucketInfo> kv = bucketIter.seek(volumePrefix);
      if (kv != null && kv.getKey().startsWith(volumePrefix)) {
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
    String keyPrefix = getBucketKey(volume, bucket);
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> keyIter =
        keyTable.iterator()) {
      KeyValue<String, OmKeyInfo> kv = keyIter.seek(keyPrefix);
      if (kv != null && kv.getKey().startsWith(keyPrefix)) {
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
          ResultCodes.VOLUME_NOT_FOUND);
    }

    String volumeNameBytes = getVolumeKey(volumeName);
    if (volumeTable.get(volumeNameBytes) == null) {
      throw new OMException("Volume " + volumeName + " not found.",
          ResultCodes.VOLUME_NOT_FOUND);
    }

    String startKey;
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

    String seekPrefix;
    if (StringUtil.isNotBlank(bucketPrefix)) {
      seekPrefix = getBucketKey(volumeName, bucketPrefix);
    } else {
      seekPrefix = getVolumeKey(volumeName + OM_KEY_PREFIX);
    }
    int currentCount = 0;

    try (TableIterator<String, ? extends KeyValue<String, OmBucketInfo>>
        bucketIter = bucketTable.iterator()) {
      KeyValue<String, OmBucketInfo> kv = bucketIter.seek(startKey);
      while (currentCount < maxNumOfBuckets && bucketIter.hasNext()) {
        kv = bucketIter.next();
        // Skip the Start Bucket if needed.
        if (kv != null && skipStartKey &&
            kv.getKey().equals(startKey)) {
          continue;
        }
        if (kv != null && kv.getKey().startsWith(seekPrefix)) {
          result.add(kv.getValue());
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
          ResultCodes.VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new OMException("Bucket name is required.",
          ResultCodes.BUCKET_NOT_FOUND);
    }

    String bucketNameBytes = getBucketKey(volumeName, bucketName);
    if (getBucketTable().get(bucketNameBytes) == null) {
      throw new OMException("Bucket " + bucketName + " not found.",
          ResultCodes.BUCKET_NOT_FOUND);
    }

    String seekKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startKey)) {
      // Seek to the specified key.
      seekKey = getOzoneKey(volumeName, bucketName, startKey);
      skipStartKey = true;
    } else {
      // This allows us to seek directly to the first key with the right prefix.
      seekKey = getOzoneKey(volumeName, bucketName, keyPrefix);
    }

    String seekPrefix;
    if (StringUtil.isNotBlank(keyPrefix)) {
      seekPrefix = getOzoneKey(volumeName, bucketName, keyPrefix);
    } else {
      seekPrefix = getBucketKey(volumeName, bucketName + OM_KEY_PREFIX);
    }
    int currentCount = 0;
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> keyIter =
        getKeyTable()
            .iterator()) {
      KeyValue<String, OmKeyInfo> kv = keyIter.seek(seekKey);
      while (currentCount < maxKeys && keyIter.hasNext()) {
        kv = keyIter.next();
        // Skip the Start key if needed.
        if (kv != null && skipStartKey && kv.getKey().equals(seekKey)) {
          continue;
        }
        if (kv != null && kv.getKey().startsWith(seekPrefix)) {
          result.add(kv.getValue());
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
          ResultCodes.USER_NOT_FOUND);
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
        OmVolumeArgs volumeArgs =
            getVolumeTable().get(this.getVolumeKey(volumeName));
        if (volumeArgs == null) {
          // Could not get volume info by given volume name,
          // since the volume name is loaded from db,
          // this probably means om db is corrupted or some entries are
          // accidentally removed.
          throw new OMException("Volume info not found for " + volumeName,
              ResultCodes.VOLUME_NOT_FOUND);
        }
        result.add(volumeArgs);
      }
    }

    return result;
  }

  private VolumeList getVolumesByUser(String userNameKey)
      throws OMException {
    try {
      VolumeList volumeList = getUserTable().get(userNameKey);
      if (volumeList == null) {
        // No volume found for this user, return an empty list
        return VolumeList.newBuilder().build();
      } else {
        return volumeList;
      }
    } catch (IOException e) {
      throw new OMException("Unable to get volumes info by the given user, "
          + "metadata might be corrupted", e,
          ResultCodes.METADATA_ERROR);
    }
  }

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int keyCount)
      throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>> keyIter =
        getDeletedTable()
            .iterator()) {
      int currentCount = 0;
      while (keyIter.hasNext() && currentCount < keyCount) {
        KeyValue<String, OmKeyInfo> kv = keyIter.next();
        if (kv != null) {
          OmKeyInfo info = kv.getValue();
          // Get block keys as a list.
          OmKeyLocationInfoGroup latest = info.getLatestVersionLocations();
          List<BlockID> item = latest.getLocationList().stream()
              .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
              .collect(Collectors.toList());
          BlockGroup keyBlocks = BlockGroup.newBuilder()
              .setKeyName(kv.getKey())
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
    // TODO: Fix the getExpiredOpenKeys, Not part of this patch.
    return keyBlocksList;
  }

  @Override
  public <KEY, VALUE> long countRowsInTable(Table<KEY, VALUE> table)
      throws IOException {
    long count = 0;
    if (table != null) {
      try (TableIterator<KEY, ? extends KeyValue<KEY, VALUE>>
          keyValueTableIterator = table.iterator()) {
        while (keyValueTableIterator.hasNext()) {
          keyValueTableIterator.next();
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public Table<byte[], byte[]> getS3SecretTable() {
    return s3SecretTable;
  }

  /**
   * Update store used by subclass.
   *
   * @param store DB store.
   */
  protected void setStore(DBStore store) {
    this.store = store;
  }

}
