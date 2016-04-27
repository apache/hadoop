/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.timeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * LevelDB implementation of {@link KeyValueBasedTimelineStore}. This
 * implementation stores the entity hash map into a LevelDB instance.
 * There are two partitions of the key space. One partition is to store a
 * entity id to start time mapping:
 *
 * i!ENTITY_ID!ENTITY_TYPE to ENTITY_START_TIME
 *
 * The other partition is to store the actual data:
 *
 * e!START_TIME!ENTITY_ID!ENTITY_TYPE to ENTITY_BYTES
 *
 * This storage does not have any garbage collection mechanism, and is designed
 * mainly for caching usages.
 */
@Private
@Unstable
public class LevelDBCacheTimelineStore extends KeyValueBasedTimelineStore {
  private static final Log LOG
      = LogFactory.getLog(LevelDBCacheTimelineStore.class);
  private static final String CACHED_LDB_FILE_PREFIX = "-timeline-cache.ldb";
  private String dbId;
  private DB entityDb;
  private Configuration configuration;

  public LevelDBCacheTimelineStore(String id, String name) {
    super(name);
    dbId = id;
    entityInsertTimes = new MemoryTimelineStore.HashMapStoreAdapter<>();
    domainById = new MemoryTimelineStore.HashMapStoreAdapter<>();
    domainsByOwner = new MemoryTimelineStore.HashMapStoreAdapter<>();
  }

  public LevelDBCacheTimelineStore(String id) {
    this(id, LevelDBCacheTimelineStore.class.getName());
  }

  @Override
  protected synchronized void serviceInit(Configuration conf) throws Exception {
    configuration = conf;
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_LEVELDB_CACHE_READ_CACHE_SIZE));
    JniDBFactory factory = new JniDBFactory();
    Path dbPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        dbId + CACHED_LDB_FILE_PREFIX);
    FileSystem localFS = null;

    try {
      localFS = FileSystem.getLocal(conf);
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb " +
              "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LeveldbUtils.LEVELDB_DIR_UMASK);
      }
    } finally {
      IOUtils.cleanup(LOG, localFS);
    }
    LOG.info("Using leveldb path " + dbPath);
    entityDb = factory.open(new File(dbPath.toString()), options);
    entities = new LevelDBMapAdapter<>(entityDb);

    super.serviceInit(conf);
  }

  @Override
  protected synchronized void serviceStop() throws Exception {
    IOUtils.cleanup(LOG, entityDb);
    Path dbPath = new Path(
        configuration.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH),
        dbId + CACHED_LDB_FILE_PREFIX);
    FileSystem localFS = null;
    try {
      localFS = FileSystem.getLocal(configuration);
      if (!localFS.delete(dbPath, true)) {
          throw new IOException("Couldn't delete data file for leveldb " +
              "timeline store " + dbPath);
      }
    } finally {
      IOUtils.cleanup(LOG, localFS);
    }
    super.serviceStop();
  }

  /**
   * A specialized hash map storage that uses LevelDB for storing entity id to
   * entity mappings.
   *
   * @param <K> an {@link EntityIdentifier} typed hash key
   * @param <V> a {@link TimelineEntity} typed value
   */
  static class LevelDBMapAdapter<K extends EntityIdentifier,
      V extends TimelineEntity> implements TimelineStoreMapAdapter<K, V> {
    private static final String TIME_INDEX_PREFIX = "i";
    private static final String ENTITY_STORAGE_PREFIX = "e";
    DB entityDb;

    public LevelDBMapAdapter(DB currLevelDb) {
      entityDb = currLevelDb;
    }

    @Override
    public V get(K entityId) {
      V result = null;
      // Read the start time from the index
      byte[] startTimeBytes = entityDb.get(getStartTimeKey(entityId));
      if (startTimeBytes == null) {
        return null;
      }

      // Build the key for the entity storage and read it
      try {
        result = getEntityForKey(getEntityKey(entityId, startTimeBytes));
      } catch (IOException e) {
        LOG.error("GenericObjectMapper cannot read key from key "
            + entityId.toString()
            + " into an object. Read aborted! ");
        LOG.error(e.getMessage());
      }

      return result;
    }

    @Override
    public void put(K entityId, V entity) {
      Long startTime = entity.getStartTime();
      if (startTime == null) {
        startTime = System.currentTimeMillis();
      }
      // Build the key for the entity storage and read it
      byte[] startTimeBytes = GenericObjectMapper.writeReverseOrderedLong(
          startTime);
      try {
        byte[] valueBytes = GenericObjectMapper.write(entity);
        entityDb.put(getEntityKey(entityId, startTimeBytes), valueBytes);
      } catch (IOException e) {
        LOG.error("GenericObjectMapper cannot write "
            + entity.getClass().getName()
            + " into a byte array. Write aborted! ");
        LOG.error(e.getMessage());
      }

      // Build the key for the start time index
      entityDb.put(getStartTimeKey(entityId), startTimeBytes);
    }

    @Override
    public void remove(K entityId) {
      // Read the start time from the index (key starts with an "i") then delete
      // the record
      LeveldbUtils.KeyBuilder startTimeKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      startTimeKeyBuilder.add(TIME_INDEX_PREFIX).add(entityId.getId())
          .add(entityId.getType());
      byte[] startTimeBytes = entityDb.get(startTimeKeyBuilder.getBytes());
      if (startTimeBytes == null) {
        return;
      }
      entityDb.delete(startTimeKeyBuilder.getBytes());

      // Build the key for the entity storage and delete it
      entityDb.delete(getEntityKey(entityId, startTimeBytes));
    }

    @Override
    public Iterator<V> valueSetIterator() {
      return getIterator(null, Long.MAX_VALUE);
    }

    @Override
    public Iterator<V> valueSetIterator(V minV) {
      return getIterator(
          new EntityIdentifier(minV.getEntityId(), minV.getEntityType()),
          minV.getStartTime());
    }

    private Iterator<V> getIterator(
        EntityIdentifier startId, long startTimeMax) {

      final DBIterator internalDbIterator = entityDb.iterator();

      // we need to iterate from the first element with key greater than or
      // equal to ENTITY_STORAGE_PREFIX!maxTS(!startId), but stop on the first
      // key who does not have prefix ENTITY_STORATE_PREFIX

      // decide end prefix
      LeveldbUtils.KeyBuilder entityPrefixKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      entityPrefixKeyBuilder.add(ENTITY_STORAGE_PREFIX);
      final byte[] prefixBytes = entityPrefixKeyBuilder.getBytesForLookup();
      // decide start prefix on top of end prefix and seek
      final byte[] startTimeBytes
          = GenericObjectMapper.writeReverseOrderedLong(startTimeMax);
      entityPrefixKeyBuilder.add(startTimeBytes, true);
      if (startId != null) {
        entityPrefixKeyBuilder.add(startId.getId());
      }
      final byte[] startPrefixBytes
          = entityPrefixKeyBuilder.getBytesForLookup();
      internalDbIterator.seek(startPrefixBytes);

      return new Iterator<V>() {
        @Override
        public boolean hasNext() {
          if (!internalDbIterator.hasNext()) {
            return false;
          }
          Map.Entry<byte[], byte[]> nextEntry = internalDbIterator.peekNext();
          if (LeveldbUtils.prefixMatches(
              prefixBytes, prefixBytes.length, nextEntry.getKey())) {
            return true;
          }
          return false;
        }

        @Override
        public V next() {
          if (hasNext()) {
            Map.Entry<byte[], byte[]> nextRaw = internalDbIterator.next();
            try {
              V result = getEntityForKey(nextRaw.getKey());
              return result;
            } catch (IOException e) {
              LOG.error("GenericObjectMapper cannot read key from key "
                  + nextRaw.getKey()
                  + " into an object. Read aborted! ");
              LOG.error(e.getMessage());
            }
          }
          return null;
        }

        // We do not support remove operations within one iteration
        @Override
        public void remove() {
          LOG.error("LevelDB map adapter does not support iterate-and-remove"
              + " use cases. ");
        }
      };
    }

    @SuppressWarnings("unchecked")
    private V getEntityForKey(byte[] key) throws IOException {
      byte[] resultRaw = entityDb.get(key);
      if (resultRaw == null) {
        return null;
      }
      ObjectMapper entityMapper = new ObjectMapper();
      return (V) entityMapper.readValue(resultRaw, TimelineEntity.class);
    }

    private byte[] getStartTimeKey(K entityId) {
      LeveldbUtils.KeyBuilder startTimeKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      startTimeKeyBuilder.add(TIME_INDEX_PREFIX).add(entityId.getId())
          .add(entityId.getType());
      return startTimeKeyBuilder.getBytes();
    }

    private byte[] getEntityKey(K entityId, byte[] startTimeBytes) {
      LeveldbUtils.KeyBuilder entityKeyBuilder
          = LeveldbUtils.KeyBuilder.newInstance();
      entityKeyBuilder.add(ENTITY_STORAGE_PREFIX).add(startTimeBytes, true)
          .add(entityId.getId()).add(entityId.getType());
      return entityKeyBuilder.getBytes();
    }
  }
}
