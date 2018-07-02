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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.timeline.RollingLevelDB.RollingWriteBatch;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager.CheckAcl;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyBuilder;
import org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.KeyParser;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTClazzNameRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.TimelineDataManager.DEFAULT_DOMAIN_ID;
import static org.apache.hadoop.yarn.server.timeline.util.LeveldbUtils.prefixMatches;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.TIMELINE_SERVICE_TTL_MS;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;

/**
 * <p>
 * An implementation of an application timeline store backed by leveldb.
 * </p>
 *
 * <p>
 * There are three sections of the db, the start time section, the entity
 * section, and the indexed entity section.
 * </p>
 *
 * <p>
 * The start time section is used to retrieve the unique start time for a given
 * entity. Its values each contain a start time while its keys are of the form:
 * </p>
 *
 * <pre>
 *   START_TIME_LOOKUP_PREFIX + entity type + entity id
 * </pre>
 *
 * <p>
 * The entity section is ordered by entity type, then entity start time
 * descending, then entity ID. There are four sub-sections of the entity
 * section: events, primary filters, related entities, and other info. The event
 * entries have event info serialized into their values. The other info entries
 * have values corresponding to the values of the other info name/value map for
 * the entry (note the names are contained in the key). All other entries have
 * empty values. The key structure is as follows:
 * </p>
 *
 * <pre>
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     DOMAIN_ID_COLUMN
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     EVENTS_COLUMN + reveventtimestamp + eventtype
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     PRIMARY_FILTERS_COLUMN + name + value
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     OTHER_INFO_COLUMN + name
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     RELATED_ENTITIES_COLUMN + relatedentity type + relatedentity id
 * </pre>
 *
 * <p>
 * The indexed entity section contains a primary filter name and primary filter
 * value as the prefix. Within a given name/value, entire entity entries are
 * stored in the same format as described in the entity section above (below,
 * "key" represents any one of the possible entity entry keys described above).
 * </p>
 *
 * <pre>
 *   INDEXED_ENTRY_PREFIX + primaryfilter name + primaryfilter value +
 *     key
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RollingLevelDBTimelineStore extends AbstractService implements
    TimelineStore {
  private static final Logger LOG = LoggerFactory
      .getLogger(RollingLevelDBTimelineStore.class);
  private static FSTConfiguration fstConf =
      FSTConfiguration.createDefaultConfiguration();
  // Fall back to 2.24 parsing if 2.50 parsing fails
  private static FSTConfiguration fstConf224 =
      FSTConfiguration.createDefaultConfiguration();
  // Static class code for 2.24
  private static final int LINKED_HASH_MAP_224_CODE = 83;

  static {
    fstConf.setShareReferences(false);
    fstConf224.setShareReferences(false);
    // YARN-6654 unable to find class for code 83 (LinkedHashMap)
    // The linked hash map was changed between 2.24 and 2.50 so that
    // the static code for LinkedHashMap (83) was changed to a dynamic
    // code.
    FSTClazzNameRegistry registry = fstConf224.getClassRegistry();
    registry.registerClass(
        LinkedHashMap.class, LINKED_HASH_MAP_224_CODE, fstConf224);
  }

  @Private
  @VisibleForTesting
  static final String FILENAME = "leveldb-timeline-store";
  static final String DOMAIN = "domain-ldb";
  static final String ENTITY = "entity-ldb";
  static final String INDEX = "indexes-ldb";
  static final String STARTTIME = "starttime-ldb";
  static final String OWNER = "owner-ldb";

  private static final byte[] DOMAIN_ID_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] EVENTS_COLUMN = "e".getBytes(UTF_8);
  private static final byte[] PRIMARY_FILTERS_COLUMN = "f".getBytes(UTF_8);
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes(UTF_8);
  private static final byte[] RELATED_ENTITIES_COLUMN = "r".getBytes(UTF_8);

  private static final byte[] DESCRIPTION_COLUMN = "d".getBytes(UTF_8);
  private static final byte[] OWNER_COLUMN = "o".getBytes(UTF_8);
  private static final byte[] READER_COLUMN = "r".getBytes(UTF_8);
  private static final byte[] WRITER_COLUMN = "w".getBytes(UTF_8);
  private static final byte[] TIMESTAMP_COLUMN = "t".getBytes(UTF_8);

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final String TIMELINE_STORE_VERSION_KEY =
      "timeline-store-version";

  private static final Version CURRENT_VERSION_INFO = Version.newInstance(1, 0);

  private static long writeBatchSize = 10000;

  @Private
  @VisibleForTesting
  static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  private Map<EntityIdentifier, Long> startTimeWriteCache;
  private Map<EntityIdentifier, Long> startTimeReadCache;

  private DB domaindb;
  private RollingLevelDB entitydb;
  private RollingLevelDB indexdb;
  private DB starttimedb;
  private DB ownerdb;

  private Thread deletionThread;

  public RollingLevelDBTimelineStore() {
    super(RollingLevelDBTimelineStore.class.getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void serviceInit(Configuration conf) throws Exception {
    Preconditions
        .checkArgument(conf.getLong(TIMELINE_SERVICE_TTL_MS,
            DEFAULT_TIMELINE_SERVICE_TTL_MS) > 0,
            "%s property value should be greater than zero",
            TIMELINE_SERVICE_TTL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE) >= 0,
        "%s property value should be greater than or equal to zero",
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE) > 0,
        " %s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES);
    Preconditions.checkArgument(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE) > 0,
        "%s property value should be greater than zero",
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE);

    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    JniDBFactory factory = new JniDBFactory();
    Path dbPath = new Path(
        conf.get(TIMELINE_SERVICE_LEVELDB_PATH), FILENAME);
    Path domainDBPath = new Path(dbPath, DOMAIN);
    Path starttimeDBPath = new Path(dbPath, STARTTIME);
    Path ownerDBPath = new Path(dbPath, OWNER);
    try (FileSystem localFS = FileSystem.getLocal(conf)) {
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(domainDBPath)) {
        if (!localFS.mkdirs(domainDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + domainDBPath);
        }
        localFS.setPermission(domainDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(starttimeDBPath)) {
        if (!localFS.mkdirs(starttimeDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + starttimeDBPath);
        }
        localFS.setPermission(starttimeDBPath, LEVELDB_DIR_UMASK);
      }
      if (!localFS.exists(ownerDBPath)) {
        if (!localFS.mkdirs(ownerDBPath)) {
          throw new IOException("Couldn't create directory for leveldb "
              + "timeline store " + ownerDBPath);
        }
        localFS.setPermission(ownerDBPath, LEVELDB_DIR_UMASK);
      }
    }
    options.maxOpenFiles(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_MAX_OPEN_FILES));
    options.writeBufferSize(conf.getInt(
        TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BUFFER_SIZE));
    LOG.info("Using leveldb path " + dbPath);
    domaindb = factory.open(new File(domainDBPath.toString()), options);
    entitydb = new RollingLevelDB(ENTITY);
    entitydb.init(conf);
    indexdb = new RollingLevelDB(INDEX);
    indexdb.init(conf);
    starttimedb = factory.open(new File(starttimeDBPath.toString()), options);
    ownerdb = factory.open(new File(ownerDBPath.toString()), options);
    checkVersion();
    startTimeWriteCache = Collections.synchronizedMap(new LRUMap(
        getStartTimeWriteCacheSize(conf)));
    startTimeReadCache = Collections.synchronizedMap(new LRUMap(
        getStartTimeReadCacheSize(conf)));

    writeBatchSize = conf.getInt(
        TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE,
        DEFAULT_TIMELINE_SERVICE_LEVELDB_WRITE_BATCH_SIZE);

    super.serviceInit(conf);
  }
  
  @Override
  protected void serviceStart() throws Exception {
    if (getConfig().getBoolean(TIMELINE_SERVICE_TTL_ENABLE, true)) {
      deletionThread = new EntityDeletionThread(getConfig());
      deletionThread.start();
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (deletionThread != null) {
      deletionThread.interrupt();
      LOG.info("Waiting for deletion thread to complete its current action");
      try {
        deletionThread.join();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for deletion thread to complete,"
            + " closing db now", e);
      }
    }
    IOUtils.cleanupWithLogger(LOG, domaindb);
    IOUtils.cleanupWithLogger(LOG, starttimedb);
    IOUtils.cleanupWithLogger(LOG, ownerdb);
    entitydb.stop();
    indexdb.stop();
    super.serviceStop();
  }

  private class EntityDeletionThread extends Thread {
    private final long ttl;
    private final long ttlInterval;

    EntityDeletionThread(Configuration conf) {
      ttl = conf.getLong(TIMELINE_SERVICE_TTL_MS,
          DEFAULT_TIMELINE_SERVICE_TTL_MS);
      ttlInterval = conf.getLong(
          TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
          DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
      LOG.info("Starting deletion thread with ttl " + ttl + " and cycle "
          + "interval " + ttlInterval);
    }

    @Override
    public void run() {
      Thread.currentThread().setName("Leveldb Timeline Store Retention");
      while (true) {
        long timestamp = System.currentTimeMillis() - ttl;
        try {
          discardOldEntities(timestamp);
          Thread.sleep(ttlInterval);
        } catch (IOException e) {
          LOG.error(e.toString());
        } catch (InterruptedException e) {
          LOG.info("Deletion thread received interrupt, exiting");
          break;
        }
      }
    }
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fields) throws IOException {
    Long revStartTime = getStartTimeLong(entityId, entityType);
    if (revStartTime == null) {
      if ( LOG.isDebugEnabled()) {
        LOG.debug("Could not find start time for {} {} ", entityType, entityId);
      }
      return null;
    }
    byte[] prefix = KeyBuilder.newInstance().add(entityType)
        .add(writeReverseOrderedLong(revStartTime)).add(entityId)
        .getBytesForLookup();

    DB db = entitydb.getDBForStartTime(revStartTime);
    if (db == null) {
      if ( LOG.isDebugEnabled()) {
        LOG.debug("Could not find db for {} {} ", entityType, entityId);
      }
      return null;
    }
    try (DBIterator iterator = db.iterator()) {
      iterator.seek(prefix);

      return getEntity(entityId, entityType, revStartTime, fields, iterator,
          prefix, prefix.length);
    }
  }

  /**
   * Read entity from a db iterator. If no information is found in the specified
   * fields for this entity, return null.
   */
  private static TimelineEntity getEntity(String entityId, String entityType,
      Long startTime, EnumSet<Field> fields, DBIterator iterator,
      byte[] prefix, int prefixlen) throws IOException {
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }

    TimelineEntity entity = new TimelineEntity();
    boolean events = false;
    boolean lastEvent = false;
    if (fields.contains(Field.EVENTS)) {
      events = true;
    } else if (fields.contains(Field.LAST_EVENT_ONLY)) {
      lastEvent = true;
    } else {
      entity.setEvents(null);
    }
    boolean relatedEntities = false;
    if (fields.contains(Field.RELATED_ENTITIES)) {
      relatedEntities = true;
    } else {
      entity.setRelatedEntities(null);
    }
    boolean primaryFilters = false;
    if (fields.contains(Field.PRIMARY_FILTERS)) {
      primaryFilters = true;
    } else {
      entity.setPrimaryFilters(null);
    }
    boolean otherInfo = false;
    if (fields.contains(Field.OTHER_INFO)) {
      otherInfo = true;
    } else {
      entity.setOtherInfo(null);
    }

    // iterate through the entity's entry, parsing information if it is part
    // of a requested field
    for (; iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefixlen, key)) {
        break;
      }
      if (key.length == prefixlen) {
        continue;
      }
      if (key[prefixlen] == PRIMARY_FILTERS_COLUMN[0]) {
        if (primaryFilters) {
          addPrimaryFilter(entity, key, prefixlen
              + PRIMARY_FILTERS_COLUMN.length);
        }
      } else if (key[prefixlen] == OTHER_INFO_COLUMN[0]) {
        if (otherInfo) {
          Object o = null;
          String keyStr = parseRemainingKey(key,
              prefixlen + OTHER_INFO_COLUMN.length);
          try {
            o = fstConf.asObject(iterator.peekNext().getValue());
            entity.addOtherInfo(keyStr, o);
          } catch (Exception ignore) {
            try {
              // Fall back to 2.24 parser
              o = fstConf224.asObject(iterator.peekNext().getValue());
              entity.addOtherInfo(keyStr, o);
            } catch (Exception e) {
              LOG.warn("Error while decoding "
                  + entityId + ":otherInfo:" + keyStr, e);
            }
          }
        }
      } else if (key[prefixlen] == RELATED_ENTITIES_COLUMN[0]) {
        if (relatedEntities) {
          addRelatedEntity(entity, key, prefixlen
              + RELATED_ENTITIES_COLUMN.length);
        }
      } else if (key[prefixlen] == EVENTS_COLUMN[0]) {
        if (events || (lastEvent && entity.getEvents().size() == 0)) {
          TimelineEvent event = getEntityEvent(null, key, prefixlen
              + EVENTS_COLUMN.length, iterator.peekNext().getValue());
          if (event != null) {
            entity.addEvent(event);
          }
        }
      } else if (key[prefixlen] == DOMAIN_ID_COLUMN[0]) {
        byte[] v = iterator.peekNext().getValue();
        String domainId = new String(v, UTF_8);
        entity.setDomainId(domainId);
      } else {
        LOG.warn(String.format("Found unexpected column for entity %s of "
            + "type %s (0x%02x)", entityId, entityType, key[prefixlen]));
      }
    }

    entity.setEntityId(entityId);
    entity.setEntityType(entityType);
    entity.setStartTime(startTime);

    return entity;
  }

  @Override
  public TimelineEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventType) throws IOException {
    TimelineEvents events = new TimelineEvents();
    if (entityIds == null || entityIds.isEmpty()) {
      return events;
    }
    // create a lexicographically-ordered map from start time to entities
    Map<byte[], List<EntityIdentifier>> startTimeMap =
        new TreeMap<byte[], List<EntityIdentifier>>(
        new Comparator<byte[]>() {
          @Override
          public int compare(byte[] o1, byte[] o2) {
            return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0,
                o2.length);
          }
        });

      // look up start times for the specified entities
      // skip entities with no start time
    for (String entityId : entityIds) {
      byte[] startTime = getStartTime(entityId, entityType);
      if (startTime != null) {
        List<EntityIdentifier> entities = startTimeMap.get(startTime);
        if (entities == null) {
          entities = new ArrayList<EntityIdentifier>();
          startTimeMap.put(startTime, entities);
        }
        entities.add(new EntityIdentifier(entityId, entityType));
      }
    }
    for (Entry<byte[], List<EntityIdentifier>> entry : startTimeMap
          .entrySet()) {
      // look up the events matching the given parameters (limit,
      // start time, end time, event types) for entities whose start times
      // were found and add the entities to the return list
      byte[] revStartTime = entry.getKey();
      for (EntityIdentifier entityIdentifier : entry.getValue()) {
        EventsOfOneEntity entity = new EventsOfOneEntity();
        entity.setEntityId(entityIdentifier.getId());
        entity.setEntityType(entityType);
        events.addEvent(entity);
        KeyBuilder kb = KeyBuilder.newInstance().add(entityType)
            .add(revStartTime).add(entityIdentifier.getId())
            .add(EVENTS_COLUMN);
        byte[] prefix = kb.getBytesForLookup();
        if (windowEnd == null) {
          windowEnd = Long.MAX_VALUE;
        }
        byte[] revts = writeReverseOrderedLong(windowEnd);
        kb.add(revts);
        byte[] first = kb.getBytesForLookup();
        byte[] last = null;
        if (windowStart != null) {
          last = KeyBuilder.newInstance().add(prefix)
              .add(writeReverseOrderedLong(windowStart)).getBytesForLookup();
        }
        if (limit == null) {
          limit = DEFAULT_LIMIT;
        }
        DB db = entitydb.getDBForStartTime(readReverseOrderedLong(
            revStartTime, 0));
        if (db == null) {
          continue;
        }
        try (DBIterator iterator = db.iterator()) {
          for (iterator.seek(first); entity.getEvents().size() < limit
              && iterator.hasNext(); iterator.next()) {
            byte[] key = iterator.peekNext().getKey();
            if (!prefixMatches(prefix, prefix.length, key)
                || (last != null && WritableComparator.compareBytes(key, 0,
                key.length, last, 0, last.length) > 0)) {
              break;
            }
            TimelineEvent event = getEntityEvent(eventType, key, prefix.length,
                iterator.peekNext().getValue());
            if (event != null) {
              entity.addEvent(event);
            }
          }
        }
      }
    }
    return events;
  }

  @Override
  public TimelineEntities getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields, CheckAcl checkAcl) throws IOException {
    if (primaryFilter == null) {
      // if no primary filter is specified, prefix the lookup with
      // ENTITY_ENTRY_PREFIX
      return getEntityByTime(EMPTY_BYTES, entityType, limit, windowStart,
          windowEnd, fromId, fromTs, secondaryFilters, fields, checkAcl, false);
    } else {
      // if a primary filter is specified, prefix the lookup with
      // INDEXED_ENTRY_PREFIX + primaryFilterName + primaryFilterValue +
      // ENTITY_ENTRY_PREFIX
      byte[] base = KeyBuilder.newInstance().add(primaryFilter.getName())
          .add(fstConf.asByteArray(primaryFilter.getValue()), true)
          .getBytesForLookup();
      return getEntityByTime(base, entityType, limit, windowStart, windowEnd,
          fromId, fromTs, secondaryFilters, fields, checkAcl, true);
    }
  }

  /**
   * Retrieves a list of entities satisfying given parameters.
   *
   * @param base
   *          A byte array prefix for the lookup
   * @param entityType
   *          The type of the entity
   * @param limit
   *          A limit on the number of entities to return
   * @param starttime
   *          The earliest entity start time to retrieve (exclusive)
   * @param endtime
   *          The latest entity start time to retrieve (inclusive)
   * @param fromId
   *          Retrieve entities starting with this entity
   * @param fromTs
   *          Ignore entities with insert timestamp later than this ts
   * @param secondaryFilters
   *          Filter pairs that the entities should match
   * @param fields
   *          The set of fields to retrieve
   * @param usingPrimaryFilter
   *          true if this query is using a primary filter
   * @return A list of entities
   * @throws IOException
   */
  private TimelineEntities getEntityByTime(byte[] base, String entityType,
      Long limit, Long starttime, Long endtime, String fromId, Long fromTs,
      Collection<NameValuePair> secondaryFilters, EnumSet<Field> fields,
      CheckAcl checkAcl, boolean usingPrimaryFilter) throws IOException {
    KeyBuilder kb = KeyBuilder.newInstance().add(base).add(entityType);
    // only db keys matching the prefix (base + entity type) will be parsed
    byte[] prefix = kb.getBytesForLookup();
    if (endtime == null) {
      // if end time is null, place no restriction on end time
      endtime = Long.MAX_VALUE;
    }

    // Sanitize the fields parameter
    if (fields == null) {
      fields = EnumSet.allOf(Field.class);
    }

    // construct a first key that will be seeked to using end time or fromId
    long firstStartTime = Long.MAX_VALUE;
    byte[] first = null;
    if (fromId != null) {
      Long fromIdStartTime = getStartTimeLong(fromId, entityType);
      if (fromIdStartTime == null) {
        // no start time for provided id, so return empty entities
        return new TimelineEntities();
      }
      if (fromIdStartTime <= endtime) {
        // if provided id's start time falls before the end of the window,
        // use it to construct the seek key
        firstStartTime = fromIdStartTime;
        first = kb.add(writeReverseOrderedLong(fromIdStartTime)).add(fromId)
            .getBytesForLookup();
      }
    }
    // if seek key wasn't constructed using fromId, construct it using end ts
    if (first == null) {
      firstStartTime = endtime;
      first = kb.add(writeReverseOrderedLong(endtime)).getBytesForLookup();
    }
    byte[] last = null;
    if (starttime != null) {
      // if start time is not null, set a last key that will not be
      // iterated past
      last = KeyBuilder.newInstance().add(base).add(entityType)
          .add(writeReverseOrderedLong(starttime)).getBytesForLookup();
    }
    if (limit == null) {
      // if limit is not specified, use the default
      limit = DEFAULT_LIMIT;
    }

    TimelineEntities entities = new TimelineEntities();
    RollingLevelDB rollingdb = null;
    if (usingPrimaryFilter) {
      rollingdb = indexdb;
    } else {
      rollingdb = entitydb;
    }

    DB db = rollingdb.getDBForStartTime(firstStartTime);
    while (entities.getEntities().size() < limit && db != null) {
      try (DBIterator iterator = db.iterator()) {
        iterator.seek(first);

        // iterate until one of the following conditions is met: limit is
        // reached, there are no more keys, the key prefix no longer matches,
        // or a start time has been specified and reached/exceeded
        while (entities.getEntities().size() < limit && iterator.hasNext()) {
          byte[] key = iterator.peekNext().getKey();
          if (!prefixMatches(prefix, prefix.length, key)
              || (last != null && WritableComparator.compareBytes(key, 0,
              key.length, last, 0, last.length) > 0)) {
            break;
          }
          // read the start time and entity id from the current key
          KeyParser kp = new KeyParser(key, prefix.length);
          Long startTime = kp.getNextLong();
          String entityId = kp.getNextString();

          if (fromTs != null) {
            long insertTime = readReverseOrderedLong(iterator.peekNext()
                .getValue(), 0);
            if (insertTime > fromTs) {
              byte[] firstKey = key;
              while (iterator.hasNext()) {
                key = iterator.peekNext().getKey();
                iterator.next();
                if (!prefixMatches(firstKey, kp.getOffset(), key)) {
                  break;
                }
              }
              continue;
            }
          }
          // Even if other info and primary filter fields are not included, we
          // still need to load them to match secondary filters when they are
          // non-empty
          EnumSet<Field> queryFields = EnumSet.copyOf(fields);
          boolean addPrimaryFilters = false;
          boolean addOtherInfo = false;
          if (secondaryFilters != null && secondaryFilters.size() > 0) {
            if (!queryFields.contains(Field.PRIMARY_FILTERS)) {
              queryFields.add(Field.PRIMARY_FILTERS);
              addPrimaryFilters = true;
            }
            if (!queryFields.contains(Field.OTHER_INFO)) {
              queryFields.add(Field.OTHER_INFO);
              addOtherInfo = true;
            }
          }

          // parse the entity that owns this key, iterating over all keys for
          // the entity
          TimelineEntity entity = null;
          if (usingPrimaryFilter) {
            entity = getEntity(entityId, entityType, queryFields);
            iterator.next();
          } else {
            entity = getEntity(entityId, entityType, startTime, queryFields,
                iterator, key, kp.getOffset());
          }
          // determine if the retrieved entity matches the provided secondary
          // filters, and if so add it to the list of entities to return
          boolean filterPassed = true;
          if (secondaryFilters != null) {
            for (NameValuePair filter : secondaryFilters) {
              Object v = entity.getOtherInfo().get(filter.getName());
              if (v == null) {
                Set<Object> vs = entity.getPrimaryFilters()
                    .get(filter.getName());
                if (vs == null || !vs.contains(filter.getValue())) {
                  filterPassed = false;
                  break;
                }
              } else if (!v.equals(filter.getValue())) {
                filterPassed = false;
                break;
              }
            }
          }
          if (filterPassed) {
            if (entity.getDomainId() == null) {
              entity.setDomainId(DEFAULT_DOMAIN_ID);
            }
            if (checkAcl == null || checkAcl.check(entity)) {
              // Remove primary filter and other info if they are added for
              // matching secondary filters
              if (addPrimaryFilters) {
                entity.setPrimaryFilters(null);
              }
              if (addOtherInfo) {
                entity.setOtherInfo(null);
              }
              entities.addEntity(entity);
            }
          }
        }
        db = rollingdb.getPreviousDB(db);
      }
    }
    return entities;
  }

  /**
   * Put a single entity. If there is an error, add a TimelinePutError to the
   * given response.
   *
   * @param entityUpdates
   *          a map containing all the scheduled writes for this put to the
   *          entity db
   * @param indexUpdates
   *          a map containing all the scheduled writes for this put to the
   *          index db
   */
  private long putEntities(TreeMap<Long, RollingWriteBatch> entityUpdates,
      TreeMap<Long, RollingWriteBatch> indexUpdates, TimelineEntity entity,
      TimelinePutResponse response) {

    long putCount = 0;
    List<EntityIdentifier> relatedEntitiesWithoutStartTimes =
        new ArrayList<EntityIdentifier>();
    byte[] revStartTime = null;
    Map<String, Set<Object>> primaryFilters = null;
    try {
      List<TimelineEvent> events = entity.getEvents();
      // look up the start time for the entity
      Long startTime = getAndSetStartTime(entity.getEntityId(),
          entity.getEntityType(), entity.getStartTime(), events);
      if (startTime == null) {
        // if no start time is found, add an error and return
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.NO_START_TIME);
        response.addError(error);
        return putCount;
      }

      // Must have a domain
      if (StringUtils.isEmpty(entity.getDomainId())) {
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.NO_DOMAIN);
        response.addError(error);
        return putCount;
      }

      revStartTime = writeReverseOrderedLong(startTime);
      long roundedStartTime = entitydb.computeCurrentCheckMillis(startTime);
      RollingWriteBatch rollingWriteBatch = entityUpdates.get(roundedStartTime);
      if (rollingWriteBatch == null) {
        DB db = entitydb.getDBForStartTime(startTime);
        if (db != null) {
          WriteBatch writeBatch = db.createWriteBatch();
          rollingWriteBatch = new RollingWriteBatch(db, writeBatch);
          entityUpdates.put(roundedStartTime, rollingWriteBatch);
        }
      }
      if (rollingWriteBatch == null) {
        // if no start time is found, add an error and return
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
        response.addError(error);
        return putCount;
      }
      WriteBatch writeBatch = rollingWriteBatch.getWriteBatch();

      // Save off the getBytes conversion to avoid unnecessary cost
      byte[] entityIdBytes = entity.getEntityId().getBytes(UTF_8);
      byte[] entityTypeBytes = entity.getEntityType().getBytes(UTF_8);
      byte[] domainIdBytes = entity.getDomainId().getBytes(UTF_8);

      // write entity marker
      byte[] markerKey = KeyBuilder.newInstance(3).add(entityTypeBytes, true)
          .add(revStartTime).add(entityIdBytes, true).getBytesForLookup();
      writeBatch.put(markerKey, EMPTY_BYTES);
      ++putCount;

      // write domain id entry
      byte[] domainkey = KeyBuilder.newInstance(4).add(entityTypeBytes, true)
          .add(revStartTime).add(entityIdBytes, true).add(DOMAIN_ID_COLUMN)
          .getBytes();
      writeBatch.put(domainkey, domainIdBytes);
      ++putCount;

      // write event entries
      if (events != null) {
        for (TimelineEvent event : events) {
          byte[] revts = writeReverseOrderedLong(event.getTimestamp());
          byte[] key = KeyBuilder.newInstance().add(entityTypeBytes, true)
              .add(revStartTime).add(entityIdBytes, true).add(EVENTS_COLUMN)
              .add(revts).add(event.getEventType().getBytes(UTF_8)).getBytes();
          byte[] value = fstConf.asByteArray(event.getEventInfo());
          writeBatch.put(key, value);
          ++putCount;
        }
      }

      // write primary filter entries
      primaryFilters = entity.getPrimaryFilters();
      if (primaryFilters != null) {
        for (Entry<String, Set<Object>> primaryFilter : primaryFilters
            .entrySet()) {
          for (Object primaryFilterValue : primaryFilter.getValue()) {
            byte[] key = KeyBuilder.newInstance(6).add(entityTypeBytes, true)
                .add(revStartTime).add(entityIdBytes, true)
                .add(PRIMARY_FILTERS_COLUMN).add(primaryFilter.getKey())
                .add(fstConf.asByteArray(primaryFilterValue)).getBytes();
            writeBatch.put(key, EMPTY_BYTES);
            ++putCount;
          }
        }
      }

      // write other info entries
      Map<String, Object> otherInfo = entity.getOtherInfo();
      if (otherInfo != null) {
        for (Entry<String, Object> info : otherInfo.entrySet()) {
          byte[] key = KeyBuilder.newInstance(5).add(entityTypeBytes, true)
              .add(revStartTime).add(entityIdBytes, true)
              .add(OTHER_INFO_COLUMN).add(info.getKey()).getBytes();
          byte[] value = fstConf.asByteArray(info.getValue());
          writeBatch.put(key, value);
          ++putCount;
        }
      }

      // write related entity entries
      Map<String, Set<String>> relatedEntities = entity.getRelatedEntities();
      if (relatedEntities != null) {
        for (Entry<String, Set<String>> relatedEntityList : relatedEntities
            .entrySet()) {
          String relatedEntityType = relatedEntityList.getKey();
          for (String relatedEntityId : relatedEntityList.getValue()) {
            // look up start time of related entity
            Long relatedStartTimeLong = getStartTimeLong(relatedEntityId,
                relatedEntityType);
            // delay writing the related entity if no start time is found
            if (relatedStartTimeLong == null) {
              relatedEntitiesWithoutStartTimes.add(new EntityIdentifier(
                  relatedEntityId, relatedEntityType));
              continue;
            }

            byte[] relatedEntityStartTime =
                writeReverseOrderedLong(relatedStartTimeLong);
            long relatedRoundedStartTime = entitydb
                .computeCurrentCheckMillis(relatedStartTimeLong);
            RollingWriteBatch relatedRollingWriteBatch = entityUpdates
                .get(relatedRoundedStartTime);
            if (relatedRollingWriteBatch == null) {
              DB db = entitydb.getDBForStartTime(relatedStartTimeLong);
              if (db != null) {
                WriteBatch relatedWriteBatch = db.createWriteBatch();
                relatedRollingWriteBatch = new RollingWriteBatch(db,
                    relatedWriteBatch);
                entityUpdates.put(relatedRoundedStartTime,
                    relatedRollingWriteBatch);
              }
            }
            if (relatedRollingWriteBatch == null) {
              // if no start time is found, add an error and return
              TimelinePutError error = new TimelinePutError();
              error.setEntityId(entity.getEntityId());
              error.setEntityType(entity.getEntityType());
              error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
              response.addError(error);
              continue;
            }
            // This is the existing entity
            byte[] relatedDomainIdBytes = relatedRollingWriteBatch.getDB().get(
                createDomainIdKey(relatedEntityId, relatedEntityType,
                    relatedEntityStartTime));
            // The timeline data created by the server before 2.6 won't have
            // the domain field. We assume this timeline data is in the
            // default timeline domain.
            String domainId = null;
            if (relatedDomainIdBytes == null) {
              domainId = TimelineDataManager.DEFAULT_DOMAIN_ID;
            } else {
              domainId = new String(relatedDomainIdBytes, UTF_8);
            }
            if (!domainId.equals(entity.getDomainId())) {
              // in this case the entity will be put, but the relation will be
              // ignored
              TimelinePutError error = new TimelinePutError();
              error.setEntityId(entity.getEntityId());
              error.setEntityType(entity.getEntityType());
              error.setErrorCode(TimelinePutError.FORBIDDEN_RELATION);
              response.addError(error);
              continue;
            }
            // write "forward" entry (related entity -> entity)
            byte[] key = createRelatedEntityKey(relatedEntityId,
                relatedEntityType, relatedEntityStartTime,
                entity.getEntityId(), entity.getEntityType());
            WriteBatch relatedWriteBatch = relatedRollingWriteBatch
                .getWriteBatch();
            relatedWriteBatch.put(key, EMPTY_BYTES);
            ++putCount;
          }
        }
      }

      // write index entities
      RollingWriteBatch indexRollingWriteBatch = indexUpdates
          .get(roundedStartTime);
      if (indexRollingWriteBatch == null) {
        DB db = indexdb.getDBForStartTime(startTime);
        if (db != null) {
          WriteBatch indexWriteBatch = db.createWriteBatch();
          indexRollingWriteBatch = new RollingWriteBatch(db, indexWriteBatch);
          indexUpdates.put(roundedStartTime, indexRollingWriteBatch);
        }
      }
      if (indexRollingWriteBatch == null) {
        // if no start time is found, add an error and return
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
        response.addError(error);
        return putCount;
      }
      WriteBatch indexWriteBatch = indexRollingWriteBatch.getWriteBatch();
      putCount += writePrimaryFilterEntries(indexWriteBatch, primaryFilters,
          markerKey, EMPTY_BYTES);
    } catch (IOException e) {
      LOG.error("Error putting entity " + entity.getEntityId() + " of type "
          + entity.getEntityType(), e);
      TimelinePutError error = new TimelinePutError();
      error.setEntityId(entity.getEntityId());
      error.setEntityType(entity.getEntityType());
      error.setErrorCode(TimelinePutError.IO_EXCEPTION);
      response.addError(error);
    }

    for (EntityIdentifier relatedEntity : relatedEntitiesWithoutStartTimes) {
      try {
        Long relatedEntityStartAndInsertTime = getAndSetStartTime(
            relatedEntity.getId(), relatedEntity.getType(),
            readReverseOrderedLong(revStartTime, 0), null);
        if (relatedEntityStartAndInsertTime == null) {
          throw new IOException("Error setting start time for related entity");
        }
        long relatedStartTimeLong = relatedEntityStartAndInsertTime;
        long relatedRoundedStartTime = entitydb
            .computeCurrentCheckMillis(relatedStartTimeLong);
        RollingWriteBatch relatedRollingWriteBatch = entityUpdates
            .get(relatedRoundedStartTime);
        if (relatedRollingWriteBatch == null) {
          DB db = entitydb.getDBForStartTime(relatedStartTimeLong);
          if (db != null) {
            WriteBatch relatedWriteBatch = db.createWriteBatch();
            relatedRollingWriteBatch = new RollingWriteBatch(db,
                relatedWriteBatch);
            entityUpdates
                .put(relatedRoundedStartTime, relatedRollingWriteBatch);
          }
        }
        if (relatedRollingWriteBatch == null) {
          // if no start time is found, add an error and return
          TimelinePutError error = new TimelinePutError();
          error.setEntityId(entity.getEntityId());
          error.setEntityType(entity.getEntityType());
          error.setErrorCode(TimelinePutError.EXPIRED_ENTITY);
          response.addError(error);
          continue;
        }
        WriteBatch relatedWriteBatch = relatedRollingWriteBatch.getWriteBatch();
        byte[] relatedEntityStartTime =
            writeReverseOrderedLong(relatedEntityStartAndInsertTime);
        // This is the new entity, the domain should be the same
        byte[] key = createDomainIdKey(relatedEntity.getId(),
            relatedEntity.getType(), relatedEntityStartTime);
        relatedWriteBatch.put(key, entity.getDomainId().getBytes(UTF_8));
        ++putCount;
        relatedWriteBatch.put(
            createRelatedEntityKey(relatedEntity.getId(),
                relatedEntity.getType(), relatedEntityStartTime,
                entity.getEntityId(), entity.getEntityType()), EMPTY_BYTES);
        ++putCount;
        relatedWriteBatch.put(
            createEntityMarkerKey(relatedEntity.getId(),
                relatedEntity.getType(), relatedEntityStartTime), EMPTY_BYTES);
        ++putCount;
      } catch (IOException e) {
        LOG.error(
            "Error putting related entity " + relatedEntity.getId()
                + " of type " + relatedEntity.getType() + " for entity "
                + entity.getEntityId() + " of type " + entity.getEntityType(),
            e);
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.IO_EXCEPTION);
        response.addError(error);
      }
    }

    return putCount;
  }

  /**
   * For a given key / value pair that has been written to the db, write
   * additional entries to the db for each primary filter.
   */
  private static long writePrimaryFilterEntries(WriteBatch writeBatch,
      Map<String, Set<Object>> primaryFilters, byte[] key, byte[] value)
      throws IOException {
    long putCount = 0;
    if (primaryFilters != null) {
      for (Entry<String, Set<Object>> pf : primaryFilters.entrySet()) {
        for (Object pfval : pf.getValue()) {
          writeBatch.put(addPrimaryFilterToKey(pf.getKey(), pfval, key), value);
          ++putCount;
        }
      }
    }
    return putCount;
  }

  @Override
  public TimelinePutResponse put(TimelineEntities entities) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting put");
    }
    TimelinePutResponse response = new TimelinePutResponse();
    TreeMap<Long, RollingWriteBatch> entityUpdates =
        new TreeMap<Long, RollingWriteBatch>();
    TreeMap<Long, RollingWriteBatch> indexUpdates =
        new TreeMap<Long, RollingWriteBatch>();

    long entityCount = 0;
    long indexCount = 0;

    try {

      for (TimelineEntity entity : entities.getEntities()) {
        entityCount += putEntities(entityUpdates, indexUpdates, entity,
            response);
      }

      for (RollingWriteBatch entityUpdate : entityUpdates.values()) {
        entityUpdate.write();
      }

      for (RollingWriteBatch indexUpdate : indexUpdates.values()) {
        indexUpdate.write();
      }

    } finally {

      for (RollingWriteBatch entityRollingWriteBatch : entityUpdates.values()) {
        entityRollingWriteBatch.close();
      }
      for (RollingWriteBatch indexRollingWriteBatch : indexUpdates.values()) {
        indexRollingWriteBatch.close();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Put " + entityCount + " new leveldb entity entries and "
          + indexCount + " new leveldb index entries from "
          + entities.getEntities().size() + " timeline entities");
    }
    return response;
  }

  /**
   * Get the unique start time for a given entity as a byte array that sorts the
   * timestamps in reverse order (see
   * {@link GenericObjectMapper#writeReverseOrderedLong(long)}).
   *
   * @param entityId
   *          The id of the entity
   * @param entityType
   *          The type of the entity
   * @return A byte array, null if not found
   * @throws IOException
   */
  private byte[] getStartTime(String entityId, String entityType)
      throws IOException {
    Long l = getStartTimeLong(entityId, entityType);
    return l == null ? null : writeReverseOrderedLong(l);
  }

  /**
   * Get the unique start time for a given entity as a Long.
   *
   * @param entityId
   *          The id of the entity
   * @param entityType
   *          The type of the entity
   * @return A Long, null if not found
   * @throws IOException
   */
  private Long getStartTimeLong(String entityId, String entityType)
      throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    // start time is not provided, so try to look it up
    if (startTimeReadCache.containsKey(entity)) {
      // found the start time in the cache
      return startTimeReadCache.get(entity);
    } else {
      // try to look up the start time in the db
      byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
      byte[] v = starttimedb.get(b);
      if (v == null) {
        // did not find the start time in the db
        return null;
      } else {
        // found the start time in the db
        Long l = readReverseOrderedLong(v, 0);
        startTimeReadCache.put(entity, l);
        return l;
      }
    }
  }

  /**
   * Get the unique start time for a given entity as a byte array that sorts the
   * timestamps in reverse order (see
   * {@link GenericObjectMapper#writeReverseOrderedLong(long)}). If the start
   * time doesn't exist, set it based on the information provided.
   *
   * @param entityId
   *          The id of the entity
   * @param entityType
   *          The type of the entity
   * @param startTime
   *          The start time of the entity, or null
   * @param events
   *          A list of events for the entity, or null
   * @return A StartAndInsertTime
   * @throws IOException
   */
  private Long getAndSetStartTime(String entityId,
      String entityType, Long startTime, List<TimelineEvent> events)
      throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    Long time = startTimeWriteCache.get(entity);
    if (time != null) {
      // return the value in the cache
      return time;
    }
    if (startTime == null && events != null) {
      // calculate best guess start time based on lowest event time
      startTime = Long.MAX_VALUE;
      for (TimelineEvent e : events) {
        if (e.getTimestamp() < startTime) {
          startTime = e.getTimestamp();
        }
      }
    }
    // check the provided start time matches the db
    return checkStartTimeInDb(entity, startTime);
  }

  /**
   * Checks db for start time and returns it if it exists. If it doesn't exist,
   * writes the suggested start time (if it is not null). This is only called
   * when the start time is not found in the cache, so it adds it back into the
   * cache if it is found.
   */
  private Long checkStartTimeInDb(EntityIdentifier entity,
      Long suggestedStartTime) throws IOException {
    Long startAndInsertTime = null;
    // create lookup key for start time
    byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
    synchronized (this) {
      // retrieve value for key
      byte[] v = starttimedb.get(b);
      if (v == null) {
        // start time doesn't exist in db
        if (suggestedStartTime == null) {
          return null;
        }
        startAndInsertTime = suggestedStartTime;

        // write suggested start time
        starttimedb.put(b, writeReverseOrderedLong(suggestedStartTime));
      } else {
        // found start time in db, so ignore suggested start time
        startAndInsertTime = readReverseOrderedLong(v, 0);
      }
    }
    startTimeWriteCache.put(entity, startAndInsertTime);
    startTimeReadCache.put(entity, startAndInsertTime);
    return startAndInsertTime;
  }

  /**
   * Creates a key for looking up the start time of a given entity, of the form
   * START_TIME_LOOKUP_PREFIX + entity type + entity id.
   */
  private static byte[] createStartTimeLookupKey(String entityId,
      String entityType) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(entityId).getBytes();
  }

  /**
   * Creates an entity marker, serializing ENTITY_ENTRY_PREFIX + entity type +
   * revstarttime + entity id.
   */
  private static byte[] createEntityMarkerKey(String entityId,
      String entityType, byte[] revStartTime) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).getBytesForLookup();
  }

  /**
   * Creates an index entry for the given key of the form INDEXED_ENTRY_PREFIX +
   * primaryfiltername + primaryfiltervalue + key.
   */
  private static byte[] addPrimaryFilterToKey(String primaryFilterName,
      Object primaryFilterValue, byte[] key) throws IOException {
    return KeyBuilder.newInstance().add(primaryFilterName)
        .add(fstConf.asByteArray(primaryFilterValue), true).add(key).getBytes();
  }

  /**
   * Creates an event object from the given key, offset, and value. If the event
   * type is not contained in the specified set of event types, returns null.
   */
  private static TimelineEvent getEntityEvent(Set<String> eventTypes,
      byte[] key, int offset, byte[] value) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    long ts = kp.getNextLong();
    String tstype = kp.getNextString();
    if (eventTypes == null || eventTypes.contains(tstype)) {
      TimelineEvent event = new TimelineEvent();
      event.setTimestamp(ts);
      event.setEventType(tstype);
      Object o = null;
      try {
        o = fstConf.asObject(value);
      } catch (Exception ignore) {
        try {
          // Fall back to 2.24 parser
          o = fstConf224.asObject(value);
        } catch (Exception e) {
          LOG.warn("Error while decoding " + tstype, e);
        }
      }
      if (o == null) {
        event.setEventInfo(null);
      } else if (o instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) o;
        event.setEventInfo(m);
      } else {
        throw new IOException("Couldn't deserialize event info map");
      }
      return event;
    }
    return null;
  }

  /**
   * Parses the primary filter from the given key at the given offset and adds
   * it to the given entity.
   */
  private static void addPrimaryFilter(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String name = kp.getNextString();
    byte[] bytes = kp.getRemainingBytes();
    Object value = null;
    try {
      value = fstConf.asObject(bytes);
      entity.addPrimaryFilter(name, value);
    } catch (Exception ignore) {
      try {
        // Fall back to 2.24 parser
        value = fstConf224.asObject(bytes);
        entity.addPrimaryFilter(name, value);
      } catch (Exception e) {
        LOG.warn("Error while decoding " + name, e);
      }
    }
  }

  /**
   * Creates a string representation of the byte array from the given offset to
   * the end of the array (for parsing other info keys).
   */
  private static String parseRemainingKey(byte[] b, int offset) {
    return new String(b, offset, b.length - offset, UTF_8);
  }

  /**
   * Creates a related entity key, serializing ENTITY_ENTRY_PREFIX + entity type
   * + revstarttime + entity id + RELATED_ENTITIES_COLUMN + relatedentity type +
   * relatedentity id.
   */
  private static byte[] createRelatedEntityKey(String entityId,
      String entityType, byte[] revStartTime, String relatedEntityId,
      String relatedEntityType) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).add(RELATED_ENTITIES_COLUMN).add(relatedEntityType)
        .add(relatedEntityId).getBytes();
  }

  /**
   * Parses the related entity from the given key at the given offset and adds
   * it to the given entity.
   */
  private static void addRelatedEntity(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String type = kp.getNextString();
    String id = kp.getNextString();
    entity.addRelatedEntity(type, id);
  }

  /**
   * Creates a domain id key, serializing ENTITY_ENTRY_PREFIX + entity type +
   * revstarttime + entity id + DOMAIN_ID_COLUMN.
   */
  private static byte[] createDomainIdKey(String entityId, String entityType,
      byte[] revStartTime) throws IOException {
    return KeyBuilder.newInstance().add(entityType).add(revStartTime)
        .add(entityId).add(DOMAIN_ID_COLUMN).getBytes();
  }

  /**
   * Clears the cache to test reloading start times from leveldb (only for
   * testing).
   */
  @VisibleForTesting
  void clearStartTimeCache() {
    startTimeWriteCache.clear();
    startTimeReadCache.clear();
  }

  @VisibleForTesting
  static int getStartTimeReadCacheSize(Configuration conf) {
    return conf
        .getInt(
            TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
  }

  @VisibleForTesting
  static int getStartTimeWriteCacheSize(Configuration conf) {
    return conf
        .getInt(
            TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
  }

  @VisibleForTesting
  long evictOldStartTimes(long minStartTime) throws IOException {
    LOG.info("Searching for start times to evict earlier than " + minStartTime);

    long batchSize = 0;
    long totalCount = 0;
    long startTimesCount = 0;

    WriteBatch writeBatch = null;

    ReadOptions readOptions = new ReadOptions();
    readOptions.fillCache(false);
    try (DBIterator iterator = starttimedb.iterator(readOptions)) {

      // seek to the first start time entry
      iterator.seekToFirst();
      writeBatch = starttimedb.createWriteBatch();

      // evaluate each start time entry to see if it needs to be evicted or not
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> current = iterator.next();
        byte[] entityKey = current.getKey();
        byte[] entityValue = current.getValue();
        long startTime = readReverseOrderedLong(entityValue, 0);
        if (startTime < minStartTime) {
          ++batchSize;
          ++startTimesCount;
          writeBatch.delete(entityKey);

          // a large delete will hold the lock for too long
          if (batchSize >= writeBatchSize) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Preparing to delete a batch of " + batchSize
                  + " old start times");
            }
            starttimedb.write(writeBatch);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Deleted batch of " + batchSize
                  + ". Total start times deleted so far this cycle: "
                  + startTimesCount);
            }
            IOUtils.cleanupWithLogger(LOG, writeBatch);
            writeBatch = starttimedb.createWriteBatch();
            batchSize = 0;
          }
        }
        ++totalCount;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Preparing to delete a batch of " + batchSize
            + " old start times");
      }
      starttimedb.write(writeBatch);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleted batch of " + batchSize
            + ". Total start times deleted so far this cycle: "
            + startTimesCount);
      }
      LOG.info("Deleted " + startTimesCount + "/" + totalCount
          + " start time entities earlier than " + minStartTime);
    } finally {
      IOUtils.cleanupWithLogger(LOG, writeBatch);
    }
    return startTimesCount;
  }

  /**
   * Discards entities with start timestamp less than or equal to the given
   * timestamp.
   */
  @VisibleForTesting
  void discardOldEntities(long timestamp) throws IOException,
      InterruptedException {
    long totalCount = 0;
    long t1 = System.currentTimeMillis();
    try {
      totalCount += evictOldStartTimes(timestamp);
      indexdb.evictOldDBs();
      entitydb.evictOldDBs();
    } finally {
      long t2 = System.currentTimeMillis();
      LOG.info("Discarded " + totalCount + " entities for timestamp "
          + timestamp + " and earlier in " + (t2 - t1) / 1000.0 + " seconds");
    }
  }

  Version loadVersion() throws IOException {
    byte[] data = starttimedb.get(bytes(TIMELINE_STORE_VERSION_KEY));
    // if version is not stored previously, treat it as 1.0.
    if (data == null || data.length == 0) {
      return Version.newInstance(1, 0);
    }
    Version version = new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  // Only used for test
  @VisibleForTesting
  void storeVersion(Version state) throws IOException {
    dbStoreVersion(state);
  }

  private void dbStoreVersion(Version state) throws IOException {
    String key = TIMELINE_STORE_VERSION_KEY;
    byte[] data = ((VersionPBImpl) state).getProto().toByteArray();
    try {
      starttimedb.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning timeline store: major.minor. For e.g. 1.0, 1.1, 1.2...1.25,
   * 2.0 etc. 2) Any incompatible change of TS-store is a major upgrade, and any
   * compatible change of TS-store is a minor upgrade. 3) Within a minor
   * upgrade, say 1.1 to 1.2: overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0: throw exception and indicate
   * user to use a separate upgrade tool to upgrade timeline store or remove
   * incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded timeline store version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing timeline store version info " + getCurrentVersion());
      dbStoreVersion(CURRENT_VERSION_INFO);
    } else {
      String incompatibleMessage = "Incompatible version for timeline store: "
          + "expecting version " + getCurrentVersion()
          + ", but loading version " + loadedVersion;
      LOG.error(incompatibleMessage);
      throw new IOException(incompatibleMessage);
    }
  }

  // TODO: make data retention work with the domain data as well
  @Override
  public void put(TimelineDomain domain) throws IOException {
    try (WriteBatch domainWriteBatch = domaindb.createWriteBatch();
         WriteBatch ownerWriteBatch = ownerdb.createWriteBatch();) {

      if (domain.getId() == null || domain.getId().length() == 0) {
        throw new IllegalArgumentException("Domain doesn't have an ID");
      }
      if (domain.getOwner() == null || domain.getOwner().length() == 0) {
        throw new IllegalArgumentException("Domain doesn't have an owner.");
      }

      // Write description
      byte[] domainEntryKey = createDomainEntryKey(domain.getId(),
          DESCRIPTION_COLUMN);
      byte[] ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), DESCRIPTION_COLUMN);
      if (domain.getDescription() != null) {
        domainWriteBatch.put(domainEntryKey,
            domain.getDescription().getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey, domain.getDescription()
            .getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      // Write owner
      domainEntryKey = createDomainEntryKey(domain.getId(), OWNER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), OWNER_COLUMN);
      // Null check for owner is done before
      domainWriteBatch.put(domainEntryKey, domain.getOwner().getBytes(UTF_8));
      ownerWriteBatch.put(ownerLookupEntryKey, domain.getOwner()
          .getBytes(UTF_8));

      // Write readers
      domainEntryKey = createDomainEntryKey(domain.getId(), READER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), READER_COLUMN);
      if (domain.getReaders() != null && domain.getReaders().length() > 0) {
        domainWriteBatch.put(domainEntryKey, domain.getReaders()
            .getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey,
            domain.getReaders().getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      // Write writers
      domainEntryKey = createDomainEntryKey(domain.getId(), WRITER_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), WRITER_COLUMN);
      if (domain.getWriters() != null && domain.getWriters().length() > 0) {
        domainWriteBatch.put(domainEntryKey, domain.getWriters()
            .getBytes(UTF_8));
        ownerWriteBatch.put(ownerLookupEntryKey,
            domain.getWriters().getBytes(UTF_8));
      } else {
        domainWriteBatch.put(domainEntryKey, EMPTY_BYTES);
        ownerWriteBatch.put(ownerLookupEntryKey, EMPTY_BYTES);
      }

      // Write creation time and modification time
      // We put both timestamps together because they are always retrieved
      // together, and store them in the same way as we did for the entity's
      // start time and insert time.
      domainEntryKey = createDomainEntryKey(domain.getId(), TIMESTAMP_COLUMN);
      ownerLookupEntryKey = createOwnerLookupKey(domain.getOwner(),
          domain.getId(), TIMESTAMP_COLUMN);
      long currentTimestamp = System.currentTimeMillis();
      byte[] timestamps = domaindb.get(domainEntryKey);
      if (timestamps == null) {
        timestamps = new byte[16];
        writeReverseOrderedLong(currentTimestamp, timestamps, 0);
        writeReverseOrderedLong(currentTimestamp, timestamps, 8);
      } else {
        writeReverseOrderedLong(currentTimestamp, timestamps, 8);
      }
      domainWriteBatch.put(domainEntryKey, timestamps);
      ownerWriteBatch.put(ownerLookupEntryKey, timestamps);
      domaindb.write(domainWriteBatch);
      ownerdb.write(ownerWriteBatch);
    }
  }

  /**
   * Creates a domain entity key with column name suffix, of the form
   * DOMAIN_ENTRY_PREFIX + domain id + column name.
   */
  private static byte[] createDomainEntryKey(String domainId, byte[] columnName)
      throws IOException {
    return KeyBuilder.newInstance().add(domainId).add(columnName).getBytes();
  }

  /**
   * Creates an owner lookup key with column name suffix, of the form
   * OWNER_LOOKUP_PREFIX + owner + domain id + column name.
   */
  private static byte[] createOwnerLookupKey(String owner, String domainId,
      byte[] columnName) throws IOException {
    return KeyBuilder.newInstance().add(owner).add(domainId).add(columnName)
        .getBytes();
  }

  @Override
  public TimelineDomain getDomain(String domainId) throws IOException {
    try (DBIterator iterator = domaindb.iterator()) {
      byte[] prefix = KeyBuilder.newInstance().add(domainId)
          .getBytesForLookup();
      iterator.seek(prefix);
      return getTimelineDomain(iterator, domainId, prefix);
    }
  }

  @Override
  public TimelineDomains getDomains(String owner) throws IOException {
    try (DBIterator iterator = ownerdb.iterator()) {
      byte[] prefix = KeyBuilder.newInstance().add(owner).getBytesForLookup();
      iterator.seek(prefix);
      List<TimelineDomain> domains = new ArrayList<TimelineDomain>();
      while (iterator.hasNext()) {
        byte[] key = iterator.peekNext().getKey();
        if (!prefixMatches(prefix, prefix.length, key)) {
          break;
        }
        // Iterator to parse the rows of an individual domain
        KeyParser kp = new KeyParser(key, prefix.length);
        String domainId = kp.getNextString();
        byte[] prefixExt = KeyBuilder.newInstance().add(owner).add(domainId)
            .getBytesForLookup();
        TimelineDomain domainToReturn = getTimelineDomain(iterator, domainId,
            prefixExt);
        if (domainToReturn != null) {
          domains.add(domainToReturn);
        }
      }
      // Sort the domains to return
      Collections.sort(domains, new Comparator<TimelineDomain>() {
        @Override
        public int compare(TimelineDomain domain1, TimelineDomain domain2) {
          int result = domain2.getCreatedTime().compareTo(
              domain1.getCreatedTime());
          if (result == 0) {
            return domain2.getModifiedTime().compareTo(
                domain1.getModifiedTime());
          } else {
            return result;
          }
        }
      });
      TimelineDomains domainsToReturn = new TimelineDomains();
      domainsToReturn.addDomains(domains);
      return domainsToReturn;
    }
  }

  private static TimelineDomain getTimelineDomain(DBIterator iterator,
      String domainId, byte[] prefix) throws IOException {
    // Iterate over all the rows whose key starts with prefix to retrieve the
    // domain information.
    TimelineDomain domain = new TimelineDomain();
    domain.setId(domainId);
    boolean noRows = true;
    for (; iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefix.length, key)) {
        break;
      }
      if (noRows) {
        noRows = false;
      }
      byte[] value = iterator.peekNext().getValue();
      if (value != null && value.length > 0) {
        if (key[prefix.length] == DESCRIPTION_COLUMN[0]) {
          domain.setDescription(new String(value, UTF_8));
        } else if (key[prefix.length] == OWNER_COLUMN[0]) {
          domain.setOwner(new String(value, UTF_8));
        } else if (key[prefix.length] == READER_COLUMN[0]) {
          domain.setReaders(new String(value, UTF_8));
        } else if (key[prefix.length] == WRITER_COLUMN[0]) {
          domain.setWriters(new String(value, UTF_8));
        } else if (key[prefix.length] == TIMESTAMP_COLUMN[0]) {
          domain.setCreatedTime(readReverseOrderedLong(value, 0));
          domain.setModifiedTime(readReverseOrderedLong(value, 8));
        } else {
          LOG.error("Unrecognized domain column: " + key[prefix.length]);
        }
      }
    }
    if (noRows) {
      return null;
    } else {
      return domain;
    }
  }
}
