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

import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.timeline.GenericObjectMapper.writeReverseOrderedLong;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import com.google.common.annotations.VisibleForTesting;

/**
 * <p>An implementation of an application timeline store backed by leveldb.</p>
 *
 * <p>There are three sections of the db, the start time section,
 * the entity section, and the indexed entity section.</p>
 *
 * <p>The start time section is used to retrieve the unique start time for
 * a given entity. Its values each contain a start time while its keys are of
 * the form:</p>
 * <pre>
 *   START_TIME_LOOKUP_PREFIX + entity type + entity id</pre>
 *
 * <p>The entity section is ordered by entity type, then entity start time
 * descending, then entity ID. There are four sub-sections of the entity
 * section: events, primary filters, related entities,
 * and other info. The event entries have event info serialized into their
 * values. The other info entries have values corresponding to the values of
 * the other info name/value map for the entry (note the names are contained
 * in the key). All other entries have empty values. The key structure is as
 * follows:</p>
 * <pre>
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id
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
 *
 *   ENTITY_ENTRY_PREFIX + entity type + revstarttime + entity id +
 *     INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN + relatedentity type +
 *     relatedentity id</pre>
 *
 * <p>The indexed entity section contains a primary filter name and primary
 * filter value as the prefix. Within a given name/value, entire entity
 * entries are stored in the same format as described in the entity section
 * above (below, "key" represents any one of the possible entity entry keys
 * described above).</p>
 * <pre>
 *   INDEXED_ENTRY_PREFIX + primaryfilter name + primaryfilter value +
 *     key</pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LeveldbTimelineStore extends AbstractService
    implements TimelineStore {
  private static final Log LOG = LogFactory
      .getLog(LeveldbTimelineStore.class);

  @Private
  @VisibleForTesting
  static final String FILENAME = "leveldb-timeline-store.ldb";

  private static final byte[] START_TIME_LOOKUP_PREFIX = "k".getBytes();
  private static final byte[] ENTITY_ENTRY_PREFIX = "e".getBytes();
  private static final byte[] INDEXED_ENTRY_PREFIX = "i".getBytes();

  private static final byte[] EVENTS_COLUMN = "e".getBytes();
  private static final byte[] PRIMARY_FILTERS_COLUMN = "f".getBytes();
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes();
  private static final byte[] RELATED_ENTITIES_COLUMN = "r".getBytes();
  private static final byte[] INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN =
      "z".getBytes();

  private static final byte[] EMPTY_BYTES = new byte[0];

  @Private
  @VisibleForTesting
  static final FsPermission LEVELDB_DIR_UMASK = FsPermission
      .createImmutable((short) 0700);

  private Map<EntityIdentifier, StartAndInsertTime> startTimeWriteCache;
  private Map<EntityIdentifier, Long> startTimeReadCache;

  /**
   * Per-entity locks are obtained when writing.
   */
  private final LockMap<EntityIdentifier> writeLocks =
      new LockMap<EntityIdentifier>();

  private final ReentrantReadWriteLock deleteLock =
      new ReentrantReadWriteLock();

  private DB db;

  private Thread deletionThread;

  public LeveldbTimelineStore() {
    super(LeveldbTimelineStore.class.getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void serviceInit(Configuration conf) throws Exception {
    Options options = new Options();
    options.createIfMissing(true);
    options.cacheSize(conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_READ_CACHE_SIZE));
    JniDBFactory factory = new JniDBFactory();
    Path dbPath = new Path(
        conf.get(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH), FILENAME);
    FileSystem localFS = null;
    try {
      localFS = FileSystem.getLocal(conf);
      if (!localFS.exists(dbPath)) {
        if (!localFS.mkdirs(dbPath)) {
          throw new IOException("Couldn't create directory for leveldb " +
              "timeline store " + dbPath);
        }
        localFS.setPermission(dbPath, LEVELDB_DIR_UMASK);
      }
    } finally {
      IOUtils.cleanup(LOG, localFS);
    }
    LOG.info("Using leveldb path " + dbPath);
    db = factory.open(new File(dbPath.toString()), options);
    startTimeWriteCache =
        Collections.synchronizedMap(new LRUMap(getStartTimeWriteCacheSize(
            conf)));
    startTimeReadCache =
        Collections.synchronizedMap(new LRUMap(getStartTimeReadCacheSize(
            conf)));

    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, true)) {
      deletionThread = new EntityDeletionThread(conf);
      deletionThread.start();
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (deletionThread != null) {
      deletionThread.interrupt();
      LOG.info("Waiting for deletion thread to complete its current action");
      try {
        deletionThread.join();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for deletion thread to complete," +
            " closing db now", e);
      }
    }
    IOUtils.cleanup(LOG, db);
    super.serviceStop();
  }

  private static class StartAndInsertTime {
    final long startTime;
    final long insertTime;

    public StartAndInsertTime(long startTime, long insertTime) {
      this.startTime = startTime;
      this.insertTime = insertTime;
    }
  }

  private class EntityDeletionThread extends Thread {
    private final long ttl;
    private final long ttlInterval;

    public EntityDeletionThread(Configuration conf) {
      ttl  = conf.getLong(YarnConfiguration.TIMELINE_SERVICE_TTL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_TTL_MS);
      ttlInterval = conf.getLong(
          YarnConfiguration.TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_LEVELDB_TTL_INTERVAL_MS);
      LOG.info("Starting deletion thread with ttl " + ttl + " and cycle " +
          "interval " + ttlInterval);
    }

    @Override
    public void run() {
      while (true) {
        long timestamp = System.currentTimeMillis() - ttl;
        try {
          discardOldEntities(timestamp);
          Thread.sleep(ttlInterval);
        } catch (IOException e) {
          LOG.error(e);
        } catch (InterruptedException e) {
          LOG.info("Deletion thread received interrupt, exiting");
          break;
        }
      }
    }
  }

  private static class LockMap<K> {
    private static class CountingReentrantLock<K> extends ReentrantLock {
      private static final long serialVersionUID = 1L;
      private int count;
      private K key;

      CountingReentrantLock(K key) {
        super();
        this.count = 0;
        this.key = key;
      }
    }

    private Map<K, CountingReentrantLock<K>> locks =
        new HashMap<K, CountingReentrantLock<K>>();

    synchronized CountingReentrantLock<K> getLock(K key) {
      CountingReentrantLock<K> lock = locks.get(key);
      if (lock == null) {
        lock = new CountingReentrantLock<K>(key);
        locks.put(key, lock);
      }

      lock.count++;
      return lock;
    }

    synchronized void returnLock(CountingReentrantLock<K> lock) {
      if (lock.count == 0) {
        throw new IllegalStateException("Returned lock more times than it " +
            "was retrieved");
      }
      lock.count--;

      if (lock.count == 0) {
        locks.remove(lock.key);
      }
    }
  }

  private static class KeyBuilder {
    private static final int MAX_NUMBER_OF_KEY_ELEMENTS = 10;
    private byte[][] b;
    private boolean[] useSeparator;
    private int index;
    private int length;

    public KeyBuilder(int size) {
      b = new byte[size][];
      useSeparator = new boolean[size];
      index = 0;
      length = 0;
    }

    public static KeyBuilder newInstance() {
      return new KeyBuilder(MAX_NUMBER_OF_KEY_ELEMENTS);
    }

    public KeyBuilder add(String s) {
      return add(s.getBytes(), true);
    }

    public KeyBuilder add(byte[] t) {
      return add(t, false);
    }

    public KeyBuilder add(byte[] t, boolean sep) {
      b[index] = t;
      useSeparator[index] = sep;
      length += t.length;
      if (sep) {
        length++;
      }
      index++;
      return this;
    }

    public byte[] getBytes() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (i < index-1 && useSeparator[i]) {
          baos.write(0x0);
        }
      }
      return baos.toByteArray();
    }

    public byte[] getBytesForLookup() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (useSeparator[i]) {
          baos.write(0x0);
        }
      }
      return baos.toByteArray();
    }
  }

  private static class KeyParser {
    private final byte[] b;
    private int offset;

    public KeyParser(byte[] b, int offset) {
      this.b = b;
      this.offset = offset;
    }

    public String getNextString() throws IOException {
      if (offset >= b.length) {
        throw new IOException(
            "tried to read nonexistent string from byte array");
      }
      int i = 0;
      while (offset+i < b.length && b[offset+i] != 0x0) {
        i++;
      }
      String s = new String(b, offset, i);
      offset = offset + i + 1;
      return s;
    }

    public long getNextLong() throws IOException {
      if (offset+8 >= b.length) {
        throw new IOException("byte array ran out when trying to read long");
      }
      long l = readReverseOrderedLong(b, offset);
      offset += 8;
      return l;
    }

    public int getOffset() {
      return offset;
    }
  }

  @Override
  public TimelineEntity getEntity(String entityId, String entityType,
      EnumSet<Field> fields) throws IOException {
    Long revStartTime = getStartTimeLong(entityId, entityType);
    if (revStartTime == null) {
      return null;
    }
    byte[] prefix = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
        .add(entityType).add(writeReverseOrderedLong(revStartTime))
        .add(entityId).getBytesForLookup();

    DBIterator iterator = null;
    try {
      iterator = db.iterator();
      iterator.seek(prefix);

      return getEntity(entityId, entityType, revStartTime, fields, iterator,
          prefix, prefix.length);
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  /**
   * Read entity from a db iterator.  If no information is found in the
   * specified fields for this entity, return null.
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
          addPrimaryFilter(entity, key,
              prefixlen + PRIMARY_FILTERS_COLUMN.length);
        }
      } else if (key[prefixlen] == OTHER_INFO_COLUMN[0]) {
        if (otherInfo) {
          entity.addOtherInfo(parseRemainingKey(key,
              prefixlen + OTHER_INFO_COLUMN.length),
              GenericObjectMapper.read(iterator.peekNext().getValue()));
        }
      } else if (key[prefixlen] == RELATED_ENTITIES_COLUMN[0]) {
        if (relatedEntities) {
          addRelatedEntity(entity, key,
              prefixlen + RELATED_ENTITIES_COLUMN.length);
        }
      } else if (key[prefixlen] == EVENTS_COLUMN[0]) {
        if (events || (lastEvent &&
            entity.getEvents().size() == 0)) {
          TimelineEvent event = getEntityEvent(null, key, prefixlen +
              EVENTS_COLUMN.length, iterator.peekNext().getValue());
          if (event != null) {
            entity.addEvent(event);
          }
        }
      } else {
        if (key[prefixlen] !=
            INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN[0]) {
          LOG.warn(String.format("Found unexpected column for entity %s of " +
              "type %s (0x%02x)", entityId, entityType, key[prefixlen]));
        }
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
    Map<byte[], List<EntityIdentifier>> startTimeMap = new TreeMap<byte[],
        List<EntityIdentifier>>(new Comparator<byte[]>() {
          @Override
          public int compare(byte[] o1, byte[] o2) {
            return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0,
                o2.length);
          }
        });
    DBIterator iterator = null;
    try {
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
      for (Entry<byte[], List<EntityIdentifier>> entry :
          startTimeMap.entrySet()) {
        // look up the events matching the given parameters (limit,
        // start time, end time, event types) for entities whose start times
        // were found and add the entities to the return list
        byte[] revStartTime = entry.getKey();
        for (EntityIdentifier entityIdentifier : entry.getValue()) {
          EventsOfOneEntity entity = new EventsOfOneEntity();
          entity.setEntityId(entityIdentifier.getId());
          entity.setEntityType(entityType);
          events.addEvent(entity);
          KeyBuilder kb = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
              .add(entityType).add(revStartTime).add(entityIdentifier.getId())
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
          iterator = db.iterator();
          for (iterator.seek(first); entity.getEvents().size() < limit &&
              iterator.hasNext(); iterator.next()) {
            byte[] key = iterator.peekNext().getKey();
            if (!prefixMatches(prefix, prefix.length, key) || (last != null &&
                WritableComparator.compareBytes(key, 0, key.length, last, 0,
                    last.length) > 0)) {
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
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
    return events;
  }

  /**
   * Returns true if the byte array begins with the specified prefix.
   */
  private static boolean prefixMatches(byte[] prefix, int prefixlen,
      byte[] b) {
    if (b.length < prefixlen) {
      return false;
    }
    return WritableComparator.compareBytes(prefix, 0, prefixlen, b, 0,
        prefixlen) == 0;
  }

  @Override
  public TimelineEntities getEntities(String entityType,
      Long limit, Long windowStart, Long windowEnd, String fromId, Long fromTs,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) throws IOException {
    if (primaryFilter == null) {
      // if no primary filter is specified, prefix the lookup with
      // ENTITY_ENTRY_PREFIX
      return getEntityByTime(ENTITY_ENTRY_PREFIX, entityType, limit,
          windowStart, windowEnd, fromId, fromTs, secondaryFilters, fields);
    } else {
      // if a primary filter is specified, prefix the lookup with
      // INDEXED_ENTRY_PREFIX + primaryFilterName + primaryFilterValue +
      // ENTITY_ENTRY_PREFIX
      byte[] base = KeyBuilder.newInstance().add(INDEXED_ENTRY_PREFIX)
          .add(primaryFilter.getName())
          .add(GenericObjectMapper.write(primaryFilter.getValue()), true)
          .add(ENTITY_ENTRY_PREFIX).getBytesForLookup();
      return getEntityByTime(base, entityType, limit, windowStart, windowEnd,
          fromId, fromTs, secondaryFilters, fields);
    }
  }

  /**
   * Retrieves a list of entities satisfying given parameters.
   *
   * @param base A byte array prefix for the lookup
   * @param entityType The type of the entity
   * @param limit A limit on the number of entities to return
   * @param starttime The earliest entity start time to retrieve (exclusive)
   * @param endtime The latest entity start time to retrieve (inclusive)
   * @param fromId Retrieve entities starting with this entity
   * @param fromTs Ignore entities with insert timestamp later than this ts
   * @param secondaryFilters Filter pairs that the entities should match
   * @param fields The set of fields to retrieve
   * @return A list of entities
   * @throws IOException
   */
  private TimelineEntities getEntityByTime(byte[] base,
      String entityType, Long limit, Long starttime, Long endtime,
      String fromId, Long fromTs, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) throws IOException {
    DBIterator iterator = null;
    try {
      KeyBuilder kb = KeyBuilder.newInstance().add(base).add(entityType);
      // only db keys matching the prefix (base + entity type) will be parsed
      byte[] prefix = kb.getBytesForLookup();
      if (endtime == null) {
        // if end time is null, place no restriction on end time
        endtime = Long.MAX_VALUE;
      }
      // construct a first key that will be seeked to using end time or fromId
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
          first = kb.add(writeReverseOrderedLong(fromIdStartTime))
              .add(fromId).getBytesForLookup();
        }
      }
      // if seek key wasn't constructed using fromId, construct it using end ts
      if (first == null) {
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
      iterator = db.iterator();
      iterator.seek(first);
      // iterate until one of the following conditions is met: limit is
      // reached, there are no more keys, the key prefix no longer matches,
      // or a start time has been specified and reached/exceeded
      while (entities.getEntities().size() < limit && iterator.hasNext()) {
        byte[] key = iterator.peekNext().getKey();
        if (!prefixMatches(prefix, prefix.length, key) || (last != null &&
            WritableComparator.compareBytes(key, 0, key.length, last, 0,
                last.length) > 0)) {
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
            while (iterator.hasNext() && prefixMatches(firstKey,
                kp.getOffset(), key)) {
              iterator.next();
              key = iterator.peekNext().getKey();
            }
            continue;
          }
        }

        // parse the entity that owns this key, iterating over all keys for
        // the entity
        TimelineEntity entity = getEntity(entityId, entityType, startTime,
            fields, iterator, key, kp.getOffset());
        // determine if the retrieved entity matches the provided secondary
        // filters, and if so add it to the list of entities to return
        boolean filterPassed = true;
        if (secondaryFilters != null) {
          for (NameValuePair filter : secondaryFilters) {
            Object v = entity.getOtherInfo().get(filter.getName());
            if (v == null) {
              Set<Object> vs = entity.getPrimaryFilters()
                  .get(filter.getName());
              if (vs != null && !vs.contains(filter.getValue())) {
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
          entities.addEntity(entity);
        }
      }
      return entities;
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  /**
   * Put a single entity.  If there is an error, add a TimelinePutError to the
   * given response.
   */
  private void put(TimelineEntity entity, TimelinePutResponse response) {
    LockMap.CountingReentrantLock<EntityIdentifier> lock =
        writeLocks.getLock(new EntityIdentifier(entity.getEntityId(),
            entity.getEntityType()));
    lock.lock();
    WriteBatch writeBatch = null;
    List<EntityIdentifier> relatedEntitiesWithoutStartTimes =
        new ArrayList<EntityIdentifier>();
    byte[] revStartTime = null;
    try {
      writeBatch = db.createWriteBatch();
      List<TimelineEvent> events = entity.getEvents();
      // look up the start time for the entity
      StartAndInsertTime startAndInsertTime = getAndSetStartTime(
          entity.getEntityId(), entity.getEntityType(),
          entity.getStartTime(), events);
      if (startAndInsertTime == null) {
        // if no start time is found, add an error and return
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.NO_START_TIME);
        response.addError(error);
        return;
      }
      revStartTime = writeReverseOrderedLong(startAndInsertTime
          .startTime);

      Map<String, Set<Object>> primaryFilters = entity.getPrimaryFilters();

      // write entity marker
      byte[] markerKey = createEntityMarkerKey(entity.getEntityId(),
          entity.getEntityType(), revStartTime);
      byte[] markerValue = writeReverseOrderedLong(startAndInsertTime
          .insertTime);
      writeBatch.put(markerKey, markerValue);
      writePrimaryFilterEntries(writeBatch, primaryFilters, markerKey,
          markerValue);

      // write event entries
      if (events != null && !events.isEmpty()) {
        for (TimelineEvent event : events) {
          byte[] revts = writeReverseOrderedLong(event.getTimestamp());
          byte[] key = createEntityEventKey(entity.getEntityId(),
              entity.getEntityType(), revStartTime, revts,
              event.getEventType());
          byte[] value = GenericObjectMapper.write(event.getEventInfo());
          writeBatch.put(key, value);
          writePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
        }
      }

      // write related entity entries
      Map<String, Set<String>> relatedEntities =
          entity.getRelatedEntities();
      if (relatedEntities != null && !relatedEntities.isEmpty()) {
        for (Entry<String, Set<String>> relatedEntityList :
            relatedEntities.entrySet()) {
          String relatedEntityType = relatedEntityList.getKey();
          for (String relatedEntityId : relatedEntityList.getValue()) {
            // invisible "reverse" entries (entity -> related entity)
            byte[] key = createReverseRelatedEntityKey(entity.getEntityId(),
                entity.getEntityType(), revStartTime, relatedEntityId,
                relatedEntityType);
            writeBatch.put(key, EMPTY_BYTES);
            // look up start time of related entity
            byte[] relatedEntityStartTime = getStartTime(relatedEntityId,
                relatedEntityType);
            // delay writing the related entity if no start time is found
            if (relatedEntityStartTime == null) {
              relatedEntitiesWithoutStartTimes.add(
                  new EntityIdentifier(relatedEntityId, relatedEntityType));
              continue;
            }
            // write "forward" entry (related entity -> entity)
            key = createRelatedEntityKey(relatedEntityId,
                relatedEntityType, relatedEntityStartTime,
                entity.getEntityId(), entity.getEntityType());
            writeBatch.put(key, EMPTY_BYTES);
          }
        }
      }

      // write primary filter entries
      if (primaryFilters != null && !primaryFilters.isEmpty()) {
        for (Entry<String, Set<Object>> primaryFilter :
            primaryFilters.entrySet()) {
          for (Object primaryFilterValue : primaryFilter.getValue()) {
            byte[] key = createPrimaryFilterKey(entity.getEntityId(),
                entity.getEntityType(), revStartTime,
                primaryFilter.getKey(), primaryFilterValue);
            writeBatch.put(key, EMPTY_BYTES);
            writePrimaryFilterEntries(writeBatch, primaryFilters, key,
                EMPTY_BYTES);
          }
        }
      }

      // write other info entries
      Map<String, Object> otherInfo = entity.getOtherInfo();
      if (otherInfo != null && !otherInfo.isEmpty()) {
        for (Entry<String, Object> i : otherInfo.entrySet()) {
          byte[] key = createOtherInfoKey(entity.getEntityId(),
              entity.getEntityType(), revStartTime, i.getKey());
          byte[] value = GenericObjectMapper.write(i.getValue());
          writeBatch.put(key, value);
          writePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
        }
      }
      db.write(writeBatch);
    } catch (IOException e) {
      LOG.error("Error putting entity " + entity.getEntityId() +
          " of type " + entity.getEntityType(), e);
      TimelinePutError error = new TimelinePutError();
      error.setEntityId(entity.getEntityId());
      error.setEntityType(entity.getEntityType());
      error.setErrorCode(TimelinePutError.IO_EXCEPTION);
      response.addError(error);
    } finally {
      lock.unlock();
      writeLocks.returnLock(lock);
      IOUtils.cleanup(LOG, writeBatch);
    }

    for (EntityIdentifier relatedEntity : relatedEntitiesWithoutStartTimes) {
      lock = writeLocks.getLock(relatedEntity);
      lock.lock();
      try {
        StartAndInsertTime relatedEntityStartAndInsertTime =
            getAndSetStartTime(relatedEntity.getId(), relatedEntity.getType(),
            readReverseOrderedLong(revStartTime, 0), null);
        if (relatedEntityStartAndInsertTime == null) {
          throw new IOException("Error setting start time for related entity");
        }
        byte[] relatedEntityStartTime = writeReverseOrderedLong(
            relatedEntityStartAndInsertTime.startTime);
        db.put(createRelatedEntityKey(relatedEntity.getId(),
            relatedEntity.getType(), relatedEntityStartTime,
            entity.getEntityId(), entity.getEntityType()), EMPTY_BYTES);
        db.put(createEntityMarkerKey(relatedEntity.getId(),
            relatedEntity.getType(), relatedEntityStartTime),
            writeReverseOrderedLong(relatedEntityStartAndInsertTime
                .insertTime));
      } catch (IOException e) {
        LOG.error("Error putting related entity " + relatedEntity.getId() +
            " of type " + relatedEntity.getType() + " for entity " +
            entity.getEntityId() + " of type " + entity.getEntityType(), e);
        TimelinePutError error = new TimelinePutError();
        error.setEntityId(entity.getEntityId());
        error.setEntityType(entity.getEntityType());
        error.setErrorCode(TimelinePutError.IO_EXCEPTION);
        response.addError(error);
      } finally {
        lock.unlock();
        writeLocks.returnLock(lock);
      }
    }
  }

  /**
   * For a given key / value pair that has been written to the db,
   * write additional entries to the db for each primary filter.
   */
  private static void writePrimaryFilterEntries(WriteBatch writeBatch,
      Map<String, Set<Object>> primaryFilters, byte[] key, byte[] value)
      throws IOException {
    if (primaryFilters != null && !primaryFilters.isEmpty()) {
      for (Entry<String, Set<Object>> pf : primaryFilters.entrySet()) {
        for (Object pfval : pf.getValue()) {
          writeBatch.put(addPrimaryFilterToKey(pf.getKey(), pfval,
              key), value);
        }
      }
    }
  }

  @Override
  public TimelinePutResponse put(TimelineEntities entities) {
    try {
      deleteLock.readLock().lock();
      TimelinePutResponse response = new TimelinePutResponse();
      for (TimelineEntity entity : entities.getEntities()) {
        put(entity, response);
      }
      return response;
    } finally {
      deleteLock.readLock().unlock();
    }
  }

  /**
   * Get the unique start time for a given entity as a byte array that sorts
   * the timestamps in reverse order (see {@link
   * GenericObjectMapper#writeReverseOrderedLong(long)}).
   *
   * @param entityId The id of the entity
   * @param entityType The type of the entity
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
   * @param entityId The id of the entity
   * @param entityType The type of the entity
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
      byte[] v = db.get(b);
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
   * Get the unique start time for a given entity as a byte array that sorts
   * the timestamps in reverse order (see {@link
   * GenericObjectMapper#writeReverseOrderedLong(long)}). If the start time
   * doesn't exist, set it based on the information provided. Should only be
   * called when a lock has been obtained on the entity.
   *
   * @param entityId The id of the entity
   * @param entityType The type of the entity
   * @param startTime The start time of the entity, or null
   * @param events A list of events for the entity, or null
   * @return A StartAndInsertTime
   * @throws IOException
   */
  private StartAndInsertTime getAndSetStartTime(String entityId,
      String entityType, Long startTime, List<TimelineEvent> events)
      throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    if (startTime == null) {
      // start time is not provided, so try to look it up
      if (startTimeWriteCache.containsKey(entity)) {
        // found the start time in the cache
        return startTimeWriteCache.get(entity);
      } else {
        if (events != null) {
          // prepare a start time from events in case it is needed
          Long min = Long.MAX_VALUE;
          for (TimelineEvent e : events) {
            if (min > e.getTimestamp()) {
              min = e.getTimestamp();
            }
          }
          startTime = min;
        }
        return checkStartTimeInDb(entity, startTime);
      }
    } else {
      // start time is provided
      if (startTimeWriteCache.containsKey(entity)) {
        // always use start time from cache if it exists
        return startTimeWriteCache.get(entity);
      } else {
        // check the provided start time matches the db
        return checkStartTimeInDb(entity, startTime);
      }
    }
  }

  /**
   * Checks db for start time and returns it if it exists.  If it doesn't
   * exist, writes the suggested start time (if it is not null).  This is
   * only called when the start time is not found in the cache,
   * so it adds it back into the cache if it is found. Should only be called
   * when a lock has been obtained on the entity.
   */
  private StartAndInsertTime checkStartTimeInDb(EntityIdentifier entity,
      Long suggestedStartTime) throws IOException {
    StartAndInsertTime startAndInsertTime = null;
    // create lookup key for start time
    byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
    // retrieve value for key
    byte[] v = db.get(b);
    if (v == null) {
      // start time doesn't exist in db
      if (suggestedStartTime == null) {
        return null;
      }
      startAndInsertTime = new StartAndInsertTime(suggestedStartTime,
          System.currentTimeMillis());

      // write suggested start time
      v = new byte[16];
      writeReverseOrderedLong(suggestedStartTime, v, 0);
      writeReverseOrderedLong(startAndInsertTime.insertTime, v, 8);
      WriteOptions writeOptions = new WriteOptions();
      writeOptions.sync(true);
      db.put(b, v, writeOptions);
    } else {
      // found start time in db, so ignore suggested start time
      startAndInsertTime = new StartAndInsertTime(readReverseOrderedLong(v, 0),
          readReverseOrderedLong(v, 8));
    }
    startTimeWriteCache.put(entity, startAndInsertTime);
    startTimeReadCache.put(entity, startAndInsertTime.startTime);
    return startAndInsertTime;
  }

  /**
   * Creates a key for looking up the start time of a given entity,
   * of the form START_TIME_LOOKUP_PREFIX + entity type + entity id.
   */
  private static byte[] createStartTimeLookupKey(String entityId,
      String entityType) throws IOException {
    return KeyBuilder.newInstance().add(START_TIME_LOOKUP_PREFIX)
        .add(entityType).add(entityId).getBytes();
  }

  /**
   * Creates an entity marker, serializing ENTITY_ENTRY_PREFIX + entity type +
   * revstarttime + entity id.
   */
  private static byte[] createEntityMarkerKey(String entityId,
      String entityType, byte[] revStartTime) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
        .add(entityType).add(revStartTime).add(entityId).getBytesForLookup();
  }

  /**
   * Creates an index entry for the given key of the form
   * INDEXED_ENTRY_PREFIX + primaryfiltername + primaryfiltervalue + key.
   */
  private static byte[] addPrimaryFilterToKey(String primaryFilterName,
      Object primaryFilterValue, byte[] key) throws IOException {
    return KeyBuilder.newInstance().add(INDEXED_ENTRY_PREFIX)
        .add(primaryFilterName)
        .add(GenericObjectMapper.write(primaryFilterValue), true).add(key)
        .getBytes();
  }

  /**
   * Creates an event key, serializing ENTITY_ENTRY_PREFIX + entity type +
   * revstarttime + entity id + EVENTS_COLUMN + reveventtimestamp + event type.
   */
  private static byte[] createEntityEventKey(String entityId,
      String entityType, byte[] revStartTime, byte[] revEventTimestamp,
      String eventType) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
        .add(entityType).add(revStartTime).add(entityId).add(EVENTS_COLUMN)
        .add(revEventTimestamp).add(eventType).getBytes();
  }

  /**
   * Creates an event object from the given key, offset, and value.  If the
   * event type is not contained in the specified set of event types,
   * returns null.
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
      Object o = GenericObjectMapper.read(value);
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
   * Creates a primary filter key, serializing ENTITY_ENTRY_PREFIX +
   * entity type + revstarttime + entity id + PRIMARY_FILTERS_COLUMN + name +
   * value.
   */
  private static byte[] createPrimaryFilterKey(String entityId,
      String entityType, byte[] revStartTime, String name, Object value)
      throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entityType)
        .add(revStartTime).add(entityId).add(PRIMARY_FILTERS_COLUMN).add(name)
        .add(GenericObjectMapper.write(value)).getBytes();
  }

  /**
   * Parses the primary filter from the given key at the given offset and
   * adds it to the given entity.
   */
  private static void addPrimaryFilter(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String name = kp.getNextString();
    Object value = GenericObjectMapper.read(key, kp.getOffset());
    entity.addPrimaryFilter(name, value);
  }

  /**
   * Creates an other info key, serializing ENTITY_ENTRY_PREFIX + entity type +
   * revstarttime + entity id + OTHER_INFO_COLUMN + name.
   */
  private static byte[] createOtherInfoKey(String entityId, String entityType,
      byte[] revStartTime, String name) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entityType)
        .add(revStartTime).add(entityId).add(OTHER_INFO_COLUMN).add(name)
        .getBytes();
  }

  /**
   * Creates a string representation of the byte array from the given offset
   * to the end of the array (for parsing other info keys).
   */
  private static String parseRemainingKey(byte[] b, int offset) {
    return new String(b, offset, b.length - offset);
  }

  /**
   * Creates a related entity key, serializing ENTITY_ENTRY_PREFIX +
   * entity type + revstarttime + entity id + RELATED_ENTITIES_COLUMN +
   * relatedentity type + relatedentity id.
   */
  private static byte[] createRelatedEntityKey(String entityId,
      String entityType, byte[] revStartTime, String relatedEntityId,
      String relatedEntityType) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entityType)
        .add(revStartTime).add(entityId).add(RELATED_ENTITIES_COLUMN)
        .add(relatedEntityType).add(relatedEntityId).getBytes();
  }

  /**
   * Parses the related entity from the given key at the given offset and
   * adds it to the given entity.
   */
  private static void addRelatedEntity(TimelineEntity entity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String type = kp.getNextString();
    String id = kp.getNextString();
    entity.addRelatedEntity(type, id);
  }

  /**
   * Creates a reverse related entity key, serializing ENTITY_ENTRY_PREFIX +
   * entity type + revstarttime + entity id +
   * INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN +
   * relatedentity type + relatedentity id.
   */
  private static byte[] createReverseRelatedEntityKey(String entityId,
      String entityType, byte[] revStartTime, String relatedEntityId,
      String relatedEntityType) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entityType)
        .add(revStartTime).add(entityId)
        .add(INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN)
        .add(relatedEntityType).add(relatedEntityId).getBytes();
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
    return conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_READ_CACHE_SIZE);
  }

  @VisibleForTesting
  static int getStartTimeWriteCacheSize(Configuration conf) {
    return conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE,
        YarnConfiguration.
            DEFAULT_TIMELINE_SERVICE_LEVELDB_START_TIME_WRITE_CACHE_SIZE);
  }

  // warning is suppressed to prevent eclipse from noting unclosed resource
  @SuppressWarnings("resource")
  @VisibleForTesting
  List<String> getEntityTypes() throws IOException {
    DBIterator iterator = null;
    try {
      iterator = getDbIterator(false);
      List<String> entityTypes = new ArrayList<String>();
      iterator.seek(ENTITY_ENTRY_PREFIX);
      while (iterator.hasNext()) {
        byte[] key = iterator.peekNext().getKey();
        if (key[0] != ENTITY_ENTRY_PREFIX[0]) {
          break;
        }
        KeyParser kp = new KeyParser(key,
            ENTITY_ENTRY_PREFIX.length);
        String entityType = kp.getNextString();
        entityTypes.add(entityType);
        byte[] lookupKey = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
            .add(entityType).getBytesForLookup();
        if (lookupKey[lookupKey.length - 1] != 0x0) {
          throw new IOException("Found unexpected end byte in lookup key");
        }
        lookupKey[lookupKey.length - 1] = 0x1;
        iterator.seek(lookupKey);
      }
      return entityTypes;
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  /**
   * Finds all keys in the db that have a given prefix and deletes them on
   * the given write batch.
   */
  private void deleteKeysWithPrefix(WriteBatch writeBatch, byte[] prefix,
      DBIterator iterator) {
    for (iterator.seek(prefix); iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefix.length, key)) {
        break;
      }
      writeBatch.delete(key);
    }
  }

  @VisibleForTesting
  boolean deleteNextEntity(String entityType, byte[] reverseTimestamp,
      DBIterator iterator, DBIterator pfIterator, boolean seeked)
      throws IOException {
    WriteBatch writeBatch = null;
    try {
      KeyBuilder kb = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
          .add(entityType);
      byte[] typePrefix = kb.getBytesForLookup();
      kb.add(reverseTimestamp);
      if (!seeked) {
        iterator.seek(kb.getBytesForLookup());
      }
      if (!iterator.hasNext()) {
        return false;
      }
      byte[] entityKey = iterator.peekNext().getKey();
      if (!prefixMatches(typePrefix, typePrefix.length, entityKey)) {
        return false;
      }

      // read the start time and entity id from the current key
      KeyParser kp = new KeyParser(entityKey, typePrefix.length + 8);
      String entityId = kp.getNextString();
      int prefixlen = kp.getOffset();
      byte[] deletePrefix = new byte[prefixlen];
      System.arraycopy(entityKey, 0, deletePrefix, 0, prefixlen);

      writeBatch = db.createWriteBatch();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting entity type:" + entityType + " id:" + entityId);
      }
      // remove start time from cache and db
      writeBatch.delete(createStartTimeLookupKey(entityId, entityType));
      EntityIdentifier entityIdentifier =
          new EntityIdentifier(entityId, entityType);
      startTimeReadCache.remove(entityIdentifier);
      startTimeWriteCache.remove(entityIdentifier);

      // delete current entity
      for (; iterator.hasNext(); iterator.next()) {
        byte[] key = iterator.peekNext().getKey();
        if (!prefixMatches(entityKey, prefixlen, key)) {
          break;
        }
        writeBatch.delete(key);

        if (key.length == prefixlen) {
          continue;
        }
        if (key[prefixlen] == PRIMARY_FILTERS_COLUMN[0]) {
          kp = new KeyParser(key,
              prefixlen + PRIMARY_FILTERS_COLUMN.length);
          String name = kp.getNextString();
          Object value = GenericObjectMapper.read(key, kp.getOffset());
          deleteKeysWithPrefix(writeBatch, addPrimaryFilterToKey(name, value,
              deletePrefix), pfIterator);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting entity type:" + entityType + " id:" +
                entityId + " primary filter entry " + name + " " +
                value);
          }
        } else if (key[prefixlen] == RELATED_ENTITIES_COLUMN[0]) {
          kp = new KeyParser(key,
              prefixlen + RELATED_ENTITIES_COLUMN.length);
          String type = kp.getNextString();
          String id = kp.getNextString();
          byte[] relatedEntityStartTime = getStartTime(id, type);
          if (relatedEntityStartTime == null) {
            LOG.warn("Found no start time for " +
                "related entity " + id + " of type " + type + " while " +
                "deleting " + entityId + " of type " + entityType);
            continue;
          }
          writeBatch.delete(createReverseRelatedEntityKey(id, type,
              relatedEntityStartTime, entityId, entityType));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting entity type:" + entityType + " id:" +
                entityId + " from invisible reverse related entity " +
                "entry of type:" + type + " id:" + id);
          }
        } else if (key[prefixlen] ==
            INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN[0]) {
          kp = new KeyParser(key, prefixlen +
              INVISIBLE_REVERSE_RELATED_ENTITIES_COLUMN.length);
          String type = kp.getNextString();
          String id = kp.getNextString();
          byte[] relatedEntityStartTime = getStartTime(id, type);
          if (relatedEntityStartTime == null) {
            LOG.warn("Found no start time for reverse " +
                "related entity " + id + " of type " + type + " while " +
                "deleting " + entityId + " of type " + entityType);
            continue;
          }
          writeBatch.delete(createRelatedEntityKey(id, type,
              relatedEntityStartTime, entityId, entityType));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting entity type:" + entityType + " id:" +
                entityId + " from related entity entry of type:" +
                type + " id:" + id);
          }
        }
      }
      WriteOptions writeOptions = new WriteOptions();
      writeOptions.sync(true);
      db.write(writeBatch, writeOptions);
      return true;
    } finally {
      IOUtils.cleanup(LOG, writeBatch);
    }
  }

  /**
   * Discards entities with start timestamp less than or equal to the given
   * timestamp.
   */
  @VisibleForTesting
  void discardOldEntities(long timestamp)
      throws IOException, InterruptedException {
    byte[] reverseTimestamp = writeReverseOrderedLong(timestamp);
    long totalCount = 0;
    long t1 = System.currentTimeMillis();
    try {
      List<String> entityTypes = getEntityTypes();
      for (String entityType : entityTypes) {
        DBIterator iterator = null;
        DBIterator pfIterator = null;
        long typeCount = 0;
        try {
          deleteLock.writeLock().lock();
          iterator = getDbIterator(false);
          pfIterator = getDbIterator(false);

          if (deletionThread != null && deletionThread.isInterrupted()) {
            throw new InterruptedException();
          }
          boolean seeked = false;
          while (deleteNextEntity(entityType, reverseTimestamp, iterator,
              pfIterator, seeked)) {
            typeCount++;
            totalCount++;
            seeked = true;
            if (deletionThread != null && deletionThread.isInterrupted()) {
              throw new InterruptedException();
            }
          }
        } catch (IOException e) {
          LOG.error("Got IOException while deleting entities for type " +
              entityType + ", continuing to next type", e);
        } finally {
          IOUtils.cleanup(LOG, iterator, pfIterator);
          deleteLock.writeLock().unlock();
          if (typeCount > 0) {
            LOG.info("Deleted " + typeCount + " entities of type " +
                entityType);
          }
        }
      }
    } finally {
      long t2 = System.currentTimeMillis();
      LOG.info("Discarded " + totalCount + " entities for timestamp " +
          timestamp + " and earlier in " + (t2 - t1) / 1000.0 + " seconds");
    }
  }

  @VisibleForTesting
  DBIterator getDbIterator(boolean fillCache) {
    ReadOptions readOptions = new ReadOptions();
    readOptions.fillCache(fillCache);
    return db.iterator(readOptions);
  }
}
