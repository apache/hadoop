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

package org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline;

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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvent;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents.ATSEventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors.ATSPutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import static org.apache.hadoop.yarn.server.applicationhistoryservice
    .apptimeline.GenericObjectMapper.readReverseOrderedLong;
import static org.apache.hadoop.yarn.server.applicationhistoryservice
    .apptimeline.GenericObjectMapper.writeReverseOrderedLong;

/**
 * An implementation of an application timeline store backed by leveldb.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class LeveldbApplicationTimelineStore extends AbstractService
    implements ApplicationTimelineStore {
  private static final Log LOG = LogFactory
      .getLog(LeveldbApplicationTimelineStore.class);

  private static final String FILENAME = "leveldb-apptimeline-store.ldb";

  private static final byte[] START_TIME_LOOKUP_PREFIX = "k".getBytes();
  private static final byte[] ENTITY_ENTRY_PREFIX = "e".getBytes();
  private static final byte[] INDEXED_ENTRY_PREFIX = "i".getBytes();

  private static final byte[] PRIMARY_FILTER_COLUMN = "f".getBytes();
  private static final byte[] OTHER_INFO_COLUMN = "i".getBytes();
  private static final byte[] RELATED_COLUMN = "r".getBytes();
  private static final byte[] TIME_COLUMN = "t".getBytes();

  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final int START_TIME_CACHE_SIZE = 10000;

  @SuppressWarnings("unchecked")
  private final Map<EntityIdentifier, Long> startTimeCache =
      Collections.synchronizedMap(new LRUMap(START_TIME_CACHE_SIZE));

  private DB db;

  public LeveldbApplicationTimelineStore() {
    super(LeveldbApplicationTimelineStore.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Options options = new Options();
    options.createIfMissing(true);
    JniDBFactory factory = new JniDBFactory();
    String path = conf.get(YarnConfiguration.ATS_LEVELDB_PATH_PROPERTY);
    File p = new File(path);
    if (!p.exists())
      if (!p.mkdirs())
        throw new IOException("Couldn't create directory for leveldb " +
            "application timeline store " + path);
    LOG.info("Using leveldb path " + path);
    db = factory.open(new File(path, FILENAME), options);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    IOUtils.cleanup(LOG, db);
    super.serviceStop();
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
      if (sep)
        length++;
      index++;
      return this;
    }

    public byte[] getBytes() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (i < index-1 && useSeparator[i])
          baos.write(0x0);
      }
      return baos.toByteArray();
    }

    public byte[] getBytesForLookup() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
      for (int i = 0; i < index; i++) {
        baos.write(b[i]);
        if (useSeparator[i])
          baos.write(0x0);
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
      if (offset >= b.length)
        throw new IOException(
            "tried to read nonexistent string from byte array");
      int i = 0;
      while (offset+i < b.length && b[offset+i] != 0x0)
        i++;
      String s = new String(b, offset, i);
      offset = offset + i + 1;
      return s;
    }

    public long getNextLong() throws IOException {
      if (offset+8 >= b.length)
        throw new IOException("byte array ran out when trying to read long");
      long l = readReverseOrderedLong(b, offset);
      offset += 8;
      return l;
    }

    public int getOffset() {
      return offset;
    }
  }

  @Override
  public ATSEntity getEntity(String entity, String entityType,
      EnumSet<Field> fields) throws IOException {
    DBIterator iterator = null;
    try {
      byte[] revStartTime = getStartTime(entity, entityType, null, null, null);
      if (revStartTime == null)
        return null;
      byte[] prefix = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
          .add(entityType).add(revStartTime).add(entity).getBytesForLookup();

      iterator = db.iterator();
      iterator.seek(prefix);

      return getEntity(entity, entityType,
          readReverseOrderedLong(revStartTime, 0), fields, iterator, prefix,
          prefix.length);
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  /**
   * Read entity from a db iterator.  If no information is found in the
   * specified fields for this entity, return null.
   */
  private static ATSEntity getEntity(String entity, String entityType,
      Long startTime, EnumSet<Field> fields, DBIterator iterator,
      byte[] prefix, int prefixlen) throws IOException {
    if (fields == null)
      fields = EnumSet.allOf(Field.class);

    ATSEntity atsEntity = new ATSEntity();
    boolean events = false;
    boolean lastEvent = false;
    if (fields.contains(Field.EVENTS)) {
      events = true;
      atsEntity.setEvents(new ArrayList<ATSEvent>());
    } else if (fields.contains(Field.LAST_EVENT_ONLY)) {
      lastEvent = true;
      atsEntity.setEvents(new ArrayList<ATSEvent>());
    }
    else {
      atsEntity.setEvents(null);
    }
    boolean relatedEntities = false;
    if (fields.contains(Field.RELATED_ENTITIES)) {
      relatedEntities = true;
      atsEntity.setRelatedEntities(new HashMap<String, List<String>>());
    } else {
      atsEntity.setRelatedEntities(null);
    }
    boolean primaryFilters = false;
    if (fields.contains(Field.PRIMARY_FILTERS)) {
      primaryFilters = true;
      atsEntity.setPrimaryFilters(new HashMap<String, Object>());
    } else {
      atsEntity.setPrimaryFilters(null);
    }
    boolean otherInfo = false;
    if (fields.contains(Field.OTHER_INFO)) {
      otherInfo = true;
      atsEntity.setOtherInfo(new HashMap<String, Object>());
    } else {
      atsEntity.setOtherInfo(null);
    }

    // iterate through the entity's entry, parsing information if it is part
    // of a requested field
    for (; iterator.hasNext(); iterator.next()) {
      byte[] key = iterator.peekNext().getKey();
      if (!prefixMatches(prefix, prefixlen, key))
        break;
      if (key[prefixlen] == PRIMARY_FILTER_COLUMN[0]) {
        if (primaryFilters) {
          atsEntity.addPrimaryFilter(parseRemainingKey(key,
              prefixlen + PRIMARY_FILTER_COLUMN.length),
              GenericObjectMapper.read(iterator.peekNext().getValue()));
        }
      } else if (key[prefixlen] == OTHER_INFO_COLUMN[0]) {
        if (otherInfo) {
          atsEntity.addOtherInfo(parseRemainingKey(key,
              prefixlen + OTHER_INFO_COLUMN.length),
              GenericObjectMapper.read(iterator.peekNext().getValue()));
        }
      } else if (key[prefixlen] == RELATED_COLUMN[0]) {
        if (relatedEntities) {
          addRelatedEntity(atsEntity, key,
              prefixlen + RELATED_COLUMN.length);
        }
      } else if (key[prefixlen] == TIME_COLUMN[0]) {
        if (events || (lastEvent && atsEntity.getEvents().size() == 0)) {
          ATSEvent event = getEntityEvent(null, key, prefixlen +
              TIME_COLUMN.length, iterator.peekNext().getValue());
          if (event != null) {
            atsEntity.addEvent(event);
          }
        }
      } else {
        LOG.warn(String.format("Found unexpected column for entity %s of " +
            "type %s (0x%02x)", entity, entityType, key[prefixlen]));
      }
    }

    atsEntity.setEntityId(entity);
    atsEntity.setEntityType(entityType);
    atsEntity.setStartTime(startTime);

    return atsEntity;
  }

  @Override
  public ATSEvents getEntityTimelines(String entityType,
      SortedSet<String> entityIds, Long limit, Long windowStart,
      Long windowEnd, Set<String> eventType) throws IOException {
    ATSEvents atsEvents = new ATSEvents();
    if (entityIds == null || entityIds.isEmpty())
      return atsEvents;
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
      for (String entity : entityIds) {
        byte[] startTime = getStartTime(entity, entityType, null, null, null);
        if (startTime != null) {
          List<EntityIdentifier> entities = startTimeMap.get(startTime);
          if (entities == null) {
            entities = new ArrayList<EntityIdentifier>();
            startTimeMap.put(startTime, entities);
          }
          entities.add(new EntityIdentifier(entity, entityType));
        }
      }
      for (Entry<byte[], List<EntityIdentifier>> entry :
          startTimeMap.entrySet()) {
        // look up the events matching the given parameters (limit,
        // start time, end time, event types) for entities whose start times
        // were found and add the entities to the return list
        byte[] revStartTime = entry.getKey();
        for (EntityIdentifier entity : entry.getValue()) {
          ATSEventsOfOneEntity atsEntity = new ATSEventsOfOneEntity();
          atsEntity.setEntityId(entity.getId());
          atsEntity.setEntityType(entityType);
          atsEvents.addEvent(atsEntity);
          KeyBuilder kb = KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
              .add(entityType).add(revStartTime).add(entity.getId())
              .add(TIME_COLUMN);
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
          for (iterator.seek(first); atsEntity.getEvents().size() < limit &&
              iterator.hasNext(); iterator.next()) {
            byte[] key = iterator.peekNext().getKey();
            if (!prefixMatches(prefix, prefix.length, key) || (last != null &&
                WritableComparator.compareBytes(key, 0, key.length, last, 0,
                    last.length) > 0))
              break;
            ATSEvent event = getEntityEvent(eventType, key, prefix.length,
                iterator.peekNext().getValue());
            if (event != null)
              atsEntity.addEvent(event);
          }
        }
      }
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
    return atsEvents;
  }

  /**
   * Returns true if the byte array begins with the specified prefix.
   */
  private static boolean prefixMatches(byte[] prefix, int prefixlen,
      byte[] b) {
    if (b.length < prefixlen)
      return false;
    return WritableComparator.compareBytes(prefix, 0, prefixlen, b, 0,
        prefixlen) == 0;
  }

  @Override
  public ATSEntities getEntities(String entityType,
      Long limit, Long windowStart, Long windowEnd,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) throws IOException {
    if (primaryFilter == null) {
      // if no primary filter is specified, prefix the lookup with
      // ENTITY_ENTRY_PREFIX
      return getEntityByTime(ENTITY_ENTRY_PREFIX, entityType, limit,
          windowStart, windowEnd, secondaryFilters, fields);
    } else {
      // if a primary filter is specified, prefix the lookup with
      // INDEXED_ENTRY_PREFIX + primaryFilterName + primaryFilterValue +
      // ENTITY_ENTRY_PREFIX
      byte[] base = KeyBuilder.newInstance().add(INDEXED_ENTRY_PREFIX)
          .add(primaryFilter.getName())
          .add(GenericObjectMapper.write(primaryFilter.getValue()), true)
          .add(ENTITY_ENTRY_PREFIX).getBytesForLookup();
      return getEntityByTime(base, entityType, limit, windowStart, windowEnd,
          secondaryFilters, fields);
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
   * @param secondaryFilters Filter pairs that the entities should match
   * @param fields The set of fields to retrieve
   * @return A list of entities
   * @throws IOException
   */
  private ATSEntities getEntityByTime(byte[] base,
      String entityType, Long limit, Long starttime, Long endtime,
      Collection<NameValuePair> secondaryFilters, EnumSet<Field> fields)
      throws IOException {
    DBIterator iterator = null;
    try {
      KeyBuilder kb = KeyBuilder.newInstance().add(base).add(entityType);
      // only db keys matching the prefix (base + entity type) will be parsed
      byte[] prefix = kb.getBytesForLookup();
      if (endtime == null) {
        // if end time is null, place no restriction on end time
        endtime = Long.MAX_VALUE;
      }
      // using end time, construct a first key that will be seeked to
      byte[] revts = writeReverseOrderedLong(endtime);
      kb.add(revts);
      byte[] first = kb.getBytesForLookup();
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

      ATSEntities atsEntities = new ATSEntities();
      iterator = db.iterator();
      iterator.seek(first);
      // iterate until one of the following conditions is met: limit is
      // reached, there are no more keys, the key prefix no longer matches,
      // or a start time has been specified and reached/exceeded
      while (atsEntities.getEntities().size() < limit && iterator.hasNext()) {
        byte[] key = iterator.peekNext().getKey();
        if (!prefixMatches(prefix, prefix.length, key) || (last != null &&
            WritableComparator.compareBytes(key, 0, key.length, last, 0,
                last.length) > 0))
          break;
        // read the start time and entity from the current key
        KeyParser kp = new KeyParser(key, prefix.length);
        Long startTime = kp.getNextLong();
        String entity = kp.getNextString();
        // parse the entity that owns this key, iterating over all keys for
        // the entity
        ATSEntity atsEntity = getEntity(entity, entityType, startTime,
            fields, iterator, key, kp.getOffset());
        if (atsEntity == null)
          continue;
        // determine if the retrieved entity matches the provided secondary
        // filters, and if so add it to the list of entities to return
        boolean filterPassed = true;
        if (secondaryFilters != null) {
          for (NameValuePair filter : secondaryFilters) {
            Object v = atsEntity.getOtherInfo().get(filter.getName());
            if (v == null)
              v = atsEntity.getPrimaryFilters().get(filter.getName());
            if (v == null || !v.equals(filter.getValue())) {
              filterPassed = false;
              break;
            }
          }
        }
        if (filterPassed)
          atsEntities.addEntity(atsEntity);
      }
      return atsEntities;
    } finally {
      IOUtils.cleanup(LOG, iterator);
    }
  }

  /**
   * Put a single entity.  If there is an error, add a PutError to the given
   * response.
   */
  private void put(ATSEntity atsEntity, ATSPutErrors response) {
    WriteBatch writeBatch = null;
    try {
      writeBatch = db.createWriteBatch();
      List<ATSEvent> events = atsEntity.getEvents();
      // look up the start time for the entity
      byte[] revStartTime = getStartTime(atsEntity.getEntityId(),
          atsEntity.getEntityType(), atsEntity.getStartTime(), events,
          writeBatch);
      if (revStartTime == null) {
        // if no start time is found, add an error and return
        ATSPutError error = new ATSPutError();
        error.setEntityId(atsEntity.getEntityId());
        error.setEntityType(atsEntity.getEntityType());
        error.setErrorCode(ATSPutError.NO_START_TIME);
        response.addError(error);
        return;
      }
      Long revStartTimeLong = readReverseOrderedLong(revStartTime, 0);
      Map<String, Object> primaryFilters = atsEntity.getPrimaryFilters();

      // write event entries
      if (events != null && !events.isEmpty()) {
        for (ATSEvent event : events) {
          byte[] revts = writeReverseOrderedLong(event.getTimestamp());
          byte[] key = createEntityEventKey(atsEntity.getEntityId(),
              atsEntity.getEntityType(), revStartTime, revts,
              event.getEventType());
          byte[] value = GenericObjectMapper.write(event.getEventInfo());
          writeBatch.put(key, value);
          writePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
        }
      }

      // write related entity entries
      Map<String,List<String>> relatedEntities =
          atsEntity.getRelatedEntities();
      if (relatedEntities != null && !relatedEntities.isEmpty()) {
        for (Entry<String, List<String>> relatedEntityList :
            relatedEntities.entrySet()) {
          String relatedEntityType = relatedEntityList.getKey();
          for (String relatedEntityId : relatedEntityList.getValue()) {
            // look up start time of related entity
            byte[] relatedEntityStartTime = getStartTime(relatedEntityId,
                relatedEntityType, null, null, writeBatch);
            if (relatedEntityStartTime == null) {
              // if start time is not found, set start time of the related
              // entity to the start time of this entity, and write it to the
              // db and the cache
              relatedEntityStartTime = revStartTime;
              writeBatch.put(createStartTimeLookupKey(relatedEntityId,
                  relatedEntityType), relatedEntityStartTime);
              startTimeCache.put(new EntityIdentifier(relatedEntityId,
                  relatedEntityType), revStartTimeLong);
            }
            // write reverse entry (related entity -> entity)
            byte[] key = createReleatedEntityKey(relatedEntityId,
                relatedEntityType, relatedEntityStartTime,
                atsEntity.getEntityId(), atsEntity.getEntityType());
            writeBatch.put(key, EMPTY_BYTES);
            // TODO: write forward entry (entity -> related entity)?
          }
        }
      }

      // write primary filter entries
      if (primaryFilters != null && !primaryFilters.isEmpty()) {
        for (Entry<String, Object> primaryFilter : primaryFilters.entrySet()) {
          byte[] key = createPrimaryFilterKey(atsEntity.getEntityId(),
              atsEntity.getEntityType(), revStartTime, primaryFilter.getKey());
          byte[] value = GenericObjectMapper.write(primaryFilter.getValue());
          writeBatch.put(key, value);
          writePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
        }
      }

      // write other info entries
      Map<String, Object> otherInfo = atsEntity.getOtherInfo();
      if (otherInfo != null && !otherInfo.isEmpty()) {
        for (Entry<String, Object> i : otherInfo.entrySet()) {
          byte[] key = createOtherInfoKey(atsEntity.getEntityId(),
              atsEntity.getEntityType(), revStartTime, i.getKey());
          byte[] value = GenericObjectMapper.write(i.getValue());
          writeBatch.put(key, value);
          writePrimaryFilterEntries(writeBatch, primaryFilters, key, value);
        }
      }
      db.write(writeBatch);
    } catch (IOException e) {
      LOG.error("Error putting entity " + atsEntity.getEntityId() +
          " of type " + atsEntity.getEntityType(), e);
      ATSPutError error = new ATSPutError();
      error.setEntityId(atsEntity.getEntityId());
      error.setEntityType(atsEntity.getEntityType());
      error.setErrorCode(ATSPutError.IO_EXCEPTION);
      response.addError(error);
    } finally {
      IOUtils.cleanup(LOG, writeBatch);
    }
  }

  /**
   * For a given key / value pair that has been written to the db,
   * write additional entries to the db for each primary filter.
   */
  private static void writePrimaryFilterEntries(WriteBatch writeBatch,
      Map<String, Object> primaryFilters, byte[] key, byte[] value)
      throws IOException {
    if (primaryFilters != null && !primaryFilters.isEmpty()) {
      for (Entry<String, Object> p : primaryFilters.entrySet()) {
        writeBatch.put(addPrimaryFilterToKey(p.getKey(), p.getValue(),
            key), value);
      }
    }
  }

  @Override
  public ATSPutErrors put(ATSEntities atsEntities) {
    ATSPutErrors response = new ATSPutErrors();
    for (ATSEntity atsEntity : atsEntities.getEntities()) {
      put(atsEntity, response);
    }
    return response;
  }

  /**
   * Get the unique start time for a given entity as a byte array that sorts
   * the timestamps in reverse order (see {@link
   * GenericObjectMapper#writeReverseOrderedLong(long)}).
   *
   * @param entityId The id of the entity
   * @param entityType The type of the entity
   * @param startTime The start time of the entity, or null
   * @param events A list of events for the entity, or null
   * @param writeBatch A leveldb write batch, if the method is called by a
   *                   put as opposed to a get
   * @return A byte array
   * @throws IOException
   */
  private byte[] getStartTime(String entityId, String entityType,
      Long startTime, List<ATSEvent> events, WriteBatch writeBatch)
      throws IOException {
    EntityIdentifier entity = new EntityIdentifier(entityId, entityType);
    if (startTime == null) {
      // start time is not provided, so try to look it up
      if (startTimeCache.containsKey(entity)) {
        // found the start time in the cache
        startTime = startTimeCache.get(entity);
      } else {
        // try to look up the start time in the db
        byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
        byte[] v = db.get(b);
        if (v == null) {
          // did not find the start time in the db
          // if this is a put, try to set it from the provided events
          if (events == null || writeBatch == null) {
            // no events, or not a put, so return null
            return null;
          }
          Long min = Long.MAX_VALUE;
          for (ATSEvent e : events)
            if (min > e.getTimestamp())
              min = e.getTimestamp();
          startTime = min;
          // selected start time as minimum timestamp of provided events
          // write start time to db and cache
          writeBatch.put(b, writeReverseOrderedLong(startTime));
          startTimeCache.put(entity, startTime);
        } else {
          // found the start time in the db
          startTime = readReverseOrderedLong(v, 0);
          if (writeBatch != null) {
            // if this is a put, re-add the start time to the cache
            startTimeCache.put(entity, startTime);
          }
        }
      }
    } else {
      // start time is provided
      // TODO: verify start time in db as well as cache?
      if (startTimeCache.containsKey(entity)) {
        // if the start time is already in the cache,
        // and it is different from the provided start time,
        // use the one from the cache
        if (!startTime.equals(startTimeCache.get(entity)))
          startTime = startTimeCache.get(entity);
      } else if (writeBatch != null) {
        // if this is a put, write the provided start time to the db and the
        // cache
        byte[] b = createStartTimeLookupKey(entity.getId(), entity.getType());
        writeBatch.put(b, writeReverseOrderedLong(startTime));
        startTimeCache.put(entity, startTime);
      }
    }
    return writeReverseOrderedLong(startTime);
  }

  /**
   * Creates a key for looking up the start time of a given entity,
   * of the form START_TIME_LOOKUP_PREFIX + entitytype + entity.
   */
  private static byte[] createStartTimeLookupKey(String entity,
      String entitytype) throws IOException {
    return KeyBuilder.newInstance().add(START_TIME_LOOKUP_PREFIX)
        .add(entitytype).add(entity).getBytes();
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
   * Creates an event key, serializing ENTITY_ENTRY_PREFIX + entitytype +
   * revstarttime + entity + TIME_COLUMN + reveventtimestamp + eventtype.
   */
  private static byte[] createEntityEventKey(String entity, String entitytype,
      byte[] revStartTime, byte[] reveventtimestamp, String eventtype)
      throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX)
        .add(entitytype).add(revStartTime).add(entity).add(TIME_COLUMN)
        .add(reveventtimestamp).add(eventtype).getBytes();
  }

  /**
   * Creates an event object from the given key, offset, and value.  If the
   * event type is not contained in the specified set of event types,
   * returns null.
   */
  private static ATSEvent getEntityEvent(Set<String> eventTypes, byte[] key,
      int offset, byte[] value) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    long ts = kp.getNextLong();
    String tstype = kp.getNextString();
    if (eventTypes == null || eventTypes.contains(tstype)) {
      ATSEvent event = new ATSEvent();
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
   * entitytype + revstarttime + entity + PRIMARY_FILTER_COLUMN + name.
   */
  private static byte[] createPrimaryFilterKey(String entity,
      String entitytype, byte[] revStartTime, String name) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entitytype)
        .add(revStartTime).add(entity).add(PRIMARY_FILTER_COLUMN).add(name)
        .getBytes();
  }

  /**
   * Creates an other info key, serializing ENTITY_ENTRY_PREFIX + entitytype +
   * revstarttime + entity + OTHER_INFO_COLUMN + name.
   */
  private static byte[] createOtherInfoKey(String entity, String entitytype,
      byte[] revStartTime, String name) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entitytype)
        .add(revStartTime).add(entity).add(OTHER_INFO_COLUMN).add(name)
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
   * entitytype + revstarttime + entity + RELATED_COLUMN + relatedentitytype +
   * relatedentity.
   */
  private static byte[] createReleatedEntityKey(String entity,
      String entitytype, byte[] revStartTime, String relatedEntity,
      String relatedEntityType) throws IOException {
    return KeyBuilder.newInstance().add(ENTITY_ENTRY_PREFIX).add(entitytype)
        .add(revStartTime).add(entity).add(RELATED_COLUMN)
        .add(relatedEntityType).add(relatedEntity).getBytes();
  }

  /**
   * Parses the related entity from the given key at the given offset and
   * adds it to the given entity.
   */
  private static void addRelatedEntity(ATSEntity atsEntity, byte[] key,
      int offset) throws IOException {
    KeyParser kp = new KeyParser(key, offset);
    String type = kp.getNextString();
    String id = kp.getNextString();
    atsEntity.addRelatedEntity(type, id);
  }

  /**
   * Clears the cache to test reloading start times from leveldb (only for
   * testing).
   */
  @VisibleForTesting
  void clearStartTimeCache() {
    startTimeCache.clear();
  }
}
