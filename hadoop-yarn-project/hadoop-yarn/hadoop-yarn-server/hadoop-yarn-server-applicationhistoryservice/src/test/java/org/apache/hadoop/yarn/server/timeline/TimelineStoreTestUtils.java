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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.server.timeline.TimelineReader.Field;

public class TimelineStoreTestUtils {

  protected static final List<TimelineEvent> EMPTY_EVENTS =
      Collections.emptyList();
  protected static final Map<String, Object> EMPTY_MAP =
      Collections.emptyMap();
  protected static final Map<String, Set<Object>> EMPTY_PRIMARY_FILTERS =
      Collections.emptyMap();
  protected static final Map<String, Set<String>> EMPTY_REL_ENTITIES =
      Collections.emptyMap();

  protected TimelineStore store;
  protected String entityId1;
  protected String entityType1;
  protected String entityId1b;
  protected String entityId2;
  protected String entityType2;
  protected String entityId4;
  protected String entityType4;
  protected String entityId5;
  protected String entityType5;
  protected String entityId6;
  protected String entityId7;
  protected String entityType7;

  protected Map<String, Set<Object>> primaryFilters;
  protected Map<String, Object> secondaryFilters;
  protected Map<String, Object> allFilters;
  protected Map<String, Object> otherInfo;
  protected Map<String, Set<String>> relEntityMap;
  protected Map<String, Set<String>> relEntityMap2;
  protected NameValuePair userFilter;
  protected NameValuePair numericFilter1;
  protected NameValuePair numericFilter2;
  protected NameValuePair numericFilter3;
  protected Collection<NameValuePair> goodTestingFilters;
  protected Collection<NameValuePair> badTestingFilters;
  protected TimelineEvent ev1;
  protected TimelineEvent ev2;
  protected TimelineEvent ev3;
  protected TimelineEvent ev4;
  protected Map<String, Object> eventInfo;
  protected List<TimelineEvent> events1;
  protected List<TimelineEvent> events2;
  protected long beforeTs;
  protected String domainId1;
  protected String domainId2;

  /**
   * Load test entity data into the given store
   */
  protected void loadTestEntityData() throws IOException {
    beforeTs = System.currentTimeMillis()-1;
    TimelineEntities entities = new TimelineEntities();
    Map<String, Set<Object>> primaryFilters =
        new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(Integer.MAX_VALUE);
    Set<Object> l3 = new HashSet<Object>();
    l3.add("123abc");
    Set<Object> l4 = new HashSet<Object>();
    l4.add((long)Integer.MAX_VALUE + 1l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    primaryFilters.put("other", l3);
    primaryFilters.put("long", l4);
    Map<String, Object> secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456);
    secondaryFilters.put("status", "RUNNING");
    Map<String, Object> otherInfo1 = new HashMap<String, Object>();
    otherInfo1.put("info1", "val1");
    otherInfo1.putAll(secondaryFilters);

    String entityId1 = "id_1";
    String entityType1 = "type_1";
    String entityId1b = "id_2";
    String entityId2 = "id_2";
    String entityType2 = "type_2";
    String entityId4 = "id_4";
    String entityType4 = "type_4";
    String entityId5 = "id_5";
    String entityType5 = "type_5";
    String entityId6 = "id_6";
    String entityId7 = "id_7";
    String entityType7 = "type_7";

    Map<String, Set<String>> relatedEntities =
        new HashMap<String, Set<String>>();
    relatedEntities.put(entityType2, Collections.singleton(entityId2));

    TimelineEvent ev3 = createEvent(789l, "launch_event", null);
    TimelineEvent ev4 = createEvent(0l, "init_event", null);
    List<TimelineEvent> events = new ArrayList<TimelineEvent>();
    events.add(ev3);
    events.add(ev4);
    entities.setEntities(Collections.singletonList(createEntity(entityId2,
        entityType2, null, events, null, null, null, "domain_id_1")));
    TimelinePutResponse response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    TimelineEvent ev1 = createEvent(123l, "start_event", null);
    entities.setEntities(Collections.singletonList(createEntity(entityId1,
        entityType1, 123l, Collections.singletonList(ev1),
        relatedEntities, primaryFilters, otherInfo1, "domain_id_1")));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());
    entities.setEntities(Collections.singletonList(createEntity(entityId1b,
        entityType1, null, Collections.singletonList(ev1), relatedEntities,
        primaryFilters, otherInfo1, "domain_id_1")));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put("event info 1", "val1");
    TimelineEvent ev2 = createEvent(456l, "end_event", eventInfo);
    Map<String, Object> otherInfo2 = new HashMap<String, Object>();
    otherInfo2.put("info2", "val2");
    entities.setEntities(Collections.singletonList(createEntity(entityId1,
        entityType1, null, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2, "domain_id_1")));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());
    entities.setEntities(Collections.singletonList(createEntity(entityId1b,
        entityType1, 789l, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2, "domain_id_1")));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    entities.setEntities(Collections.singletonList(createEntity(
        "badentityid", "badentity", null, null, null, null, otherInfo1,
        "domain_id_1")));
    response = store.put(entities);
    assertEquals(1, response.getErrors().size());
    TimelinePutError error = response.getErrors().get(0);
    assertEquals("badentityid", error.getEntityId());
    assertEquals("badentity", error.getEntityType());
    assertEquals(TimelinePutError.NO_START_TIME, error.getErrorCode());

    relatedEntities.clear();
    relatedEntities.put(entityType5, Collections.singleton(entityId5));
    entities.setEntities(Collections.singletonList(createEntity(entityId4,
        entityType4, 42l, null, relatedEntities, null, null,
        "domain_id_1")));
    response = store.put(entities);

    relatedEntities.clear();
    otherInfo1.put("info2", "val2");
    entities.setEntities(Collections.singletonList(createEntity(entityId6,
        entityType1, 61l, null, relatedEntities, primaryFilters, otherInfo1,
        "domain_id_2")));
    response = store.put(entities);

    relatedEntities.clear();
    relatedEntities.put(entityType1, Collections.singleton(entityId1));
    entities.setEntities(Collections.singletonList(createEntity(entityId7,
        entityType7, 62l, null, relatedEntities, null, null,
        "domain_id_2")));
    response = store.put(entities);
    assertEquals(1, response.getErrors().size());
    assertEquals(entityType7, response.getErrors().get(0).getEntityType());
    assertEquals(entityId7, response.getErrors().get(0).getEntityId());
    assertEquals(TimelinePutError.FORBIDDEN_RELATION,
        response.getErrors().get(0).getErrorCode());

    if (store instanceof LeveldbTimelineStore) {
      LeveldbTimelineStore leveldb = (LeveldbTimelineStore) store;
      entities.setEntities(Collections.singletonList(createEntity(
          "OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", 63l, null, null, null, null,
          null)));
      leveldb.putWithNoDomainId(entities);
      entities.setEntities(Collections.singletonList(createEntity(
          "OLD_ENTITY_ID_2", "OLD_ENTITY_TYPE_1", 64l, null, null, null, null,
          null)));
      leveldb.putWithNoDomainId(entities);
    }
  }

  /**
   * Load verification entity data
   */
  protected void loadVerificationEntityData() throws Exception {
    userFilter = new NameValuePair("user", "username");
    numericFilter1 = new NameValuePair("appname", Integer.MAX_VALUE);
    numericFilter2 = new NameValuePair("long", (long)Integer.MAX_VALUE + 1l);
    numericFilter3 = new NameValuePair("other", "123abc");
    goodTestingFilters = new ArrayList<NameValuePair>();
    goodTestingFilters.add(new NameValuePair("appname", Integer.MAX_VALUE));
    goodTestingFilters.add(new NameValuePair("status", "RUNNING"));
    badTestingFilters = new ArrayList<NameValuePair>();
    badTestingFilters.add(new NameValuePair("appname", Integer.MAX_VALUE));
    badTestingFilters.add(new NameValuePair("status", "FINISHED"));

    primaryFilters = new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(Integer.MAX_VALUE);
    Set<Object> l3 = new HashSet<Object>();
    l3.add("123abc");
    Set<Object> l4 = new HashSet<Object>();
    l4.add((long)Integer.MAX_VALUE + 1l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    primaryFilters.put("other", l3);
    primaryFilters.put("long", l4);
    secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456);
    secondaryFilters.put("status", "RUNNING");
    allFilters = new HashMap<String, Object>();
    allFilters.putAll(secondaryFilters);
    for (Entry<String, Set<Object>> pf : primaryFilters.entrySet()) {
      for (Object o : pf.getValue()) {
        allFilters.put(pf.getKey(), o);
      }
    }
    otherInfo = new HashMap<String, Object>();
    otherInfo.put("info1", "val1");
    otherInfo.put("info2", "val2");
    otherInfo.putAll(secondaryFilters);

    entityId1 = "id_1";
    entityType1 = "type_1";
    entityId1b = "id_2";
    entityId2 = "id_2";
    entityType2 = "type_2";
    entityId4 = "id_4";
    entityType4 = "type_4";
    entityId5 = "id_5";
    entityType5 = "type_5";
    entityId6 = "id_6";
    entityId7 = "id_7";
    entityType7 = "type_7";

    ev1 = createEvent(123l, "start_event", null);

    eventInfo = new HashMap<String, Object>();
    eventInfo.put("event info 1", "val1");
    ev2 = createEvent(456l, "end_event", eventInfo);
    events1 = new ArrayList<TimelineEvent>();
    events1.add(ev2);
    events1.add(ev1);

    relEntityMap =
        new HashMap<String, Set<String>>();
    Set<String> ids = new HashSet<String>();
    ids.add(entityId1);
    ids.add(entityId1b);
    relEntityMap.put(entityType1, ids);

    relEntityMap2 =
        new HashMap<String, Set<String>>();
    relEntityMap2.put(entityType4, Collections.singleton(entityId4));

    ev3 = createEvent(789l, "launch_event", null);
    ev4 = createEvent(0l, "init_event", null);
    events2 = new ArrayList<TimelineEvent>();
    events2.add(ev3);
    events2.add(ev4);

    domainId1 = "domain_id_1";
    domainId2 = "domain_id_2";
  }

  private TimelineDomain domain1;
  private TimelineDomain domain2;
  private TimelineDomain domain3;
  private long elapsedTime;

  protected void loadTestDomainData() throws IOException {
    domain1 = new TimelineDomain();
    domain1.setId("domain_id_1");
    domain1.setDescription("description_1");
    domain1.setOwner("owner_1");
    domain1.setReaders("reader_user_1 reader_group_1");
    domain1.setWriters("writer_user_1 writer_group_1");
    store.put(domain1);

    domain2 = new TimelineDomain();
    domain2.setId("domain_id_2");
    domain2.setDescription("description_2");
    domain2.setOwner("owner_2");
    domain2.setReaders("reader_user_2 reader_group_2");
    domain2.setWriters("writer_user_2 writer_group_2");
    store.put(domain2);

    // Wait a second before updating the domain information
    elapsedTime = 1000;
    try {
      Thread.sleep(elapsedTime);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    domain2.setDescription("description_3");
    domain2.setOwner("owner_3");
    domain2.setReaders("reader_user_3 reader_group_3");
    domain2.setWriters("writer_user_3 writer_group_3");
    store.put(domain2);

    domain3 = new TimelineDomain();
    domain3.setId("domain_id_4");
    domain3.setDescription("description_4");
    domain3.setOwner("owner_1");
    domain3.setReaders("reader_user_4 reader_group_4");
    domain3.setWriters("writer_user_4 writer_group_4");
    store.put(domain3);

    TimelineEntities entities = new TimelineEntities();
    if (store instanceof LeveldbTimelineStore) {
      LeveldbTimelineStore leveldb = (LeveldbTimelineStore) store;
      entities.setEntities(Collections.singletonList(createEntity(
              "ACL_ENTITY_ID_11", "ACL_ENTITY_TYPE_1", 63l, null, null, null, null,
              "domain_id_4")));
      leveldb.put(entities);
      entities.setEntities(Collections.singletonList(createEntity(
              "ACL_ENTITY_ID_22", "ACL_ENTITY_TYPE_1", 64l, null, null, null, null,
              "domain_id_2")));
      leveldb.put(entities);
    }
  }

  public void testGetSingleEntity() throws IOException {
    // test getting entity info
    verifyEntityInfo(null, null, null, null, null, null,
        store.getEntity("id_1", "type_2", EnumSet.allOf(Field.class)),
        domainId1);

    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, 123l, store.getEntity(entityId1,
        entityType1, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, 123l, store.getEntity(entityId1b,
        entityType1, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 0l, store.getEntity(entityId2,
        entityType2, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId4, entityType4, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 42l, store.getEntity(entityId4,
        entityType4, EnumSet.allOf(Field.class)), domainId1);

    verifyEntityInfo(entityId5, entityType5, EMPTY_EVENTS, relEntityMap2,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 42l, store.getEntity(entityId5,
        entityType5, EnumSet.allOf(Field.class)), domainId1);

    // test getting single fields
    verifyEntityInfo(entityId1, entityType1, events1, null, null, null,
        store.getEntity(entityId1, entityType1, EnumSet.of(Field.EVENTS)),
        domainId1);

    verifyEntityInfo(entityId1, entityType1, Collections.singletonList(ev2),
        null, null, null, store.getEntity(entityId1, entityType1,
        EnumSet.of(Field.LAST_EVENT_ONLY)), domainId1);

    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entityId1b, entityType1,
        null), domainId1);

    verifyEntityInfo(entityId1, entityType1, null, null, primaryFilters, null,
        store.getEntity(entityId1, entityType1,
            EnumSet.of(Field.PRIMARY_FILTERS)), domainId1);

    verifyEntityInfo(entityId1, entityType1, null, null, null, otherInfo,
        store.getEntity(entityId1, entityType1, EnumSet.of(Field.OTHER_INFO)),
        domainId1);

    verifyEntityInfo(entityId2, entityType2, null, relEntityMap, null, null,
        store.getEntity(entityId2, entityType2,
            EnumSet.of(Field.RELATED_ENTITIES)), domainId1);

    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entityId6, entityType1,
            EnumSet.allOf(Field.class)), domainId2);

    // entity is created, but it doesn't relate to <entityType1, entityId1>
    verifyEntityInfo(entityId7, entityType7, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, store.getEntity(entityId7, entityType7,
            EnumSet.allOf(Field.class)), domainId2);
  }

  protected List<TimelineEntity> getEntities(String entityType)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        null, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter) throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromId(String entityType,
      String fromId) throws IOException {
    return store.getEntities(entityType, null, null, null, fromId, null,
        null, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromTs(String entityType,
      long fromTs) throws IOException {
    return store.getEntities(entityType, null, null, null, null, fromTs,
        null, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter, String fromId)
      throws IOException {
    return store.getEntities(entityType, null, null, null, fromId, null,
        primaryFilter, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromTsWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter, long fromTs)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, fromTs,
        primaryFilter, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithWindow(String entityType,
      Long windowEnd, String fromId) throws IOException {
    return store.getEntities(entityType, null, null, windowEnd, fromId, null,
        null, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithPrimaryFilterAndWindow(
      String entityType, Long windowEnd, String fromId,
      NameValuePair primaryFilter) throws IOException {
    return store.getEntities(entityType, null, null, windowEnd, fromId, null,
        primaryFilter, null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithFilters(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, secondaryFilters, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithFilters(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters,
      EnumSet<Field> fields) throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, secondaryFilters, fields, null).getEntities();
  }

  protected List<TimelineEntity> getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, NameValuePair primaryFilter,
      EnumSet<Field> fields) throws IOException {
    return store.getEntities(entityType, limit, windowStart, windowEnd, null,
        null, primaryFilter, null, fields, null).getEntities();
  }

  public void testGetEntities() throws IOException {
    // test getting entities
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntities("type_0").size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntities("type_3").size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntities("type_6").size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntitiesWithPrimaryFilter("type_0", userFilter).size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntitiesWithPrimaryFilter("type_3", userFilter).size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        getEntitiesWithPrimaryFilter("type_6", userFilter).size());

    List<TimelineEntity> entities = getEntities("type_1");
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntities("type_2");
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, entities.get(0), domainId1);

    entities = getEntities("type_1", 1l, null, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);

    entities = getEntities("type_1", 1l, 0l, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);

    entities = getEntities("type_1", null, 234l, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, 123l, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, 234l, 345l, null,
        EnumSet.allOf(Field.class));
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, null, 345l, null,
        EnumSet.allOf(Field.class));
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntities("type_1", null, null, 123l, null,
        EnumSet.allOf(Field.class));
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);
  }

  public void testGetEntitiesWithFromId() throws IOException {
    List<TimelineEntity> entities = getEntitiesFromId("type_1", entityId1);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesFromId("type_1", entityId1b);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId2);

    entities = getEntitiesFromId("type_1", entityId6);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId2);

    entities = getEntitiesFromIdWithWindow("type_1", 0l, entityId6);
    assertEquals(0, entities.size());

    entities = getEntitiesFromId("type_2", "a");
    assertEquals(0, entities.size());

    entities = getEntitiesFromId("type_2", entityId2);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, entities.get(0), domainId1);

    entities = getEntitiesFromIdWithWindow("type_2", -456l, null);
    assertEquals(0, entities.size());

    entities = getEntitiesFromIdWithWindow("type_2", -456l, "a");
    assertEquals(0, entities.size());

    entities = getEntitiesFromIdWithWindow("type_2", 0l, null);
    assertEquals(1, entities.size());

    entities = getEntitiesFromIdWithWindow("type_2", 0l, entityId2);
    assertEquals(1, entities.size());

    // same tests with primary filters
    entities = getEntitiesFromIdWithPrimaryFilter("type_1", userFilter,
        entityId1);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesFromIdWithPrimaryFilter("type_1", userFilter,
        entityId1b);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId2);

    entities = getEntitiesFromIdWithPrimaryFilter("type_1", userFilter,
        entityId6);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId2);

    entities = getEntitiesFromIdWithPrimaryFilterAndWindow("type_1", 0l,
        entityId6, userFilter);
    assertEquals(0, entities.size());

    entities = getEntitiesFromIdWithPrimaryFilter("type_2", userFilter, "a");
    assertEquals(0, entities.size());
  }

  public void testGetEntitiesWithFromTs() throws IOException {
    assertEquals(0, getEntitiesFromTs("type_1", beforeTs).size());
    assertEquals(0, getEntitiesFromTs("type_2", beforeTs).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        beforeTs).size());
    long afterTs = System.currentTimeMillis();
    assertEquals(3, getEntitiesFromTs("type_1", afterTs).size());
    assertEquals(1, getEntitiesFromTs("type_2", afterTs).size());
    assertEquals(3, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        afterTs).size());
    assertEquals(3, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());
    assertEquals(3, getEntitiesWithPrimaryFilter("type_1", userFilter).size());
    // check insert time is not overwritten
    long beforeTs = this.beforeTs;
    loadTestEntityData();
    assertEquals(0, getEntitiesFromTs("type_1", beforeTs).size());
    assertEquals(0, getEntitiesFromTs("type_2", beforeTs).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        beforeTs).size());
    assertEquals(3, getEntitiesFromTs("type_1", afterTs).size());
    assertEquals(1, getEntitiesFromTs("type_2", afterTs).size());
    assertEquals(3, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        afterTs).size());
  }

  public void testGetEntitiesWithPrimaryFilters() throws IOException {
    // test using primary filter
    assertEquals("nonzero entities size for primary filter", 0,
        getEntitiesWithPrimaryFilter("type_1",
            new NameValuePair("none", "none")).size());
    assertEquals("nonzero entities size for primary filter", 0,
        getEntitiesWithPrimaryFilter("type_2",
            new NameValuePair("none", "none")).size());
    assertEquals("nonzero entities size for primary filter", 0,
        getEntitiesWithPrimaryFilter("type_3",
            new NameValuePair("none", "none")).size());

    List<TimelineEntity> entities = getEntitiesWithPrimaryFilter("type_1",
        userFilter);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter1);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter2);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter3);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);

    entities = getEntitiesWithPrimaryFilter("type_2", userFilter);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", 1l, null, null, userFilter, null);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);

    entities = getEntities("type_1", 1l, 0l, null, userFilter, null);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);

    entities = getEntities("type_1", null, 234l, null, userFilter, null);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, 234l, 345l, userFilter, null);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, null, 345l, userFilter, null);
    assertEquals(3, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0), domainId1);
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1), domainId1);
    verifyEntityInfo(entityId6, entityType1, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(2), domainId2);
  }

  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    for (int i = 0; i < 4; ++i) {
      // Verify the secondary filter works both other info is included or not.
      EnumSet<Field> fields = null;
      if (i == 1) {
        fields = EnumSet.noneOf(Field.class);
      } else if (i == 2) {
        fields = EnumSet.of(Field.PRIMARY_FILTERS);
      } else if (i == 3) {
        fields = EnumSet.of(Field.OTHER_INFO);
      }
      // test using secondary filter
      List<TimelineEntity> entities = getEntitiesWithFilters("type_1", null,
          goodTestingFilters, fields);
      assertEquals(3, entities.size());
      verifyEntityInfo(entityId1, entityType1,
          (i == 0 ? events1 : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(0), domainId1);
      verifyEntityInfo(entityId1b, entityType1,
          (i == 0 ? events1 : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(1), domainId1);
      verifyEntityInfo(entityId6, entityType1,
          (i == 0 ? EMPTY_EVENTS : null),
          (i == 0 ? EMPTY_REL_ENTITIES : null),
          (i == 0 || i == 2 ? primaryFilters : null),
          (i == 0 || i == 3 ? otherInfo : null), entities.get(2), domainId2);

      entities =
          getEntitiesWithFilters("type_1", userFilter, goodTestingFilters, fields);
      assertEquals(3, entities.size());
      if (i == 0) {
        verifyEntityInfo(entityId1, entityType1,
            (i == 0 ? events1 : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(0), domainId1);
        verifyEntityInfo(entityId1b, entityType1,
            (i == 0 ? events1 : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(1), domainId1);
        verifyEntityInfo(entityId6, entityType1,
            (i == 0 ? EMPTY_EVENTS : null),
            (i == 0 ? EMPTY_REL_ENTITIES : null),
            (i == 0 || i == 2 ? primaryFilters : null),
            (i == 0 || i == 3 ? otherInfo : null), entities.get(2), domainId2);
      }

      entities = getEntitiesWithFilters("type_1", null,
          Collections.singleton(new NameValuePair("user", "none")), fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_1", null, badTestingFilters, fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_1", userFilter, badTestingFilters, fields);
      assertEquals(0, entities.size());

      entities =
          getEntitiesWithFilters("type_5", null, badTestingFilters, fields);
      assertEquals(0, entities.size());
    }
  }

  public void testGetEvents() throws IOException {
    // test getting entity timelines
    SortedSet<String> sortedSet = new TreeSet<String>();
    sortedSet.add(entityId1);
    List<EventsOfOneEntity> timelines =
        store.getEntityTimelines(entityType1, sortedSet, null, null,
            null, null).getAllEvents();
    assertEquals(1, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2, ev1);

    sortedSet.add(entityId1b);
    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2, ev1);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev2, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, 1l,
        null, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        345l, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        123l, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, 345l, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev1);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, 123l, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev1);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, null, Collections.singleton("end_event")).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entityId1b, entityType1, ev2);

    sortedSet.add(entityId2);
    timelines = store.getEntityTimelines(entityType2, sortedSet, null,
        null, null, null).getAllEvents();
    assertEquals(1, timelines.size());
    verifyEntityTimeline(timelines.get(0), entityId2, entityType2, ev3, ev4);
  }

  /**
   * Verify a single entity and its start time
   */
  protected static void verifyEntityInfo(String entityId, String entityType,
      List<TimelineEvent> events, Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters, Map<String, Object> otherInfo,
      Long startTime, TimelineEntity retrievedEntityInfo, String domainId) {

    verifyEntityInfo(entityId, entityType, events, relatedEntities,
        primaryFilters, otherInfo, retrievedEntityInfo, domainId);
    assertEquals(startTime, retrievedEntityInfo.getStartTime());
  }

  /**
   * Verify a single entity
   */
  protected static void verifyEntityInfo(String entityId, String entityType,
      List<TimelineEvent> events, Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters, Map<String, Object> otherInfo,
      TimelineEntity retrievedEntityInfo, String domainId) {
    if (entityId == null) {
      assertNull(retrievedEntityInfo);
      return;
    }
    assertEquals(entityId, retrievedEntityInfo.getEntityId());
    assertEquals(entityType, retrievedEntityInfo.getEntityType());
    assertEquals(domainId, retrievedEntityInfo.getDomainId());
    if (events == null) {
      assertNull(retrievedEntityInfo.getEvents());
    } else {
      assertEquals(events, retrievedEntityInfo.getEvents());
    }
    if (relatedEntities == null) {
      assertNull(retrievedEntityInfo.getRelatedEntities());
    } else {
      assertEquals(relatedEntities, retrievedEntityInfo.getRelatedEntities());
    }
    if (primaryFilters == null) {
      assertNull(retrievedEntityInfo.getPrimaryFilters());
    } else {
      assertTrue(primaryFilters.equals(
          retrievedEntityInfo.getPrimaryFilters()));
    }
    if (otherInfo == null) {
      assertNull(retrievedEntityInfo.getOtherInfo());
    } else {
      assertTrue(otherInfo.equals(retrievedEntityInfo.getOtherInfo()));
    }
  }

  /**
   * Verify timeline events
   */
  private static void verifyEntityTimeline(
      EventsOfOneEntity retrievedEvents, String entityId, String entityType,
      TimelineEvent... actualEvents) {
    assertEquals(entityId, retrievedEvents.getEntityId());
    assertEquals(entityType, retrievedEvents.getEntityType());
    assertEquals(actualEvents.length, retrievedEvents.getEvents().size());
    for (int i = 0; i < actualEvents.length; i++) {
      assertEquals(actualEvents[i], retrievedEvents.getEvents().get(i));
    }
  }

  /**
   * Create a test entity
   */
  protected static TimelineEntity createEntity(String entityId, String entityType,
      Long startTime, List<TimelineEvent> events,
      Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters,
      Map<String, Object> otherInfo, String domainId) {
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(entityId);
    entity.setEntityType(entityType);
    entity.setStartTime(startTime);
    entity.setEvents(events);
    if (relatedEntities != null) {
      for (Entry<String, Set<String>> e : relatedEntities.entrySet()) {
        for (String v : e.getValue()) {
          entity.addRelatedEntity(e.getKey(), v);
        }
      }
    } else {
      entity.setRelatedEntities(null);
    }
    entity.setPrimaryFilters(primaryFilters);
    entity.setOtherInfo(otherInfo);
    entity.setDomainId(domainId);
    return entity;
  }

  /**
   * Create a test event
   */
  private static TimelineEvent createEvent(long timestamp, String type, Map<String,
      Object> info) {
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(timestamp);
    event.setEventType(type);
    event.setEventInfo(info);
    return event;
  }

  public void testGetDomain() throws IOException {
    TimelineDomain actualDomain1 =
        store.getDomain(domain1.getId());
    verifyDomainInfo(domain1, actualDomain1);
    assertTrue(actualDomain1.getCreatedTime() > 0);
    assertTrue(actualDomain1.getModifiedTime() > 0);
    assertEquals(
        actualDomain1.getCreatedTime(), actualDomain1.getModifiedTime());

    TimelineDomain actualDomain2 =
        store.getDomain(domain2.getId());
    verifyDomainInfo(domain2, actualDomain2);
    assertEquals("domain_id_2", actualDomain2.getId());
    assertTrue(actualDomain2.getCreatedTime() > 0);
    assertTrue(actualDomain2.getModifiedTime() > 0);
    assertTrue(
        actualDomain2.getCreatedTime() < actualDomain2.getModifiedTime());
  }

  public void testGetDomains() throws IOException {
    TimelineDomains actualDomains =
        store.getDomains("owner_1");
    assertEquals(2, actualDomains.getDomains().size());
    verifyDomainInfo(domain3, actualDomains.getDomains().get(0));
    verifyDomainInfo(domain1, actualDomains.getDomains().get(1));

    // owner without any domain
    actualDomains = store.getDomains("owner_4");
    assertEquals(0, actualDomains.getDomains().size());
  }

  private static void verifyDomainInfo(
      TimelineDomain expected, TimelineDomain actual) {
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getDescription(), actual.getDescription());
    assertEquals(expected.getOwner(), actual.getOwner());
    assertEquals(expected.getReaders(), actual.getReaders());
    assertEquals(expected.getWriters(), actual.getWriters());
  }
}
