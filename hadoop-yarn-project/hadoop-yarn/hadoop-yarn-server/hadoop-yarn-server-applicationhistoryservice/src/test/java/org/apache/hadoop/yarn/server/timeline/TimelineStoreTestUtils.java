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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents.EventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.server.timeline.NameValuePair;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
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

  /**
   * Load test data into the given store
   */
  protected void loadTestData() throws IOException {
    beforeTs = System.currentTimeMillis()-1;
    TimelineEntities entities = new TimelineEntities();
    Map<String, Set<Object>> primaryFilters =
        new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add((long)Integer.MAX_VALUE);
    Set<Object> l3 = new HashSet<Object>();
    l3.add("123abc");
    Set<Object> l4 = new HashSet<Object>();
    l4.add((long)Integer.MAX_VALUE + 1l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    primaryFilters.put("other", l3);
    primaryFilters.put("long", l4);
    Map<String, Object> secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456l);
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

    Map<String, Set<String>> relatedEntities =
        new HashMap<String, Set<String>>();
    relatedEntities.put(entityType2, Collections.singleton(entityId2));

    TimelineEvent ev3 = createEvent(789l, "launch_event", null);
    TimelineEvent ev4 = createEvent(-123l, "init_event", null);
    List<TimelineEvent> events = new ArrayList<TimelineEvent>();
    events.add(ev3);
    events.add(ev4);
    entities.setEntities(Collections.singletonList(createEntity(entityId2,
        entityType2, null, events, null, null, null)));
    TimelinePutResponse response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    TimelineEvent ev1 = createEvent(123l, "start_event", null);
    entities.setEntities(Collections.singletonList(createEntity(entityId1,
        entityType1, 123l, Collections.singletonList(ev1),
        relatedEntities, primaryFilters, otherInfo1)));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());
    entities.setEntities(Collections.singletonList(createEntity(entityId1b,
        entityType1, null, Collections.singletonList(ev1), relatedEntities,
        primaryFilters, otherInfo1)));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put("event info 1", "val1");
    TimelineEvent ev2 = createEvent(456l, "end_event", eventInfo);
    Map<String, Object> otherInfo2 = new HashMap<String, Object>();
    otherInfo2.put("info2", "val2");
    entities.setEntities(Collections.singletonList(createEntity(entityId1,
        entityType1, null, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2)));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());
    entities.setEntities(Collections.singletonList(createEntity(entityId1b,
        entityType1, 789l, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2)));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());

    entities.setEntities(Collections.singletonList(createEntity(
        "badentityid", "badentity", null, null, null, null, otherInfo1)));
    response = store.put(entities);
    assertEquals(1, response.getErrors().size());
    TimelinePutError error = response.getErrors().get(0);
    assertEquals("badentityid", error.getEntityId());
    assertEquals("badentity", error.getEntityType());
    assertEquals(TimelinePutError.NO_START_TIME, error.getErrorCode());

    relatedEntities.clear();
    relatedEntities.put(entityType5, Collections.singleton(entityId5));
    entities.setEntities(Collections.singletonList(createEntity(entityId4,
        entityType4, 42l, null, relatedEntities, null, null)));
    response = store.put(entities);
    assertEquals(0, response.getErrors().size());
  }

  /**
   * Load verification data
   */
  protected void loadVerificationData() throws Exception {
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
    ev4 = createEvent(-123l, "init_event", null);
    events2 = new ArrayList<TimelineEvent>();
    events2.add(ev3);
    events2.add(ev4);
  }

  public void testGetSingleEntity() throws IOException {
    // test getting entity info
    verifyEntityInfo(null, null, null, null, null, null,
        store.getEntity("id_1", "type_2", EnumSet.allOf(Field.class)));

    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, 123l, store.getEntity(entityId1,
        entityType1, EnumSet.allOf(Field.class)));

    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, 123l, store.getEntity(entityId1b,
        entityType1, EnumSet.allOf(Field.class)));

    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, -123l, store.getEntity(entityId2,
        entityType2, EnumSet.allOf(Field.class)));

    verifyEntityInfo(entityId4, entityType4, EMPTY_EVENTS, EMPTY_REL_ENTITIES,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 42l, store.getEntity(entityId4,
        entityType4, EnumSet.allOf(Field.class)));

    verifyEntityInfo(entityId5, entityType5, EMPTY_EVENTS, relEntityMap2,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, 42l, store.getEntity(entityId5,
        entityType5, EnumSet.allOf(Field.class)));

    // test getting single fields
    verifyEntityInfo(entityId1, entityType1, events1, null, null, null,
        store.getEntity(entityId1, entityType1, EnumSet.of(Field.EVENTS)));

    verifyEntityInfo(entityId1, entityType1, Collections.singletonList(ev2),
        null, null, null, store.getEntity(entityId1, entityType1,
        EnumSet.of(Field.LAST_EVENT_ONLY)));

    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entityId1b, entityType1,
        null));

    verifyEntityInfo(entityId1, entityType1, null, null, primaryFilters, null,
        store.getEntity(entityId1, entityType1,
            EnumSet.of(Field.PRIMARY_FILTERS)));

    verifyEntityInfo(entityId1, entityType1, null, null, null, otherInfo,
        store.getEntity(entityId1, entityType1, EnumSet.of(Field.OTHER_INFO)));

    verifyEntityInfo(entityId2, entityType2, null, relEntityMap, null, null,
        store.getEntity(entityId2, entityType2,
            EnumSet.of(Field.RELATED_ENTITIES)));
  }

  protected List<TimelineEntity> getEntities(String entityType)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter) throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromId(String entityType,
      String fromId) throws IOException {
    return store.getEntities(entityType, null, null, null, fromId, null,
        null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromTs(String entityType,
      long fromTs) throws IOException {
    return store.getEntities(entityType, null, null, null, null, fromTs,
        null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter, String fromId)
      throws IOException {
    return store.getEntities(entityType, null, null, null, fromId, null,
        primaryFilter, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromTsWithPrimaryFilter(
      String entityType, NameValuePair primaryFilter, long fromTs)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, fromTs,
        primaryFilter, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithWindow(String entityType,
      Long windowEnd, String fromId) throws IOException {
    return store.getEntities(entityType, null, null, windowEnd, fromId, null,
        null, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesFromIdWithPrimaryFilterAndWindow(
      String entityType, Long windowEnd, String fromId,
      NameValuePair primaryFilter) throws IOException {
    return store.getEntities(entityType, null, null, windowEnd, fromId, null,
        primaryFilter, null, null).getEntities();
  }

  protected List<TimelineEntity> getEntitiesWithFilters(String entityType,
      NameValuePair primaryFilter, Collection<NameValuePair> secondaryFilters)
      throws IOException {
    return store.getEntities(entityType, null, null, null, null, null,
        primaryFilter, secondaryFilters, null).getEntities();
  }

  protected List<TimelineEntity> getEntities(String entityType, Long limit,
      Long windowStart, Long windowEnd, NameValuePair primaryFilter,
      EnumSet<Field> fields) throws IOException {
    return store.getEntities(entityType, limit, windowStart, windowEnd, null,
        null, primaryFilter, null, fields).getEntities();
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
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntities("type_2");
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, entities.get(0));

    entities = getEntities("type_1", 1l, null, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = getEntities("type_1", 1l, 0l, null, null,
        EnumSet.allOf(Field.class));
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

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
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntities("type_1", null, null, 123l, null,
        EnumSet.allOf(Field.class));
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));
  }

  public void testGetEntitiesWithFromId() throws IOException {
    List<TimelineEntity> entities = getEntitiesFromId("type_1", entityId1);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesFromId("type_1", entityId1b);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = getEntitiesFromIdWithWindow("type_1", 0l, entityId1);
    assertEquals(0, entities.size());

    entities = getEntitiesFromId("type_2", "a");
    assertEquals(0, entities.size());

    entities = getEntitiesFromId("type_2", entityId2);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, entities.get(0));

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
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesFromIdWithPrimaryFilter("type_1", userFilter,
        entityId1b);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = getEntitiesFromIdWithPrimaryFilterAndWindow("type_1", 0l,
        entityId1, userFilter);
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
    assertEquals(2, getEntitiesFromTs("type_1", afterTs).size());
    assertEquals(1, getEntitiesFromTs("type_2", afterTs).size());
    assertEquals(2, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        afterTs).size());
    assertEquals(2, getEntities("type_1").size());
    assertEquals(1, getEntities("type_2").size());
    assertEquals(2, getEntitiesWithPrimaryFilter("type_1", userFilter).size());
    // check insert time is not overwritten
    long beforeTs = this.beforeTs;
    loadTestData();
    assertEquals(0, getEntitiesFromTs("type_1", beforeTs).size());
    assertEquals(0, getEntitiesFromTs("type_2", beforeTs).size());
    assertEquals(0, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
        beforeTs).size());
    assertEquals(2, getEntitiesFromTs("type_1", afterTs).size());
    assertEquals(1, getEntitiesFromTs("type_2", afterTs).size());
    assertEquals(2, getEntitiesFromTsWithPrimaryFilter("type_1", userFilter,
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
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter1);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter2);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithPrimaryFilter("type_1", numericFilter3);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithPrimaryFilter("type_2", userFilter);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", 1l, null, null, userFilter, null);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = getEntities("type_1", 1l, 0l, null, userFilter, null);
    assertEquals(1, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = getEntities("type_1", null, 234l, null, userFilter, null);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, 234l, 345l, userFilter, null);
    assertEquals(0, entities.size());

    entities = getEntities("type_1", null, null, 345l, userFilter, null);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));
  }

  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    // test using secondary filter
    List<TimelineEntity> entities = getEntitiesWithFilters("type_1", null,
        goodTestingFilters);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithFilters("type_1", userFilter, goodTestingFilters);
    assertEquals(2, entities.size());
    verifyEntityInfo(entityId1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entityId1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = getEntitiesWithFilters("type_1", null,
        Collections.singleton(new NameValuePair("user", "none")));
    assertEquals(0, entities.size());

    entities = getEntitiesWithFilters("type_1", null, badTestingFilters);
    assertEquals(0, entities.size());

    entities = getEntitiesWithFilters("type_1", userFilter, badTestingFilters);
    assertEquals(0, entities.size());
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
      Long startTime, TimelineEntity retrievedEntityInfo) {

    verifyEntityInfo(entityId, entityType, events, relatedEntities,
        primaryFilters, otherInfo, retrievedEntityInfo);
    assertEquals(startTime, retrievedEntityInfo.getStartTime());
  }

  /**
   * Verify a single entity
   */
  protected static void verifyEntityInfo(String entityId, String entityType,
      List<TimelineEvent> events, Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters, Map<String, Object> otherInfo,
      TimelineEntity retrievedEntityInfo) {
    if (entityId == null) {
      assertNull(retrievedEntityInfo);
      return;
    }
    assertEquals(entityId, retrievedEntityInfo.getEntityId());
    assertEquals(entityType, retrievedEntityInfo.getEntityType());
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
      Map<String, Object> otherInfo) {
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

}
