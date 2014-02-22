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

import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvent;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents.ATSEventsOfOneEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors.ATSPutError;
import org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline.ApplicationTimelineReader.Field;

public class ApplicationTimelineStoreTestUtils {

  protected static final Map<String, Object> EMPTY_MAP =
      Collections.emptyMap();
  protected static final Map<String, Set<Object>> EMPTY_PRIMARY_FILTERS =
      Collections.emptyMap();
  protected static final Map<String, Set<String>> EMPTY_REL_ENTITIES =
      Collections.emptyMap();

  protected ApplicationTimelineStore store;
  protected String entity1;
  protected String entityType1;
  protected String entity1b;
  protected String entity2;
  protected String entityType2;
  protected Map<String, Set<Object>> primaryFilters;
  protected Map<String, Object> secondaryFilters;
  protected Map<String, Object> allFilters;
  protected Map<String, Object> otherInfo;
  protected Map<String, Set<String>> relEntityMap;
  protected NameValuePair userFilter;
  protected Collection<NameValuePair> goodTestingFilters;
  protected Collection<NameValuePair> badTestingFilters;
  protected ATSEvent ev1;
  protected ATSEvent ev2;
  protected ATSEvent ev3;
  protected ATSEvent ev4;
  protected Map<String, Object> eventInfo;
  protected List<ATSEvent> events1;
  protected List<ATSEvent> events2;

  /**
   * Load test data into the given store
   */
  protected void loadTestData() throws IOException {
    ATSEntities atsEntities = new ATSEntities();
    Map<String, Set<Object>> primaryFilters =
        new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(12345l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    Map<String, Object> secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456l);
    secondaryFilters.put("status", "RUNNING");
    Map<String, Object> otherInfo1 = new HashMap<String, Object>();
    otherInfo1.put("info1", "val1");
    otherInfo1.putAll(secondaryFilters);

    String entity1 = "id_1";
    String entityType1 = "type_1";
    String entity1b = "id_2";
    String entity2 = "id_2";
    String entityType2 = "type_2";

    Map<String, Set<String>> relatedEntities =
        new HashMap<String, Set<String>>();
    relatedEntities.put(entityType2, Collections.singleton(entity2));

    ATSEvent ev3 = createEvent(789l, "launch_event", null);
    ATSEvent ev4 = createEvent(-123l, "init_event", null);
    List<ATSEvent> events = new ArrayList<ATSEvent>();
    events.add(ev3);
    events.add(ev4);
    atsEntities.setEntities(Collections.singletonList(createEntity(entity2,
        entityType2, null, events, null, null, null)));
    ATSPutErrors response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());

    ATSEvent ev1 = createEvent(123l, "start_event", null);
    atsEntities.setEntities(Collections.singletonList(createEntity(entity1,
        entityType1, 123l, Collections.singletonList(ev1),
        relatedEntities, primaryFilters, otherInfo1)));
    response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());
    atsEntities.setEntities(Collections.singletonList(createEntity(entity1b,
        entityType1, null, Collections.singletonList(ev1), relatedEntities,
        primaryFilters, otherInfo1)));
    response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());

    Map<String, Object> eventInfo = new HashMap<String, Object>();
    eventInfo.put("event info 1", "val1");
    ATSEvent ev2 = createEvent(456l, "end_event", eventInfo);
    Map<String, Object> otherInfo2 = new HashMap<String, Object>();
    otherInfo2.put("info2", "val2");
    atsEntities.setEntities(Collections.singletonList(createEntity(entity1,
        entityType1, null, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2)));
    response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());
    atsEntities.setEntities(Collections.singletonList(createEntity(entity1b,
        entityType1, 789l, Collections.singletonList(ev2), null,
        primaryFilters, otherInfo2)));
    response = store.put(atsEntities);
    assertEquals(0, response.getErrors().size());

    atsEntities.setEntities(Collections.singletonList(createEntity(
        "badentityid", "badentity", null, null, null, null, otherInfo1)));
    response = store.put(atsEntities);
    assertEquals(1, response.getErrors().size());
    ATSPutError error = response.getErrors().get(0);
    assertEquals("badentityid", error.getEntityId());
    assertEquals("badentity", error.getEntityType());
    assertEquals(ATSPutError.NO_START_TIME, error.getErrorCode());
  }

  /**
   * Load verification data
   */
  protected void loadVerificationData() throws Exception {
    userFilter = new NameValuePair("user",
        "username");
    goodTestingFilters = new ArrayList<NameValuePair>();
    goodTestingFilters.add(new NameValuePair("appname", 12345l));
    goodTestingFilters.add(new NameValuePair("status", "RUNNING"));
    badTestingFilters = new ArrayList<NameValuePair>();
    badTestingFilters.add(new NameValuePair("appname", 12345l));
    badTestingFilters.add(new NameValuePair("status", "FINISHED"));

    primaryFilters = new HashMap<String, Set<Object>>();
    Set<Object> l1 = new HashSet<Object>();
    l1.add("username");
    Set<Object> l2 = new HashSet<Object>();
    l2.add(12345l);
    primaryFilters.put("user", l1);
    primaryFilters.put("appname", l2);
    secondaryFilters = new HashMap<String, Object>();
    secondaryFilters.put("startTime", 123456l);
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

    entity1 = "id_1";
    entityType1 = "type_1";
    entity1b = "id_2";
    entity2 = "id_2";
    entityType2 = "type_2";

    ev1 = createEvent(123l, "start_event", null);

    eventInfo = new HashMap<String, Object>();
    eventInfo.put("event info 1", "val1");
    ev2 = createEvent(456l, "end_event", eventInfo);
    events1 = new ArrayList<ATSEvent>();
    events1.add(ev2);
    events1.add(ev1);

    relEntityMap =
        new HashMap<String, Set<String>>();
    Set<String> ids = new HashSet<String>();
    ids.add(entity1);
    ids.add(entity1b);
    relEntityMap.put(entityType1, ids);

    ev3 = createEvent(789l, "launch_event", null);
    ev4 = createEvent(-123l, "init_event", null);
    events2 = new ArrayList<ATSEvent>();
    events2.add(ev3);
    events2.add(ev4);
  }

  public void testGetSingleEntity() throws IOException {
    // test getting entity info
    verifyEntityInfo(null, null, null, null, null, null,
        store.getEntity("id_1", "type_2", EnumSet.allOf(Field.class)));

    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entity1, entityType1,
        EnumSet.allOf(Field.class)));

    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entity1b, entityType1,
        EnumSet.allOf(Field.class)));

    verifyEntityInfo(entity2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, store.getEntity(entity2, entityType2,
        EnumSet.allOf(Field.class)));

    // test getting single fields
    verifyEntityInfo(entity1, entityType1, events1, null, null, null,
        store.getEntity(entity1, entityType1, EnumSet.of(Field.EVENTS)));

    verifyEntityInfo(entity1, entityType1, Collections.singletonList(ev2),
        null, null, null, store.getEntity(entity1, entityType1,
        EnumSet.of(Field.LAST_EVENT_ONLY)));

    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, store.getEntity(entity1b, entityType1,
        null));

    verifyEntityInfo(entity1, entityType1, null, null, primaryFilters, null,
        store.getEntity(entity1, entityType1,
            EnumSet.of(Field.PRIMARY_FILTERS)));

    verifyEntityInfo(entity1, entityType1, null, null, null, otherInfo,
        store.getEntity(entity1, entityType1, EnumSet.of(Field.OTHER_INFO)));

    verifyEntityInfo(entity2, entityType2, null, relEntityMap, null, null,
        store.getEntity(entity2, entityType2,
            EnumSet.of(Field.RELATED_ENTITIES)));
  }

  public void testGetEntities() throws IOException {
    // test getting entities
    assertEquals("nonzero entities size for nonexistent type", 0,
        store.getEntities("type_0", null, null, null, null, null,
            null).getEntities().size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        store.getEntities("type_3", null, null, null, null, null,
            null).getEntities().size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        store.getEntities("type_0", null, null, null, userFilter,
            null, null).getEntities().size());
    assertEquals("nonzero entities size for nonexistent type", 0,
        store.getEntities("type_3", null, null, null, userFilter,
            null, null).getEntities().size());

    List<ATSEntity> entities =
        store.getEntities("type_1", null, null, null, null, null,
            EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = store.getEntities("type_2", null, null, null, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(1, entities.size());
    verifyEntityInfo(entity2, entityType2, events2, relEntityMap,
        EMPTY_PRIMARY_FILTERS, EMPTY_MAP, entities.get(0));

    entities = store.getEntities("type_1", 1l, null, null, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(1, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = store.getEntities("type_1", 1l, 0l, null, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(1, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = store.getEntities("type_1", null, 234l, null, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, 123l, null, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, 234l, 345l, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, null, 345l, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = store.getEntities("type_1", null, null, 123l, null, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));
  }

  public void testGetEntitiesWithPrimaryFilters() throws IOException {
    // test using primary filter
    assertEquals("nonzero entities size for primary filter", 0,
        store.getEntities("type_1", null, null, null,
            new NameValuePair("none", "none"), null,
            EnumSet.allOf(Field.class)).getEntities().size());
    assertEquals("nonzero entities size for primary filter", 0,
        store.getEntities("type_2", null, null, null,
            new NameValuePair("none", "none"), null,
            EnumSet.allOf(Field.class)).getEntities().size());
    assertEquals("nonzero entities size for primary filter", 0,
        store.getEntities("type_3", null, null, null,
            new NameValuePair("none", "none"), null,
            EnumSet.allOf(Field.class)).getEntities().size());

    List<ATSEntity> entities = store.getEntities("type_1", null, null, null,
        userFilter, null, EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = store.getEntities("type_2", null, null, null, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", 1l, null, null, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(1, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = store.getEntities("type_1", 1l, 0l, null, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(1, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));

    entities = store.getEntities("type_1", null, 234l, null, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, 234l, 345l, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, null, 345l, userFilter, null,
        EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));
  }

  public void testGetEntitiesWithSecondaryFilters() throws IOException {
    // test using secondary filter
    List<ATSEntity> entities = store.getEntities("type_1", null, null, null,
        null, goodTestingFilters, EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = store.getEntities("type_1", null, null, null, userFilter,
        goodTestingFilters, EnumSet.allOf(Field.class)).getEntities();
    assertEquals(2, entities.size());
    verifyEntityInfo(entity1, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(0));
    verifyEntityInfo(entity1b, entityType1, events1, EMPTY_REL_ENTITIES,
        primaryFilters, otherInfo, entities.get(1));

    entities = store.getEntities("type_1", null, null, null, null,
        badTestingFilters, EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());

    entities = store.getEntities("type_1", null, null, null, userFilter,
        badTestingFilters, EnumSet.allOf(Field.class)).getEntities();
    assertEquals(0, entities.size());
  }

  public void testGetEvents() throws IOException {
    // test getting entity timelines
    SortedSet<String> sortedSet = new TreeSet<String>();
    sortedSet.add(entity1);
    List<ATSEventsOfOneEntity> timelines =
        store.getEntityTimelines(entityType1, sortedSet, null, null,
            null, null).getAllEvents();
    assertEquals(1, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2, ev1);

    sortedSet.add(entity1b);
    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2, ev1);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev2, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, 1l,
        null, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        345l, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        123l, null, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev2);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, 345l, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev1);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, 123l, null).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev1);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev1);

    timelines = store.getEntityTimelines(entityType1, sortedSet, null,
        null, null, Collections.singleton("end_event")).getAllEvents();
    assertEquals(2, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity1, entityType1, ev2);
    verifyEntityTimeline(timelines.get(1), entity1b, entityType1, ev2);

    sortedSet.add(entity2);
    timelines = store.getEntityTimelines(entityType2, sortedSet, null,
        null, null, null).getAllEvents();
    assertEquals(1, timelines.size());
    verifyEntityTimeline(timelines.get(0), entity2, entityType2, ev3, ev4);
  }

  /**
   * Verify a single entity
   */
  protected static void verifyEntityInfo(String entity, String entityType,
      List<ATSEvent> events, Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters, Map<String, Object> otherInfo,
      ATSEntity retrievedEntityInfo) {
    if (entity == null) {
      assertNull(retrievedEntityInfo);
      return;
    }
    assertEquals(entity, retrievedEntityInfo.getEntityId());
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
      ATSEventsOfOneEntity retrievedEvents, String entity, String entityType,
      ATSEvent... actualEvents) {
    assertEquals(entity, retrievedEvents.getEntityId());
    assertEquals(entityType, retrievedEvents.getEntityType());
    assertEquals(actualEvents.length, retrievedEvents.getEvents().size());
    for (int i = 0; i < actualEvents.length; i++) {
      assertEquals(actualEvents[i], retrievedEvents.getEvents().get(i));
    }
  }

  /**
   * Create a test entity
   */
  protected static ATSEntity createEntity(String entity, String entityType,
      Long startTime, List<ATSEvent> events,
      Map<String, Set<String>> relatedEntities,
      Map<String, Set<Object>> primaryFilters,
      Map<String, Object> otherInfo) {
    ATSEntity atsEntity = new ATSEntity();
    atsEntity.setEntityId(entity);
    atsEntity.setEntityType(entityType);
    atsEntity.setStartTime(startTime);
    atsEntity.setEvents(events);
    if (relatedEntities != null) {
      for (Entry<String, Set<String>> e : relatedEntities.entrySet()) {
        for (String v : e.getValue()) {
          atsEntity.addRelatedEntity(e.getKey(), v);
        }
      }
    } else {
      atsEntity.setRelatedEntities(null);
    }
    atsEntity.setPrimaryFilters(primaryFilters);
    atsEntity.setOtherInfo(otherInfo);
    return atsEntity;
  }

  /**
   * Create a test event
   */
  private static ATSEvent createEvent(long timestamp, String type, Map<String,
      Object> info) {
    ATSEvent event = new ATSEvent();
    event.setTimestamp(timestamp);
    event.setEventType(type);
    event.setEventInfo(info);
    return event;
  }

}
