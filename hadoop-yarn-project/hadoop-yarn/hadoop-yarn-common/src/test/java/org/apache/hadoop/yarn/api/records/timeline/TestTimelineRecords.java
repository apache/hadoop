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

package org.apache.hadoop.yarn.api.records.timeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestTimelineRecords {

  private static final Log LOG =
      LogFactory.getLog(TestTimelineRecords.class);

  @Test
  public void testEntities() throws Exception {
    TimelineEntities entities = new TimelineEntities();
    for (int j = 0; j < 2; ++j) {
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("entity id " + j);
      entity.setEntityType("entity type " + j);
      entity.setStartTime(System.currentTimeMillis());
      for (int i = 0; i < 2; ++i) {
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType("event type " + i);
        event.addEventInfo("key1", "val1");
        event.addEventInfo("key2", "val2");
        entity.addEvent(event);
      }
      entity.addRelatedEntity("test ref type 1", "test ref id 1");
      entity.addRelatedEntity("test ref type 2", "test ref id 2");
      entity.addPrimaryFilter("pkey1", "pval1");
      entity.addPrimaryFilter("pkey2", "pval2");
      entity.addOtherInfo("okey1", "oval1");
      entity.addOtherInfo("okey2", "oval2");
      entity.setDomainId("domain id " + j);
      entities.addEntity(entity);
    }
    LOG.info("Entities in JSON:");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(entities, true));

    Assert.assertEquals(2, entities.getEntities().size());
    TimelineEntity entity1 = entities.getEntities().get(0);
    Assert.assertEquals("entity id 0", entity1.getEntityId());
    Assert.assertEquals("entity type 0", entity1.getEntityType());
    Assert.assertEquals(2, entity1.getRelatedEntities().size());
    Assert.assertEquals(2, entity1.getEvents().size());
    Assert.assertEquals(2, entity1.getPrimaryFilters().size());
    Assert.assertEquals(2, entity1.getOtherInfo().size());
    Assert.assertEquals("domain id 0", entity1.getDomainId());
    TimelineEntity entity2 = entities.getEntities().get(1);
    Assert.assertEquals("entity id 1", entity2.getEntityId());
    Assert.assertEquals("entity type 1", entity2.getEntityType());
    Assert.assertEquals(2, entity2.getRelatedEntities().size());
    Assert.assertEquals(2, entity2.getEvents().size());
    Assert.assertEquals(2, entity2.getPrimaryFilters().size());
    Assert.assertEquals(2, entity2.getOtherInfo().size());
    Assert.assertEquals("domain id 1", entity2.getDomainId());
  }

  @Test
  public void testEvents() throws Exception {
    TimelineEvents events = new TimelineEvents();
    for (int j = 0; j < 2; ++j) {
      TimelineEvents.EventsOfOneEntity partEvents =
          new TimelineEvents.EventsOfOneEntity();
      partEvents.setEntityId("entity id " + j);
      partEvents.setEntityType("entity type " + j);
      for (int i = 0; i < 2; ++i) {
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType("event type " + i);
        event.addEventInfo("key1", "val1");
        event.addEventInfo("key2", "val2");
        partEvents.addEvent(event);
      }
      events.addEvent(partEvents);
    }
    LOG.info("Events in JSON:");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(events, true));

    Assert.assertEquals(2, events.getAllEvents().size());
    TimelineEvents.EventsOfOneEntity partEvents1 = events.getAllEvents().get(0);
    Assert.assertEquals("entity id 0", partEvents1.getEntityId());
    Assert.assertEquals("entity type 0", partEvents1.getEntityType());
    Assert.assertEquals(2, partEvents1.getEvents().size());
    TimelineEvent event11 = partEvents1.getEvents().get(0);
    Assert.assertEquals("event type 0", event11.getEventType());
    Assert.assertEquals(2, event11.getEventInfo().size());
    TimelineEvent event12 = partEvents1.getEvents().get(1);
    Assert.assertEquals("event type 1", event12.getEventType());
    Assert.assertEquals(2, event12.getEventInfo().size());
    TimelineEvents.EventsOfOneEntity partEvents2 = events.getAllEvents().get(1);
    Assert.assertEquals("entity id 1", partEvents2.getEntityId());
    Assert.assertEquals("entity type 1", partEvents2.getEntityType());
    Assert.assertEquals(2, partEvents2.getEvents().size());
    TimelineEvent event21 = partEvents2.getEvents().get(0);
    Assert.assertEquals("event type 0", event21.getEventType());
    Assert.assertEquals(2, event21.getEventInfo().size());
    TimelineEvent event22 = partEvents2.getEvents().get(1);
    Assert.assertEquals("event type 1", event22.getEventType());
    Assert.assertEquals(2, event22.getEventInfo().size());
  }

  @Test
  public void testTimelinePutErrors() throws Exception {
    TimelinePutResponse TimelinePutErrors = new TimelinePutResponse();
    TimelinePutError error1 = new TimelinePutError();
    error1.setEntityId("entity id 1");
    error1.setEntityId("entity type 1");
    error1.setErrorCode(TimelinePutError.NO_START_TIME);
    TimelinePutErrors.addError(error1);
    List<TimelinePutError> response = new ArrayList<TimelinePutError>();
    response.add(error1);
    TimelinePutError error2 = new TimelinePutError();
    error2.setEntityId("entity id 2");
    error2.setEntityId("entity type 2");
    error2.setErrorCode(TimelinePutError.IO_EXCEPTION);
    response.add(error2);
    TimelinePutErrors.addErrors(response);
    LOG.info("Errors in JSON:");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(TimelinePutErrors, true));

    Assert.assertEquals(3, TimelinePutErrors.getErrors().size());
    TimelinePutError e = TimelinePutErrors.getErrors().get(0);
    Assert.assertEquals(error1.getEntityId(), e.getEntityId());
    Assert.assertEquals(error1.getEntityType(), e.getEntityType());
    Assert.assertEquals(error1.getErrorCode(), e.getErrorCode());
    e = TimelinePutErrors.getErrors().get(1);
    Assert.assertEquals(error1.getEntityId(), e.getEntityId());
    Assert.assertEquals(error1.getEntityType(), e.getEntityType());
    Assert.assertEquals(error1.getErrorCode(), e.getErrorCode());
    e = TimelinePutErrors.getErrors().get(2);
    Assert.assertEquals(error2.getEntityId(), e.getEntityId());
    Assert.assertEquals(error2.getEntityType(), e.getEntityType());
    Assert.assertEquals(error2.getErrorCode(), e.getErrorCode());
  }

  @Test
  public void testTimelineDomain() throws Exception {
    TimelineDomains domains = new TimelineDomains();

    TimelineDomain domain = null;
    for (int i = 0; i < 2; ++i) {
      domain = new TimelineDomain();
      domain.setId("test id " + (i + 1));
      domain.setDescription("test description " + (i + 1));
      domain.setOwner("test owner " + (i + 1));
      domain.setReaders("test_reader_user_" + (i + 1) +
          " test_reader_group+" + (i + 1));
      domain.setWriters("test_writer_user_" + (i + 1) +
          " test_writer_group+" + (i + 1));
      domain.setCreatedTime(0L);
      domain.setModifiedTime(1L);
      domains.addDomain(domain);
    }
    LOG.info("Domain in JSON:");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(domains, true));

    Assert.assertEquals(2, domains.getDomains().size());

    for (int i = 0; i < domains.getDomains().size(); ++i) {
      domain = domains.getDomains().get(i);
      Assert.assertEquals("test id " + (i + 1), domain.getId());
      Assert.assertEquals("test description " + (i + 1),
          domain.getDescription());
      Assert.assertEquals("test owner " + (i + 1), domain.getOwner());
      Assert.assertEquals("test_reader_user_" + (i + 1) +
          " test_reader_group+" + (i + 1), domain.getReaders());
      Assert.assertEquals("test_writer_user_" + (i + 1) +
          " test_writer_group+" + (i + 1), domain.getWriters());
      Assert.assertEquals(new Long(0L), domain.getCreatedTime());
      Assert.assertEquals(new Long(1L), domain.getModifiedTime());
    }
  }

  @Test
  public void testMapInterfaceOrTimelineRecords() throws Exception {
    TimelineEntity entity = new TimelineEntity();
    List<Map<String, Set<Object>>> primaryFiltersList =
        new ArrayList<Map<String, Set<Object>>>();
    primaryFiltersList.add(
        Collections.singletonMap("pkey", Collections.singleton((Object) "pval")));
    Map<String, Set<Object>> primaryFilters = new TreeMap<String, Set<Object>>();
    primaryFilters.put("pkey1", Collections.singleton((Object) "pval1"));
    primaryFilters.put("pkey2", Collections.singleton((Object) "pval2"));
    primaryFiltersList.add(primaryFilters);
    entity.setPrimaryFilters(null);
    for (Map<String, Set<Object>> primaryFiltersToSet : primaryFiltersList) {
      entity.setPrimaryFilters(primaryFiltersToSet);
      assertPrimaryFilters(entity);

      Map<String, Set<Object>> primaryFiltersToAdd =
          new WeakHashMap<String, Set<Object>>();
      primaryFiltersToAdd.put("pkey3", Collections.singleton((Object) "pval3"));
      entity.addPrimaryFilters(primaryFiltersToAdd);
      assertPrimaryFilters(entity);
    }

    List<Map<String, Set<String>>> relatedEntitiesList =
        new ArrayList<Map<String, Set<String>>>();
    relatedEntitiesList.add(
        Collections.singletonMap("rkey", Collections.singleton("rval")));
    Map<String, Set<String>> relatedEntities = new TreeMap<String, Set<String>>();
    relatedEntities.put("rkey1", Collections.singleton("rval1"));
    relatedEntities.put("rkey2", Collections.singleton("rval2"));
    relatedEntitiesList.add(relatedEntities);
    entity.setRelatedEntities(null);
    for (Map<String, Set<String>> relatedEntitiesToSet : relatedEntitiesList) {
      entity.setRelatedEntities(relatedEntitiesToSet);
      assertRelatedEntities(entity);

      Map<String, Set<String>> relatedEntitiesToAdd =
          new WeakHashMap<String, Set<String>>();
      relatedEntitiesToAdd.put("rkey3", Collections.singleton("rval3"));
      entity.addRelatedEntities(relatedEntitiesToAdd);
      assertRelatedEntities(entity);
    }

    List<Map<String, Object>> otherInfoList =
        new ArrayList<Map<String, Object>>();
    otherInfoList.add(Collections.singletonMap("okey", (Object) "oval"));
    Map<String, Object> otherInfo = new TreeMap<String, Object>();
    otherInfo.put("okey1", "oval1");
    otherInfo.put("okey2", "oval2");
    otherInfoList.add(otherInfo);
    entity.setOtherInfo(null);
    for (Map<String, Object> otherInfoToSet : otherInfoList) {
      entity.setOtherInfo(otherInfoToSet);
      assertOtherInfo(entity);

      Map<String, Object> otherInfoToAdd = new WeakHashMap<String, Object>();
      otherInfoToAdd.put("okey3", "oval3");
      entity.addOtherInfo(otherInfoToAdd);
      assertOtherInfo(entity);
    }

    TimelineEvent event = new TimelineEvent();
    List<Map<String, Object>> eventInfoList =
        new ArrayList<Map<String, Object>>();
    eventInfoList.add(Collections.singletonMap("ekey", (Object) "eval"));
    Map<String, Object> eventInfo = new TreeMap<String, Object>();
    eventInfo.put("ekey1", "eval1");
    eventInfo.put("ekey2", "eval2");
    eventInfoList.add(eventInfo);
    event.setEventInfo(null);
    for (Map<String, Object> eventInfoToSet : eventInfoList) {
      event.setEventInfo(eventInfoToSet);
      assertEventInfo(event);

      Map<String, Object> eventInfoToAdd = new WeakHashMap<String, Object>();
      eventInfoToAdd.put("ekey3", "eval3");
      event.addEventInfo(eventInfoToAdd);
      assertEventInfo(event);
    }
  }

  private static void assertPrimaryFilters(TimelineEntity entity) {
    Assert.assertNotNull(entity.getPrimaryFilters());
    Assert.assertNotNull(entity.getPrimaryFiltersJAXB());
    Assert.assertTrue(entity.getPrimaryFilters() instanceof HashMap);
    Assert.assertTrue(entity.getPrimaryFiltersJAXB() instanceof HashMap);
    Assert.assertEquals(
        entity.getPrimaryFilters(), entity.getPrimaryFiltersJAXB());
  }

  private static void assertRelatedEntities(TimelineEntity entity) {
    Assert.assertNotNull(entity.getRelatedEntities());
    Assert.assertNotNull(entity.getRelatedEntitiesJAXB());
    Assert.assertTrue(entity.getRelatedEntities() instanceof HashMap);
    Assert.assertTrue(entity.getRelatedEntitiesJAXB() instanceof HashMap);
    Assert.assertEquals(
        entity.getRelatedEntities(), entity.getRelatedEntitiesJAXB());
  }

  private static void assertOtherInfo(TimelineEntity entity) {
    Assert.assertNotNull(entity.getOtherInfo());
    Assert.assertNotNull(entity.getOtherInfoJAXB());
    Assert.assertTrue(entity.getOtherInfo() instanceof HashMap);
    Assert.assertTrue(entity.getOtherInfoJAXB() instanceof HashMap);
    Assert.assertEquals(entity.getOtherInfo(), entity.getOtherInfoJAXB());
  }

  private static void assertEventInfo(TimelineEvent event) {
    Assert.assertNotNull(event);
    Assert.assertNotNull(event.getEventInfoJAXB());
    Assert.assertTrue(event.getEventInfo() instanceof HashMap);
    Assert.assertTrue(event.getEventInfoJAXB() instanceof HashMap);
    Assert.assertEquals(event.getEventInfo(), event.getEventInfoJAXB());
  }
}
