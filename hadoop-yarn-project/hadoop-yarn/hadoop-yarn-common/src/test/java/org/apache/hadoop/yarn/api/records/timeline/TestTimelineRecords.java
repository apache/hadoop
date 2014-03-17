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
import java.util.List;

import org.junit.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
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
    TimelineEntity entity2 = entities.getEntities().get(1);
    Assert.assertEquals("entity id 1", entity2.getEntityId());
    Assert.assertEquals("entity type 1", entity2.getEntityType());
    Assert.assertEquals(2, entity2.getRelatedEntities().size());
    Assert.assertEquals(2, entity2.getEvents().size());
    Assert.assertEquals(2, entity2.getPrimaryFilters().size());
    Assert.assertEquals(2, entity2.getOtherInfo().size());
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

}
