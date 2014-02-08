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

package org.apache.hadoop.yarn.api.records.apptimeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors.ATSPutError;
import org.junit.Test;

public class TestApplicationTimelineRecords {

  @Test
  public void testATSEntities() {
    ATSEntities entities = new ATSEntities();
    for (int j = 0; j < 2; ++j) {
      ATSEntity entity = new ATSEntity();
      entity.setEntityId("entity id " + j);
      entity.setEntityType("entity type " + j);
      entity.setStartTime(System.currentTimeMillis());
      for (int i = 0; i < 2; ++i) {
        ATSEvent event = new ATSEvent();
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
    Assert.assertEquals(2, entities.getEntities().size());
    ATSEntity entity1 = entities.getEntities().get(0);
    Assert.assertEquals("entity id 0", entity1.getEntityId());
    Assert.assertEquals("entity type 0", entity1.getEntityType());
    Assert.assertEquals(2, entity1.getRelatedEntities().size());
    Assert.assertEquals(2, entity1.getEvents().size());
    Assert.assertEquals(2, entity1.getPrimaryFilters().size());
    Assert.assertEquals(2, entity1.getOtherInfo().size());
    ATSEntity entity2 = entities.getEntities().get(1);
    Assert.assertEquals("entity id 1", entity2.getEntityId());
    Assert.assertEquals("entity type 1", entity2.getEntityType());
    Assert.assertEquals(2, entity2.getRelatedEntities().size());
    Assert.assertEquals(2, entity2.getEvents().size());
    Assert.assertEquals(2, entity2.getPrimaryFilters().size());
    Assert.assertEquals(2, entity2.getOtherInfo().size());
  }

  @Test
  public void testATSEvents() {
    ATSEvents events = new ATSEvents();
    for (int j = 0; j < 2; ++j) {
      ATSEvents.ATSEventsOfOneEntity partEvents =
          new ATSEvents.ATSEventsOfOneEntity();
      partEvents.setEntityId("entity id " + j);
      partEvents.setEntityType("entity type " + j);
      for (int i = 0; i < 2; ++i) {
        ATSEvent event = new ATSEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType("event type " + i);
        event.addEventInfo("key1", "val1");
        event.addEventInfo("key2", "val2");
        partEvents.addEvent(event);
      }
      events.addEvent(partEvents);
    }
    Assert.assertEquals(2, events.getAllEvents().size());
    ATSEvents.ATSEventsOfOneEntity partEvents1 = events.getAllEvents().get(0);
    Assert.assertEquals("entity id 0", partEvents1.getEntityId());
    Assert.assertEquals("entity type 0", partEvents1.getEntityType());
    Assert.assertEquals(2, partEvents1.getEvents().size());
    ATSEvent event11 = partEvents1.getEvents().get(0);
    Assert.assertEquals("event type 0", event11.getEventType());
    Assert.assertEquals(2, event11.getEventInfo().size());
    ATSEvent event12 = partEvents1.getEvents().get(1);
    Assert.assertEquals("event type 1", event12.getEventType());
    Assert.assertEquals(2, event12.getEventInfo().size());
    ATSEvents.ATSEventsOfOneEntity partEvents2 = events.getAllEvents().get(1);
    Assert.assertEquals("entity id 1", partEvents2.getEntityId());
    Assert.assertEquals("entity type 1", partEvents2.getEntityType());
    Assert.assertEquals(2, partEvents2.getEvents().size());
    ATSEvent event21 = partEvents2.getEvents().get(0);
    Assert.assertEquals("event type 0", event21.getEventType());
    Assert.assertEquals(2, event21.getEventInfo().size());
    ATSEvent event22 = partEvents2.getEvents().get(1);
    Assert.assertEquals("event type 1", event22.getEventType());
    Assert.assertEquals(2, event22.getEventInfo().size());
  }

  @Test
  public void testATSPutErrors() {
    ATSPutErrors atsPutErrors = new ATSPutErrors();
    ATSPutError error1 = new ATSPutError();
    error1.setEntityId("entity id 1");
    error1.setEntityId("entity type 1");
    error1.setErrorCode(ATSPutError.NO_START_TIME);
    atsPutErrors.addError(error1);
    List<ATSPutError> errors = new ArrayList<ATSPutError>();
    errors.add(error1);
    ATSPutError error2 = new ATSPutError();
    error2.setEntityId("entity id 2");
    error2.setEntityId("entity type 2");
    error2.setErrorCode(ATSPutError.IO_EXCEPTION);
    errors.add(error2);
    atsPutErrors.addErrors(errors);

    Assert.assertEquals(3, atsPutErrors.getErrors().size());
    ATSPutError e = atsPutErrors.getErrors().get(0);
    Assert.assertEquals(error1.getEntityId(), e.getEntityId());
    Assert.assertEquals(error1.getEntityType(), e.getEntityType());
    Assert.assertEquals(error1.getErrorCode(), e.getErrorCode());
    e = atsPutErrors.getErrors().get(1);
    Assert.assertEquals(error1.getEntityId(), e.getEntityId());
    Assert.assertEquals(error1.getEntityType(), e.getEntityType());
    Assert.assertEquals(error1.getErrorCode(), e.getErrorCode());
    e = atsPutErrors.getErrors().get(2);
    Assert.assertEquals(error2.getEntityId(), e.getEntityId());
    Assert.assertEquals(error2.getEntityType(), e.getEntityType());
    Assert.assertEquals(error2.getErrorCode(), e.getErrorCode());
  }

}
