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

package org.apache.hadoop.yarn.server.applicationhistoryservice.webapp;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.TestMemoryTimelineStore;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;


public class TestTimelineWebServices extends JerseyTest {

  private static TimelineStore store;
  private long beforeTime;

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(YarnJacksonJaxbJsonProvider.class);
      bind(TimelineWebServices.class);
      bind(GenericExceptionHandler.class);
      try{
        store = mockTimelineStore();
      } catch (Exception e) {
        Assert.fail();
      }
      bind(TimelineStore.class).toInstance(store);
      serve("/*").with(GuiceContainer.class);
    }

  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  private TimelineStore mockTimelineStore()
      throws Exception {
    beforeTime = System.currentTimeMillis() - 1;
    TestMemoryTimelineStore store =
        new TestMemoryTimelineStore();
    store.setup();
    return store.getTimelineStore();
  }

  public TestTimelineWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.applicationhistoryservice.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter")
        .servletPath("/")
        .clientConfig(new DefaultClientConfig(YarnJacksonJaxbJsonProvider.class))
        .build());
  }

  @Test
  public void testAbout() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineWebServices.AboutInfo about =
        response.getEntity(TimelineWebServices.AboutInfo.class);
    Assert.assertNotNull(about);
    Assert.assertEquals("Timeline API", about.getAbout());
  }

  private static void verifyEntities(TimelineEntities entities) {
    Assert.assertNotNull(entities);
    Assert.assertEquals(2, entities.getEntities().size());
    TimelineEntity entity1 = entities.getEntities().get(0);
    Assert.assertNotNull(entity1);
    Assert.assertEquals("id_1", entity1.getEntityId());
    Assert.assertEquals("type_1", entity1.getEntityType());
    Assert.assertEquals(123l, entity1.getStartTime().longValue());
    Assert.assertEquals(2, entity1.getEvents().size());
    Assert.assertEquals(4, entity1.getPrimaryFilters().size());
    Assert.assertEquals(4, entity1.getOtherInfo().size());
    TimelineEntity entity2 = entities.getEntities().get(1);
    Assert.assertNotNull(entity2);
    Assert.assertEquals("id_2", entity2.getEntityId());
    Assert.assertEquals("type_1", entity2.getEntityType());
    Assert.assertEquals(123l, entity2.getStartTime().longValue());
    Assert.assertEquals(2, entity2.getEvents().size());
    Assert.assertEquals(4, entity2.getPrimaryFilters().size());
    Assert.assertEquals(4, entity2.getOtherInfo().size());
  }

  @Test
  public void testGetEntities() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testFromId() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_2")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(1, response.getEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(2, response.getEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testFromTs() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(beforeTime))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(0, response.getEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(
            System.currentTimeMillis()))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(2, response.getEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testPrimaryFilterString() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter", "user:username")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterInteger() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter",
            "appname:" + Integer.toString(Integer.MAX_VALUE))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterLong() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter",
            "long:" + Long.toString((long)Integer.MAX_VALUE + 1l))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterNumericString() {
    // without quotes, 123abc is interpreted as the number 123,
    // which finds no entities
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter", "other:123abc")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    assertEquals(0, response.getEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testPrimaryFilterNumericStringWithQuotes() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter", "other:\"123abc\"")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testSecondaryFilters() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1")
        .queryParam("secondaryFilter",
            "user:username,appname:" + Integer.toString(Integer.MAX_VALUE))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testGetEntity() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("id_1", entity.getEntityId());
    Assert.assertEquals("type_1", entity.getEntityType());
    Assert.assertEquals(123l, entity.getStartTime().longValue());
    Assert.assertEquals(2, entity.getEvents().size());
    Assert.assertEquals(4, entity.getPrimaryFilters().size());
    Assert.assertEquals(4, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEntityFields1() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1").queryParam("fields", "events,otherinfo")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("id_1", entity.getEntityId());
    Assert.assertEquals("type_1", entity.getEntityType());
    Assert.assertEquals(123l, entity.getStartTime().longValue());
    Assert.assertEquals(2, entity.getEvents().size());
    Assert.assertEquals(0, entity.getPrimaryFilters().size());
    Assert.assertEquals(4, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEntityFields2() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1").queryParam("fields", "lasteventonly," +
            "primaryfilters,relatedentities")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineEntity entity = response.getEntity(TimelineEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("id_1", entity.getEntityId());
    Assert.assertEquals("type_1", entity.getEntityType());
    Assert.assertEquals(123l, entity.getStartTime().longValue());
    Assert.assertEquals(1, entity.getEvents().size());
    Assert.assertEquals(4, entity.getPrimaryFilters().size());
    Assert.assertEquals(0, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEvents() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("events")
        .queryParam("entityId", "id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelineEvents events = response.getEntity(TimelineEvents.class);
    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.getAllEvents().size());
    TimelineEvents.EventsOfOneEntity partEvents = events.getAllEvents().get(0);
    Assert.assertEquals(2, partEvents.getEvents().size());
    TimelineEvent event1 = partEvents.getEvents().get(0);
    Assert.assertEquals(456l, event1.getTimestamp());
    Assert.assertEquals("end_event", event1.getEventType());
    Assert.assertEquals(1, event1.getEventInfo().size());
    TimelineEvent event2 = partEvents.getEvents().get(1);
    Assert.assertEquals(123l, event2.getTimestamp());
    Assert.assertEquals("start_event", event2.getEventType());
    Assert.assertEquals(0, event2.getEventInfo().size());
  }

  @Test
  public void testPostEntities() throws Exception {
    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("test id");
    entity.setEntityType("test type");
    entity.setStartTime(System.currentTimeMillis());
    entities.addEntity(entity);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    TimelinePutResponse putResposne = response.getEntity(TimelinePutResponse.class);
    Assert.assertNotNull(putResposne);
    Assert.assertEquals(0, putResposne.getErrors().size());
    // verify the entity exists in the store
    response = r.path("ws").path("v1").path("timeline")
        .path("test type").path("test id")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    entity = response.getEntity(TimelineEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("test id", entity.getEntityId());
    Assert.assertEquals("test type", entity.getEntityType());
  }

}
