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

import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntities;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEntity;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvent;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSEvents;
import org.apache.hadoop.yarn.api.records.apptimeline.ATSPutErrors;
import org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline.ApplicationTimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.apptimeline.TestMemoryApplicationTimelineStore;
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


public class TestATSWebServices extends JerseyTest {

  private static ApplicationTimelineStore store;

  private Injector injector = Guice.createInjector(new ServletModule() {

    @Override
    protected void configureServlets() {
      bind(YarnJacksonJaxbJsonProvider.class);
      bind(ATSWebServices.class);
      bind(GenericExceptionHandler.class);
      try{
        store = mockApplicationTimelineStore();
      } catch (Exception e) {
        Assert.fail();
      }
      bind(ApplicationTimelineStore.class).toInstance(store);
      serve("/*").with(GuiceContainer.class);
    }

  });

  public class GuiceServletConfig extends GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

  private ApplicationTimelineStore mockApplicationTimelineStore()
      throws Exception {
    TestMemoryApplicationTimelineStore store =
        new TestMemoryApplicationTimelineStore();
    store.setup();
    return store.getApplicationTimelineStore();
  }

  public TestATSWebServices() {
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
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSWebServices.AboutInfo about =
        response.getEntity(ATSWebServices.AboutInfo.class);
    Assert.assertNotNull(about);
    Assert.assertEquals("Application Timeline API", about.getAbout());
  }

  @Test
  public void testGetEntities() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .path("type_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSEntities entities = response.getEntity(ATSEntities.class);
    Assert.assertNotNull(entities);
    Assert.assertEquals(2, entities.getEntities().size());
    ATSEntity entity1 = entities.getEntities().get(0);
    Assert.assertNotNull(entity1);
    Assert.assertEquals("id_1", entity1.getEntityId());
    Assert.assertEquals("type_1", entity1.getEntityType());
    Assert.assertEquals(123l, entity1.getStartTime().longValue());
    Assert.assertEquals(2, entity1.getEvents().size());
    Assert.assertEquals(2, entity1.getPrimaryFilters().size());
    Assert.assertEquals(4, entity1.getOtherInfo().size());
    ATSEntity entity2 = entities.getEntities().get(1);
    Assert.assertNotNull(entity2);
    Assert.assertEquals("id_2", entity2.getEntityId());
    Assert.assertEquals("type_1", entity2.getEntityType());
    Assert.assertEquals(123l, entity2.getStartTime().longValue());
    Assert.assertEquals(2, entity2.getEvents().size());
    Assert.assertEquals(2, entity2.getPrimaryFilters().size());
    Assert.assertEquals(4, entity2.getOtherInfo().size());
  }

  @Test
  public void testGetEntity() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .path("type_1").path("id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSEntity entity = response.getEntity(ATSEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("id_1", entity.getEntityId());
    Assert.assertEquals("type_1", entity.getEntityType());
    Assert.assertEquals(123l, entity.getStartTime().longValue());
    Assert.assertEquals(2, entity.getEvents().size());
    Assert.assertEquals(2, entity.getPrimaryFilters().size());
    Assert.assertEquals(4, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEntityFields1() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .path("type_1").path("id_1").queryParam("fields", "events,otherinfo")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSEntity entity = response.getEntity(ATSEntity.class);
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
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .path("type_1").path("id_1").queryParam("fields", "lasteventonly," +
            "primaryfilters,relatedentities")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSEntity entity = response.getEntity(ATSEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("id_1", entity.getEntityId());
    Assert.assertEquals("type_1", entity.getEntityType());
    Assert.assertEquals(123l, entity.getStartTime().longValue());
    Assert.assertEquals(1, entity.getEvents().size());
    Assert.assertEquals(2, entity.getPrimaryFilters().size());
    Assert.assertEquals(0, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEvents() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .path("type_1").path("events")
        .queryParam("entityId", "id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSEvents events = response.getEntity(ATSEvents.class);
    Assert.assertNotNull(events);
    Assert.assertEquals(1, events.getAllEvents().size());
    ATSEvents.ATSEventsOfOneEntity partEvents = events.getAllEvents().get(0);
    Assert.assertEquals(2, partEvents.getEvents().size());
    ATSEvent event1 = partEvents.getEvents().get(0);
    Assert.assertEquals(456l, event1.getTimestamp());
    Assert.assertEquals("end_event", event1.getEventType());
    Assert.assertEquals(1, event1.getEventInfo().size());
    ATSEvent event2 = partEvents.getEvents().get(1);
    Assert.assertEquals(123l, event2.getTimestamp());
    Assert.assertEquals("start_event", event2.getEventType());
    Assert.assertEquals(0, event2.getEventInfo().size());
  }

  @Test
  public void testPostEntities() throws Exception {
    ATSEntities entities = new ATSEntities();
    ATSEntity entity = new ATSEntity();
    entity.setEntityId("test id");
    entity.setEntityType("test type");
    entity.setStartTime(System.currentTimeMillis());
    entities.addEntity(entity);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("apptimeline")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ATSPutErrors errors = response.getEntity(ATSPutErrors.class);
    Assert.assertNotNull(errors);
    Assert.assertEquals(0, errors.getErrors().size());
    // verify the entity exists in the store
    response = r.path("ws").path("v1").path("apptimeline")
        .path("test type").path("test id")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    entity = response.getEntity(ATSEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("test id", entity.getEntityId());
    Assert.assertEquals("test type", entity.getEntityType());
  }

}
