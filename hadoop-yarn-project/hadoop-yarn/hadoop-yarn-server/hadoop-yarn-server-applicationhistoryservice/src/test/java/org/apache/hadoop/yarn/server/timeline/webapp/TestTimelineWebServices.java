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

package org.apache.hadoop.yarn.server.timeline.webapp;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.yarn.api.records.writer.TimelineEntitiesWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelineDomainWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelineEntityWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelineDomainsWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelinePutResponseWriter;
import org.apache.hadoop.yarn.api.records.writer.TimelineEventsWriter;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineAboutReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineDomainReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineDomainsReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineEntitiesReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineEntityReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelineEventsReader;
import org.apache.hadoop.yarn.server.timeline.reader.TimelinePutResponseReader;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jettison.JettisonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.ContextFactory;
import org.apache.hadoop.yarn.server.timeline.TestMemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilter;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestTimelineWebServices extends JerseyTestBase {

  private static TimelineStore store;
  private static TimelineACLsManager timelineACLsManager;
  private static AdminACLsManager adminACLsManager;
  private static long beforeTime;

  @Override
  protected Application configure() {
    ResourceConfig config = new ResourceConfig();
    config.register(new JerseyBinder());
    config.register(TimelineWebServices.class);
    config.register(TimelineEntitiesReader.class);
    config.register(TimelineEntitiesWriter.class);
    config.register(TimelineEntityWriter.class);
    config.register(TimelineDomainReader.class);
    config.register(TimelineDomainsWriter.class);
    config.register(TimelineDomainWriter.class);
    config.register(TimelineEventsWriter.class);
    config.register(GenericExceptionHandler.class);
    config.register(TimelinePutResponseWriter.class);
    config.register(new JettisonFeature()).register(YarnJacksonJaxbJsonProvider.class);
    forceSet(TestProperties.CONTAINER_PORT, JERSEY_RANDOM_PORT);
    return config;
  }

  private static HttpServletRequest request;

  private static class JerseyBinder extends AbstractBinder {

    @Override
    protected void configure() {

      try {
        store = mockTimelineStore();
      } catch (Exception e) {
        fail();
      }

      Configuration conf = new YarnConfiguration();
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, false);
      timelineACLsManager = new TimelineACLsManager(conf);
      timelineACLsManager.setTimelineStore(store);
      conf.setBoolean(YarnConfiguration.YARN_ACL_ENABLE, true);
      conf.set(YarnConfiguration.YARN_ADMIN_ACL, "admin");
      adminACLsManager = new AdminACLsManager(conf);
      TimelineDataManager timelineDataManager =
          new TimelineDataManager(store, timelineACLsManager);
      timelineDataManager.init(conf);
      timelineDataManager.start();

      bind(timelineDataManager).to(TimelineDataManager.class);

      TimelineAuthenticationFilter taFilter = new TimelineAuthenticationFilter();
      FilterConfig filterConfig = mock(FilterConfig.class);
      when(filterConfig.getInitParameter(AuthenticationFilter.CONFIG_PREFIX)).thenReturn(null);
      when(filterConfig.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
      when(filterConfig.getInitParameter(
           PseudoAuthenticationHandler.ANONYMOUS_ALLOWED)).thenReturn("true");
      ServletContext context = mock(ServletContext.class);
      when(filterConfig.getServletContext()).thenReturn(context);
      Enumeration<String> names = mock(Enumeration.class);
      when(names.hasMoreElements()).thenReturn(true, true, true, false);
      when(names.nextElement()).thenReturn(AuthenticationFilter.AUTH_TYPE,
          PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
          DelegationTokenAuthenticationHandler.TOKEN_KIND);
      when(filterConfig.getInitParameterNames()).thenReturn(names);
      when(filterConfig.getInitParameter(DelegationTokenAuthenticationHandler.TOKEN_KIND))
          .thenReturn(TimelineDelegationTokenIdentifier.KIND_NAME.toString());

      try {
        taFilter.init(filterConfig);
      } catch (ServletException e) {
        fail("Unable to initialize TimelineAuthenticationFilter: " + e.getMessage());
      }

      taFilter = spy(taFilter);
      try {
        doNothing().when(taFilter).init(any(FilterConfig.class));
      } catch (ServletException e) {
        fail("Unable to initialize TimelineAuthenticationFilter: " + e.getMessage());
      }

      request = mock(HttpServletRequest.class);
      final HttpServletResponse response = mock(HttpServletResponse.class);
      bind(request).to(HttpServletRequest.class);
      bind(response).to(HttpServletResponse.class);
    }
  }

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
  }

  private static TimelineStore mockTimelineStore()
      throws Exception {
    beforeTime = System.currentTimeMillis() - 1;
    TestMemoryTimelineStore store =
        new TestMemoryTimelineStore();
    store.setup();
    return store.getTimelineStore();
  }

  public TestTimelineWebServices() {
  }

  @Test
  void testAbout() {
    WebTarget target = target().register(TimelineAboutReader.class);
    Response response = target.path("ws").path("v1").path("timeline")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineAbout actualAbout = response.readEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Timeline API");
    assertNotNull(
        actualAbout, "Timeline service about response is null");
    assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  private static void verifyEntities(TimelineEntities entities) {
    assertNotNull(entities);
    assertEquals(3, entities.getEntities().size());
    TimelineEntity entity1 = entities.getEntities().get(0);
    assertNotNull(entity1);
    assertEquals("id_1", entity1.getEntityId());
    assertEquals("type_1", entity1.getEntityType());
    assertEquals(123L, entity1.getStartTime().longValue());
    assertEquals(2, entity1.getEvents().size());
    assertEquals(4, entity1.getPrimaryFilters().size());
    assertEquals(4, entity1.getOtherInfo().size());
    TimelineEntity entity2 = entities.getEntities().get(1);
    assertNotNull(entity2);
    assertEquals("id_2", entity2.getEntityId());
    assertEquals("type_1", entity2.getEntityType());
    assertEquals(123L, entity2.getStartTime().longValue());
    assertEquals(2, entity2.getEvents().size());
    assertEquals(4, entity2.getPrimaryFilters().size());
    assertEquals(4, entity2.getOtherInfo().size());
    TimelineEntity entity3 = entities.getEntities().get(2);
    assertNotNull(entity2);
    assertEquals("id_6", entity3.getEntityId());
    assertEquals("type_1", entity3.getEntityType());
    assertEquals(61L, entity3.getStartTime().longValue());
    assertEquals(0, entity3.getEvents().size());
    assertEquals(4, entity3.getPrimaryFilters().size());
    assertEquals(4, entity3.getOtherInfo().size());
  }

  @Test
  void testGetEntities() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    verifyEntities(response.readEntity(TimelineEntities.class));
  }

  @Test
  void testFromId() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_2")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(2, response.readEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(3, response.readEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testFromTs() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(beforeTime))
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(0, response.readEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(
        System.currentTimeMillis()))
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertEquals(3, response.readEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testPrimaryFilterString() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter", "user:username")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    verifyEntities(response.readEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterInteger() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter",
        "appname:" + Integer.MAX_VALUE)
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    verifyEntities(response.readEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterLong() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter",
        "long:" + ((long) Integer.MAX_VALUE + 1L))
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    verifyEntities(response.readEntity(TimelineEntities.class));
  }

  @Test
  public void testSecondaryFilters() {
    WebTarget r = target().register(TimelineEntitiesReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1")
        .queryParam("secondaryFilter",
            "user:username,appname:" + Integer.MAX_VALUE)
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    verifyEntities(response.readEntity(TimelineEntities.class));
  }

  @Test
  public void testGetEntity() {
    WebTarget r = target().register(TimelineEntityReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineEntity entity = response.readEntity(TimelineEntity.class);
    assertNotNull(entity);
    assertEquals("id_1", entity.getEntityId());
    assertEquals("type_1", entity.getEntityType());
    assertEquals(123L, entity.getStartTime().longValue());
    assertEquals(2, entity.getEvents().size());
    assertEquals(4, entity.getPrimaryFilters().size());
    assertEquals(4, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEntityFields1() {
    WebTarget r = target().register(TimelineEntityReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1").queryParam("fields", "events,otherinfo")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineEntity entity = response.readEntity(TimelineEntity.class);
    assertNotNull(entity);
    assertEquals("id_1", entity.getEntityId());
    assertEquals("type_1", entity.getEntityType());
    assertEquals(123L, entity.getStartTime().longValue());
    assertEquals(2, entity.getEvents().size());
    assertEquals(0, entity.getPrimaryFilters().size());
    assertEquals(4, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEntityFields2() {
    WebTarget r = target().register(TimelineEntityReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1").queryParam("fields", "lasteventonly," +
        "primaryfilters,relatedentities")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineEntity entity = response.readEntity(TimelineEntity.class);
    assertNotNull(entity);
    assertEquals("id_1", entity.getEntityId());
    assertEquals("type_1", entity.getEntityType());
    assertEquals(123L, entity.getStartTime().longValue());
    assertEquals(1, entity.getEvents().size());
    assertEquals(4, entity.getPrimaryFilters().size());
    assertEquals(0, entity.getOtherInfo().size());
  }

  @Test
  public void testGetEvents() {
    WebTarget r = target().register(TimelineEventsReader.class);

    Response response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("events")
        .queryParam("entityId", "id_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);

    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    TimelineEvents events = response.readEntity(TimelineEvents.class);
    assertNotNull(events);
    assertEquals(1, events.getAllEvents().size());
    TimelineEvents.EventsOfOneEntity partEvents = events.getAllEvents().get(0);
    assertEquals(2, partEvents.getEvents().size());
    TimelineEvent event1 = partEvents.getEvents().get(0);
    assertEquals(456L, event1.getTimestamp());
    assertEquals("end_event", event1.getEventType());
    assertEquals(1, event1.getEventInfo().size());
    TimelineEvent event2 = partEvents.getEvents().get(1);
    assertEquals(123L, event2.getTimestamp());
    assertEquals("start_event", event2.getEventType());
    assertEquals(0, event2.getEventInfo().size());
  }

  @Test
  public void testPostEntitiesWithPrimaryFilter() {
    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    Map<String, Set<Object>> filters = new HashMap<>();
    filters.put(TimelineStore.SystemFilter.ENTITY_OWNER.toString(), new HashSet<>());
    entity.setPrimaryFilters(filters);
    entity.setEntityId("test id 6");
    entity.setEntityType("test type 6");
    entity.setStartTime(System.currentTimeMillis());
    entities.addEntity(entity);

    WebTarget r = target()
        .register(TimelineDomainsReader.class)
        .register(TimelineEntitiesWriter.class)
        .register(TimelineDomainReader.class)
        .register(TimelineEntityReader.class)
        .register(TimelinePutResponseReader.class)
        .register(TimelineEntitiesReader.class);

    when(request.getRemoteUser()).thenReturn("tester");
    Response response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(entities), Response.class);

    TimelinePutResponse putResponse =
        response.readEntity(TimelinePutResponse.class);
    assertEquals(0, putResponse.getErrors().size());
  }

  @Test
  public void testPostEntities() {

    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("test id 1");
    entity.setEntityType("test type 1");
    entity.setStartTime(System.currentTimeMillis());
    entity.setDomainId("domain_id_1");
    entities.addEntity(entity);

    WebTarget r = target()
        .register(TimelineDomainsReader.class)
        .register(TimelineEntitiesWriter.class)
        .register(TimelineDomainReader.class)
        .register(TimelineEntityReader.class)
        .register(TimelinePutResponseReader.class)
        .register(TimelineEntitiesReader.class);

    // No owner, will be rejected
    Response response = r.path("ws").path("v1").path("timeline")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(entities), Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());

    when(request.getRemoteUser()).thenReturn("tester");
    response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester")
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.json(entities), Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());

    TimelinePutResponse putResponse =
        response.readEntity(TimelinePutResponse.class);
    assertNotNull(putResponse);
    assertEquals(0, putResponse.getErrors().size());

    // verify the entity exists in the store
    response = r.path("ws").path("v1").path("timeline")
        .path("test type 1").path("test id 1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    entity = response.readEntity(TimelineEntity.class);
    assertNotNull(entity);
    assertEquals("test id 1", entity.getEntityId());
    assertEquals("test type 1", entity.getEntityType());
  }

  @Test
  public void testPostIncompleteEntities() throws Exception {
    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    entity1.setEntityId("test id 1");
    entity1.setEntityType("test type 1");
    entity1.setStartTime(System.currentTimeMillis());
    entity1.setDomainId("domain_id_1");
    entities.addEntity(entity1);
    // Add an entity with no id or type.
    entities.addEntity(new TimelineEntity());

    WebTarget r = target()
        .register(TimelineDomainsReader.class)
        .register(TimelineEntitiesWriter.class)
        .register(TimelineDomainReader.class)
        .register(TimelineEntityReader.class)
        .register(TimelinePutResponseReader.class)
        .register(TimelineEntitiesReader.class);

    // One of the entities has no id or type. HTTP 400 will be returned
    when(request.getRemoteUser()).thenReturn("tester");
    Response response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester").request(MediaType.APPLICATION_JSON)
        .post(Entity.json(entities), Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertResponseStatusCode(Status.BAD_REQUEST, response.getStatusInfo());
  }

  @Test
  public void testPostEntitiesWithYarnACLsEnabled() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);

    try {

      TimelineEntities entities = new TimelineEntities();
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("test id 2");
      entity.setEntityType("test type 2");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_1");
      entities.addEntity(entity);

      WebTarget r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelinePutResponse putResponse =
          response.readEntity(TimelinePutResponse.class);
      assertNotNull(putResponse);
      assertEquals(0, putResponse.getErrors().size());

      // override/append timeline data in the same entity with different user
      when(request.getRemoteUser()).thenReturn("writer_user_3");
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      putResponse = response.readEntity(TimelinePutResponse.class);
      assertNotNull(putResponse);
      assertEquals(1, putResponse.getErrors().size());
      assertEquals(TimelinePutError.ACCESS_DENIED,
          putResponse.getErrors().get(0).getErrorCode());

      // Cross domain relationship will be rejected
      entities = new TimelineEntities();
      entity = new TimelineEntity();
      entity.setEntityId("test id 3");
      entity.setEntityType("test type 2");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_2");
      entity.setRelatedEntities(Collections.singletonMap(
          "test type 2", Collections.singleton("test id 2")));
      entities.addEntity(entity);

      r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_3");
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      putResponse = response.readEntity(TimelinePutResponse.class);
      assertNotNull(putResponse);
      assertEquals(1, putResponse.getErrors().size());
      assertEquals(TimelinePutError.FORBIDDEN_RELATION,
          putResponse.getErrors().get(0).getErrorCode());

      // Make sure the entity has been added anyway even though the
      // relationship is been excluded
      when(request.getRemoteUser()).thenReturn("reader_user_3");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 2").path("test id 3")
          .queryParam("user.name", "reader_user_3")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("test id 3", entity.getEntityId());
      assertEquals("test type 2", entity.getEntityType());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testPostEntitiesToDefaultDomain() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      TimelineEntities entities = new TimelineEntities();
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("test id 7");
      entity.setEntityType("test type 7");
      entity.setStartTime(System.currentTimeMillis());
      entities.addEntity(entity);

      WebTarget r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("anybody_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "anybody_1")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelinePutResponse putResponse = response.readEntity(TimelinePutResponse.class);
      assertNotNull(putResponse);
      assertEquals(0, putResponse.getErrors().size());

      // verify the entity exists in the store
      when(request.getRemoteUser()).thenReturn("any_body_2");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 7").path("test id 7")
          .queryParam("user.name", "any_body_2")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("test id 7", entity.getEntityId());
      assertEquals("test type 7", entity.getEntityType());
      assertEquals(TimelineDataManager.DEFAULT_DOMAIN_ID, entity.getDomainId());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetEntityWithYarnACLsEnabled() throws Exception {

    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);

    try {
      TimelineEntities entities = new TimelineEntities();
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("test id 3");
      entity.setEntityType("test type 3");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_1");
      entities.addEntity(entity);

      WebTarget r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());

      TimelinePutResponse putResponse =
          response.readEntity(TimelinePutResponse.class);
      assertEquals(0, putResponse.getErrors().size());

      // verify the system data will not be exposed
      // 1. No field specification
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("user.name", "reader_user_1")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(TimelineEntity.class);
      assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));

      // 2. other field
      when(request.getRemoteUser()).thenReturn("reader_user_1");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("fields", "relatedentities")
          .queryParam("user.name", "reader_user_1")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(TimelineEntity.class);
      assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));

      // 3. primary filters field
      when(request.getRemoteUser()).thenReturn("reader_user_1");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("fields", "primaryfilters")
          .queryParam("user.name", "reader_user_1")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entity = response.readEntity(TimelineEntity.class);
      assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));

      // get entity with other user
      when(request.getRemoteUser()).thenReturn("reader_user_2");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("user.name", "reader_user_2")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());

    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetEntitiesWithYarnACLsEnabled() {

    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);

    try {

      // Put entity [4, 4] in domain 1
      TimelineEntities entities = new TimelineEntities();
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("test id 4");
      entity.setEntityType("test type 4");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_1");
      entities.addEntity(entity);

      WebTarget r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelinePutResponse putResponse =
          response.readEntity(TimelinePutResponse.class);
      assertEquals(0, putResponse.getErrors().size());

      // Put entity [4, 5] in domain 2
      entities = new TimelineEntities();
      entity = new TimelineEntity();
      entity.setEntityId("test id 5");
      entity.setEntityType("test type 4");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_2");
      entities.addEntity(entity);
      r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_3");
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      putResponse = response.readEntity(TimelinePutResponse.class);
      assertEquals(0, putResponse.getErrors().size());

      // Query entities of type 4
      when(request.getRemoteUser()).thenReturn("reader_user_1");
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "reader_user_1")
          .path("test type 4")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      entities = response.readEntity(TimelineEntities.class);

      // Reader 1 should just have the access to entity [4, 4]
      assertEquals(1, entities.getEntities().size());
      assertEquals("test type 4", entities.getEntities().get(0).getEntityType());
      assertEquals("test id 4", entities.getEntities().get(0).getEntityId());

    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetEventsWithYarnACLsEnabled() {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      // Put entity [5, 5] in domain 1
      TimelineEntities entities = new TimelineEntities();
      TimelineEntity entity = new TimelineEntity();
      entity.setEntityId("test id 5");
      entity.setEntityType("test type 5");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_1");
      TimelineEvent event = new TimelineEvent();
      event.setEventType("event type 1");
      event.setTimestamp(System.currentTimeMillis());
      entity.addEvent(event);
      entities.addEntity(entity);

      WebTarget r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class)
          .register(TimelineEventsReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelinePutResponse putResponse =
          response.readEntity(TimelinePutResponse.class);
      assertEquals(0, putResponse.getErrors().size());

      // Put entity [5, 6] in domain 2
      entities = new TimelineEntities();
      entity = new TimelineEntity();
      entity.setEntityId("test id 6");
      entity.setEntityType("test type 5");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_2");
      event = new TimelineEvent();
      event.setEventType("event type 2");
      event.setTimestamp(System.currentTimeMillis());
      entity.addEvent(event);
      entities.addEntity(entity);

      r = target()
          .register(TimelineDomainsReader.class)
          .register(TimelineEntitiesWriter.class)
          .register(TimelineDomainReader.class)
          .register(TimelineEntityReader.class)
          .register(TimelinePutResponseReader.class)
          .register(TimelineEntitiesReader.class)
          .register(TimelineEventsReader.class);

      when(request.getRemoteUser()).thenReturn("writer_user_3");
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .request(MediaType.APPLICATION_JSON)
          .post(Entity.json(entities), Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      putResponse = response.readEntity(TimelinePutResponse.class);
      assertEquals(0, putResponse.getErrors().size());

      // Query events belonging to the entities of type 4
      when(request.getRemoteUser()).thenReturn("reader_user_1");
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 5").path("events")
          .queryParam("user.name", "reader_user_1")
          .queryParam("entityId", "test id 5,test id 6")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());

      TimelineEvents events = response.readEntity(TimelineEvents.class);
      // Reader 1 should just have the access to the events of entity [5, 5]
      assertEquals(1, events.getAllEvents().size());
      assertEquals("test id 5", events.getAllEvents().get(0).getEntityId());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetDomain() throws Exception {
    WebTarget r = target().register(TimelineDomainReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("domain_id_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineDomain domain = response.readEntity(TimelineDomain.class);
    verifyDomain(domain, "domain_id_1");
  }

  @Test
  public void testGetDomainYarnACLsEnabled() {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      WebTarget r = target().register(TimelineDomainReader.class);

      when(request.getRemoteUser()).thenReturn("owner_1");
      Response response = r.path("ws").path("v1").path("timeline")
          .path("domain").path("domain_id_1")
          .queryParam("user.name", "owner_1")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelineDomain domain = response.readEntity(TimelineDomain.class);
      verifyDomain(domain, "domain_id_1");

      when(request.getRemoteUser()).thenReturn("tester");
      response = r.path("ws").path("v1").path("timeline")
          .path("domain").path("domain_id_1")
          .queryParam("user.name", "tester")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetDomains() throws Exception {
    WebTarget r = target().register(TimelineDomainsReader.class);
    Response response = r.path("ws").path("v1").path("timeline")
        .path("domain")
        .queryParam("owner", "owner_1")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    TimelineDomains domains = response.readEntity(TimelineDomains.class);
    assertEquals(2, domains.getDomains().size());
    for (int i = 0; i < domains.getDomains().size(); ++i) {
      verifyDomain(domains.getDomains().get(i),
          i == 0 ? "domain_id_4" : "domain_id_1");
    }
  }

  @Test
  public void testGetDomainsYarnACLsEnabled() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      when(request.getRemoteUser()).thenReturn("owner_1");
      WebTarget r = target().register(TimelineDomainsReader.class);
      Response response = r.path("ws").path("v1").path("timeline")
          .path("domain")
          .queryParam("user.name", "owner_1")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      TimelineDomains domains = response.readEntity(TimelineDomains.class);
      assertEquals(2, domains.getDomains().size());
      for (int i = 0; i < domains.getDomains().size(); ++i) {
        verifyDomain(domains.getDomains().get(i),
            i == 0 ? "domain_id_4" : "domain_id_1");
      }

      when(request.getRemoteUser()).thenReturn("testerw");
      response = r.path("ws").path("v1").path("timeline")
          .path("domain")
          .queryParam("owner", "owner_1")
          .queryParam("user.name", "tester")
          .request(MediaType.APPLICATION_JSON)
          .get(Response.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          response.getMediaType().toString());
      domains = response.readEntity(TimelineDomains.class);
      assertEquals(0, domains.getDomains().size());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testPutDomain() throws Exception {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("test_domain_id");

    WebTarget r = target()
        .register(TimelineDomainReader.class)
        .register(TimelineDomainWriter.class);

    // No owner, will be rejected
    Response response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.json(domain), Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());

    when(request.getRemoteUser()).thenReturn("tester");
    response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .queryParam("user.name", "tester")
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.json(domain), Response.class);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());

    // Verify the domain exists
    response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("test_domain_id")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    domain = response.readEntity(TimelineDomain.class);
    assertNotNull(domain);
    assertEquals("test_domain_id", domain.getId());
    assertEquals("tester", domain.getOwner());
    assertNull(domain.getDescription());

    // Update the domain
    domain.setDescription("test_description");
    response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .queryParam("user.name", "tester")
        .request(MediaType.APPLICATION_JSON)
        .put(Entity.json(domain), Response.class);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());

    // Verify the domain is updated
    response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("test_domain_id")
        .request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
        response.getMediaType().toString());
    domain = response.readEntity(TimelineDomain.class);
    assertNotNull(domain);
    assertEquals("test_domain_id", domain.getId());
    assertEquals("test_description", domain.getDescription());
  }

  @Test
  public void testPutDomainYarnACLsEnabled() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      TimelineDomain domain = new TimelineDomain();
      domain.setId("test_domain_id_acl");

      WebTarget r = target()
          .register(TimelineDomainReader.class)
          .register(TimelineDomainWriter.class);

      when(request.getRemoteUser()).thenReturn("tester");
      Response response = r.path("ws").path("v1")
          .path("timeline").path("domain")
          .queryParam("user.name", "tester")
          .request(MediaType.APPLICATION_JSON)
          .put(Entity.json(domain), Response.class);
      assertResponseStatusCode(Status.OK, response.getStatusInfo());

      // Update the domain by another user
      when(request.getRemoteUser()).thenReturn("other");
      response = r.path("ws").path("v1")
          .path("timeline").path("domain")
          .queryParam("user.name", "other")
          .request(MediaType.APPLICATION_JSON)
          .put(Entity.json(domain), Response.class);
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testContextFactory() throws Exception {
    JAXBContext jaxbContext1 = ContextFactory.createContext(
        new Class[]{TimelineDomain.class}, Collections.EMPTY_MAP);
    JAXBContext jaxbContext2 = ContextFactory.createContext(
        new Class[]{TimelineDomain.class}, Collections.EMPTY_MAP);
    assertEquals(jaxbContext1, jaxbContext2);

    try {
      ContextFactory.createContext(new Class[]{TimelineEntity.class},
          Collections.EMPTY_MAP);
      fail("Expected JAXBException");
    } catch (Exception e) {
      assertThat(e).isExactlyInstanceOf(JAXBException.class);
    }
  }

  private static void verifyDomain(TimelineDomain domain, String domainId) {
    assertNotNull(domain);
    assertEquals(domainId, domain.getId());
    // The specific values have been verified in TestMemoryTimelineStore
    assertNotNull(domain.getDescription());
    assertNotNull(domain.getOwner());
    assertNotNull(domain.getReaders());
    assertNotNull(domain.getWriters());
    assertNotNull(domain.getCreatedTime());
    assertNotNull(domain.getModifiedTime());
  }
}
