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

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvents;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse.TimelinePutError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AdminACLsManager;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.timeline.TestMemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineAuthenticationFilter;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.JerseyTestBase;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.WebAppDescriptor;

public class TestTimelineWebServices extends JerseyTestBase {

  private static TimelineStore store;
  private static TimelineACLsManager timelineACLsManager;
  private static AdminACLsManager adminACLsManager;
  private static long beforeTime;

  private static class WebServletModule extends ServletModule {
    @SuppressWarnings("unchecked")
    @Override
    protected void configureServlets() {
      bind(YarnJacksonJaxbJsonProvider.class);
      bind(TimelineWebServices.class);
      bind(GenericExceptionHandler.class);
      try {
        store = mockTimelineStore();
      } catch (Exception e) {
        Assert.fail();
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
      bind(TimelineDataManager.class).toInstance(timelineDataManager);
      serve("/*").with(GuiceContainer.class);
      TimelineAuthenticationFilter taFilter =
          new TimelineAuthenticationFilter();
      FilterConfig filterConfig = mock(FilterConfig.class);
      when(filterConfig.getInitParameter(AuthenticationFilter.CONFIG_PREFIX))
          .thenReturn(null);
      when(filterConfig.getInitParameter(AuthenticationFilter.AUTH_TYPE))
          .thenReturn("simple");
      when(filterConfig.getInitParameter(
          PseudoAuthenticationHandler.ANONYMOUS_ALLOWED)).thenReturn("true");
      ServletContext context = mock(ServletContext.class);
      when(filterConfig.getServletContext()).thenReturn(context);
      Enumeration<String> names = mock(Enumeration.class);
      when(names.hasMoreElements()).thenReturn(true, true, true, false);
      when(names.nextElement()).thenReturn(
          AuthenticationFilter.AUTH_TYPE,
          PseudoAuthenticationHandler.ANONYMOUS_ALLOWED,
          DelegationTokenAuthenticationHandler.TOKEN_KIND);
      when(filterConfig.getInitParameterNames()).thenReturn(names);
      when(filterConfig.getInitParameter(
          DelegationTokenAuthenticationHandler.TOKEN_KIND)).thenReturn(
          TimelineDelegationTokenIdentifier.KIND_NAME.toString());
      try {
        taFilter.init(filterConfig);
      } catch (ServletException e) {
        Assert.fail("Unable to initialize TimelineAuthenticationFilter: " +
            e.getMessage());
      }

      taFilter = spy(taFilter);
      try {
        doNothing().when(taFilter).init(any(FilterConfig.class));
      } catch (ServletException e) {
        Assert.fail("Unable to initialize TimelineAuthenticationFilter: " +
            e.getMessage());
      }
      filter("/*").through(taFilter);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
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
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.server.applicationhistoryservice.webapp")
        .contextListenerClass(GuiceServletConfig.class)
        .filterClass(com.google.inject.servlet.GuiceFilter.class)
        .contextPath("jersey-guice-filter")
        .servletPath("/")
        .clientConfig(
            new DefaultClientConfig(YarnJacksonJaxbJsonProvider.class))
        .build());
  }

  @Test
  public void testAbout() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    TimelineAbout actualAbout = response.getEntity(TimelineAbout.class);
    TimelineAbout expectedAbout =
        TimelineUtils.createTimelineAbout("Timeline API");
    Assert.assertNotNull(
        "Timeline service about response is null", actualAbout);
    Assert.assertEquals(expectedAbout.getAbout(), actualAbout.getAbout());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersion(),
        actualAbout.getTimelineServiceVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceBuildVersion(),
        actualAbout.getTimelineServiceBuildVersion());
    Assert.assertEquals(expectedAbout.getTimelineServiceVersionBuiltOn(),
        actualAbout.getTimelineServiceVersionBuiltOn());
    Assert.assertEquals(expectedAbout.getHadoopVersion(),
        actualAbout.getHadoopVersion());
    Assert.assertEquals(expectedAbout.getHadoopBuildVersion(),
        actualAbout.getHadoopBuildVersion());
    Assert.assertEquals(expectedAbout.getHadoopVersionBuiltOn(),
        actualAbout.getHadoopVersionBuiltOn());
  }

  private static void verifyEntities(TimelineEntities entities) {
    Assert.assertNotNull(entities);
    Assert.assertEquals(3, entities.getEntities().size());
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
    TimelineEntity entity3 = entities.getEntities().get(2);
    Assert.assertNotNull(entity2);
    Assert.assertEquals("id_6", entity3.getEntityId());
    Assert.assertEquals("type_1", entity3.getEntityType());
    Assert.assertEquals(61l, entity3.getStartTime().longValue());
    Assert.assertEquals(0, entity3.getEvents().size());
    Assert.assertEquals(4, entity3.getPrimaryFilters().size());
    Assert.assertEquals(4, entity3.getOtherInfo().size());
  }

  @Test
  public void testGetEntities() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testFromId() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_2")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(2, response.getEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromId", "id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(3, response.getEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testFromTs() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(beforeTime))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(0, response.getEntity(TimelineEntities.class).getEntities()
        .size());

    response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("fromTs", Long.toString(
            System.currentTimeMillis()))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertEquals(3, response.getEntity(TimelineEntities.class).getEntities()
        .size());
  }

  @Test
  public void testPrimaryFilterString() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter", "user:username")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testPrimaryFilterLong() {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").queryParam("primaryFilter",
            "long:" + Long.toString((long) Integer.MAX_VALUE + 1l))
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    verifyEntities(response.getEntity(TimelineEntities.class));
  }

  @Test
  public void testGetEntity() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("type_1").path("id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
  public void testPostEntitiesWithPrimaryFilter() throws Exception {
    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    Map<String, Set<Object>> filters = new HashMap<String, Set<Object>>();
    filters.put(TimelineStore.SystemFilter.ENTITY_OWNER.toString(),
        new HashSet<Object>());
    entity.setPrimaryFilters(filters);
    entity.setEntityId("test id 6");
    entity.setEntityType("test type 6");
    entity.setStartTime(System.currentTimeMillis());
    entities.addEntity(entity);
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
    TimelinePutResponse putResposne =
        response.getEntity(TimelinePutResponse.class);
    Assert.assertEquals(0, putResposne.getErrors().size());
  }

  @Test
  public void testPostEntities() throws Exception {
    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setEntityId("test id 1");
    entity.setEntityType("test type 1");
    entity.setStartTime(System.currentTimeMillis());
    entity.setDomainId("domain_id_1");
    entities.addEntity(entity);
    WebResource r = resource();
    // No owner, will be rejected
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());

    response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .post(ClientResponse.class, entities);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    TimelinePutResponse putResposne =
        response.getEntity(TimelinePutResponse.class);
    Assert.assertNotNull(putResposne);
    Assert.assertEquals(0, putResposne.getErrors().size());
    // verify the entity exists in the store
    response = r.path("ws").path("v1").path("timeline")
        .path("test type 1").path("test id 1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    entity = response.getEntity(TimelineEntity.class);
    Assert.assertNotNull(entity);
    Assert.assertEquals("test id 1", entity.getEntityId());
    Assert.assertEquals("test type 1", entity.getEntityType());
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
    WebResource r = resource();
    // One of the entities has no id or type. HTTP 400 will be returned
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .queryParam("user.name", "tester").accept(MediaType.APPLICATION_JSON)
         .type(MediaType.APPLICATION_JSON).post(ClientResponse.class, entities);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelinePutResponse putResponse =
          response.getEntity(TimelinePutResponse.class);
      Assert.assertNotNull(putResponse);
      Assert.assertEquals(0, putResponse.getErrors().size());

      // override/append timeline data in the same entity with different user
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      putResponse = response.getEntity(TimelinePutResponse.class);
      Assert.assertNotNull(putResponse);
      Assert.assertEquals(1, putResponse.getErrors().size());
      Assert.assertEquals(TimelinePutResponse.TimelinePutError.ACCESS_DENIED,
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
      r = resource();
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      putResponse = response.getEntity(TimelinePutResponse.class);
      Assert.assertNotNull(putResponse);
      Assert.assertEquals(1, putResponse.getErrors().size());
      Assert.assertEquals(TimelinePutError.FORBIDDEN_RELATION,
          putResponse.getErrors().get(0).getErrorCode());

      // Make sure the entity has been added anyway even though the
      // relationship is been excluded
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 2").path("test id 3")
          .queryParam("user.name", "reader_user_3")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entity = response.getEntity(TimelineEntity.class);
      Assert.assertNotNull(entity);
      Assert.assertEquals("test id 3", entity.getEntityId());
      Assert.assertEquals("test type 2", entity.getEntityType());
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "anybody_1")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelinePutResponse putResposne =
          response.getEntity(TimelinePutResponse.class);
      Assert.assertNotNull(putResposne);
      Assert.assertEquals(0, putResposne.getErrors().size());
      // verify the entity exists in the store
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 7").path("test id 7")
          .queryParam("user.name", "any_body_2")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entity = response.getEntity(TimelineEntity.class);
      Assert.assertNotNull(entity);
      Assert.assertEquals("test id 7", entity.getEntityId());
      Assert.assertEquals("test type 7", entity.getEntityType());
      Assert.assertEquals(TimelineDataManager.DEFAULT_DOMAIN_ID,
          entity.getDomainId());
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelinePutResponse putResponse =
          response.getEntity(TimelinePutResponse.class);
      Assert.assertEquals(0, putResponse.getErrors().size());
      // verify the system data will not be exposed
      // 1. No field specification
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("user.name", "reader_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entity = response.getEntity(TimelineEntity.class);
      Assert.assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));
      // 2. other field
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("fields", "relatedentities")
          .queryParam("user.name", "reader_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entity = response.getEntity(TimelineEntity.class);
      Assert.assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));
      // 3. primaryfilters field
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("fields", "primaryfilters")
          .queryParam("user.name", "reader_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entity = response.getEntity(TimelineEntity.class);
      Assert.assertNull(entity.getPrimaryFilters().get(
          TimelineStore.SystemFilter.ENTITY_OWNER.toString()));

      // get entity with other user
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 3").path("test id 3")
          .queryParam("user.name", "reader_user_2")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelinePutResponse putResponse =
          response.getEntity(TimelinePutResponse.class);
      Assert.assertEquals(0, putResponse.getErrors().size());

      // Put entity [4, 5] in domain 2
      entities = new TimelineEntities();
      entity = new TimelineEntity();
      entity.setEntityId("test id 5");
      entity.setEntityType("test type 4");
      entity.setStartTime(System.currentTimeMillis());
      entity.setDomainId("domain_id_2");
      entities.addEntity(entity);
      r = resource();
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      putResponse = response.getEntity(TimelinePutResponse.class);
      Assert.assertEquals(0, putResponse.getErrors().size());

      // Query entities of type 4
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "reader_user_1")
          .path("test type 4")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      entities = response.getEntity(TimelineEntities.class);
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_1")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelinePutResponse putResponse =
          response.getEntity(TimelinePutResponse.class);
      Assert.assertEquals(0, putResponse.getErrors().size());

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
      r = resource();
      response = r.path("ws").path("v1").path("timeline")
          .queryParam("user.name", "writer_user_3")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .post(ClientResponse.class, entities);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      putResponse = response.getEntity(TimelinePutResponse.class);
      Assert.assertEquals(0, putResponse.getErrors().size());

      // Query events belonging to the entities of type 4
      response = r.path("ws").path("v1").path("timeline")
          .path("test type 5").path("events")
          .queryParam("user.name", "reader_user_1")
          .queryParam("entityId", "test id 5,test id 6")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelineEvents events = response.getEntity(TimelineEvents.class);
      // Reader 1 should just have the access to the events of entity [5, 5]
      assertEquals(1, events.getAllEvents().size());
      assertEquals("test id 5", events.getAllEvents().get(0).getEntityId());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetDomain() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("domain_id_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    TimelineDomain domain = response.getEntity(TimelineDomain.class);
    verifyDomain(domain, "domain_id_1");
  }

  @Test
  public void testGetDomainYarnACLsEnabled() {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .path("domain").path("domain_id_1")
          .queryParam("user.name", "owner_1")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelineDomain domain = response.getEntity(TimelineDomain.class);
      verifyDomain(domain, "domain_id_1");

      response = r.path("ws").path("v1").path("timeline")
          .path("domain").path("domain_id_1")
          .queryParam("user.name", "tester")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testGetDomains() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("timeline")
        .path("domain")
        .queryParam("owner", "owner_1")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    TimelineDomains domains = response.getEntity(TimelineDomains.class);
    Assert.assertEquals(2, domains.getDomains().size());
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
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1").path("timeline")
          .path("domain")
          .queryParam("user.name", "owner_1")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      TimelineDomains domains = response.getEntity(TimelineDomains.class);
      Assert.assertEquals(2, domains.getDomains().size());
      for (int i = 0; i < domains.getDomains().size(); ++i) {
        verifyDomain(domains.getDomains().get(i),
            i == 0 ? "domain_id_4" : "domain_id_1");
      }

      response = r.path("ws").path("v1").path("timeline")
          .path("domain")
          .queryParam("owner", "owner_1")
          .queryParam("user.name", "tester")
          .accept(MediaType.APPLICATION_JSON)
          .get(ClientResponse.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          response.getType().toString());
      domains = response.getEntity(TimelineDomains.class);
      Assert.assertEquals(0, domains.getDomains().size());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  @Test
  public void testPutDomain() throws Exception {
    TimelineDomain domain = new TimelineDomain();
    domain.setId("test_domain_id");
    WebResource r = resource();
    // No owner, will be rejected
    ClientResponse response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .put(ClientResponse.class, domain);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());

    response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .queryParam("user.name", "tester")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .put(ClientResponse.class, domain);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());

    // Verify the domain exists
    response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("test_domain_id")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    domain = response.getEntity(TimelineDomain.class);
    Assert.assertNotNull(domain);
    Assert.assertEquals("test_domain_id", domain.getId());
    Assert.assertEquals("tester", domain.getOwner());
    Assert.assertEquals(null, domain.getDescription());

    // Update the domain
    domain.setDescription("test_description");
    response = r.path("ws").path("v1")
        .path("timeline").path("domain")
        .queryParam("user.name", "tester")
        .accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON)
        .put(ClientResponse.class, domain);
    assertResponseStatusCode(Status.OK, response.getStatusInfo());

    // Verify the domain is updated
    response = r.path("ws").path("v1").path("timeline")
        .path("domain").path("test_domain_id")
        .accept(MediaType.APPLICATION_JSON)
        .get(ClientResponse.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getType().toString());
    domain = response.getEntity(TimelineDomain.class);
    Assert.assertNotNull(domain);
    Assert.assertEquals("test_domain_id", domain.getId());
    Assert.assertEquals("test_description", domain.getDescription());
  }

  @Test
  public void testPutDomainYarnACLsEnabled() throws Exception {
    AdminACLsManager oldAdminACLsManager =
        timelineACLsManager.setAdminACLsManager(adminACLsManager);
    try {
      TimelineDomain domain = new TimelineDomain();
      domain.setId("test_domain_id_acl");
      WebResource r = resource();
      ClientResponse response = r.path("ws").path("v1")
          .path("timeline").path("domain")
          .queryParam("user.name", "tester")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .put(ClientResponse.class, domain);
      assertResponseStatusCode(Status.OK, response.getStatusInfo());

      // Update the domain by another user
      response = r.path("ws").path("v1")
          .path("timeline").path("domain")
          .queryParam("user.name", "other")
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .put(ClientResponse.class, domain);
      assertResponseStatusCode(Status.FORBIDDEN, response.getStatusInfo());
    } finally {
      timelineACLsManager.setAdminACLsManager(oldAdminACLsManager);
    }
  }

  private static void verifyDomain(TimelineDomain domain, String domainId) {
    Assert.assertNotNull(domain);
    Assert.assertEquals(domainId, domain.getId());
    // The specific values have been verified in TestMemoryTimelineStore
    Assert.assertNotNull(domain.getDescription());
    Assert.assertNotNull(domain.getOwner());
    Assert.assertNotNull(domain.getReaders());
    Assert.assertNotNull(domain.getWriters());
    Assert.assertNotNull(domain.getCreatedTime());
    Assert.assertNotNull(domain.getModifiedTime());
  }
}
