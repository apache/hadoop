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

package org.apache.hadoop.yarn.server.timelineservice.reader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Set;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timeline.TimelineHealth;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TestFileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestTimelineReaderWebServices {

  private static final String ROOT_DIR = new File("target",
      TestTimelineReaderWebServices.class.getSimpleName()).getAbsolutePath();

  private int serverPort;
  private TimelineReaderServer server;

  @BeforeAll
  public static void setup() throws Exception {
    TestFileSystemTimelineReaderImpl.initializeDataDirectory(ROOT_DIR);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @BeforeEach
  public void init() throws Exception {
    try {
      Configuration config = new YarnConfiguration();
      config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      config.set(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
          "localhost:0");
      config.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
      config.setClass(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
          FileSystemTimelineReaderImpl.class, TimelineReader.class);
      config.set(FileSystemTimelineReaderImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          ROOT_DIR);
      server = new TimelineReaderServer();
      server.init(config);
      server.start();
      serverPort = server.getWebServerPort();
    } catch (Exception e) {
      fail("Web server failed to start");
    }
  }

  @AfterEach
  public void stop() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  private static TimelineEntity newEntity(String type, String id) {
    TimelineEntity entity = new TimelineEntity();
    entity.setIdentifier(new TimelineEntity.Identifier(type, id));
    return entity;
  }

  private static void verifyHttpResponse(Client client, URI uri,
      Response.Status expectedStatus) {
    Response resp = client.target(uri).request(MediaType.APPLICATION_JSON).get(Response.class);
    assertNotNull(resp);
    assertEquals(resp.getStatusInfo().getStatusCode(),
        expectedStatus.getStatusCode());
  }

  private static Client createClient() {
    ClientConfig cfg = new ClientConfig();
    cfg.register(YarnJacksonJaxbJsonProvider.class);
    cfg.connectorProvider(
        new HttpUrlConnectorProvider().connectionFactory(new DummyURLConnectionFactory()));
    return ClientBuilder.newClient(cfg);
  }

  private static Response getResponse(Client client, URI uri)
      throws Exception {
    Response resp = client.target(uri).request(MediaType.APPLICATION_JSON)
        .get(Response.class);
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() != Response.Status.OK.getStatusCode()) {
      String msg = new String();
      if (resp != null) {
        msg = String.valueOf(resp.getStatusInfo().getStatusCode());
      }
      throw new IOException("Incorrect response from timeline reader. " +
          "Status=" + msg);
    }
    return resp;
  }

  private static class DummyURLConnectionFactory
      implements HttpUrlConnectorProvider.ConnectionFactory {

    @Override
    public HttpURLConnection getConnection(final URL url)
        throws IOException {
      try {
        return (HttpURLConnection)url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  @Test
  public void testAbout() throws Exception {
    URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/");
    Client client = createClient()
        .register(TimelineAboutReader.class);
    try {
      Response resp = getResponse(client, uri);
      TimelineAbout about = resp.readEntity(TimelineAbout.class);
      assertNotNull(about);
      assertEquals("Timeline Reader API", about.getAbout());
    } finally {
      client.close();
    }
  }

  @Test
  public void testGetEntityDefaultView() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1");
      Response resp = getResponse(client, uri);
      TimelineEntity entity = resp.readEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals((Long) 1425016502000L, entity.getCreatedTime());
      // Default view i.e. when no fields are specified, entity contains only
      // entity id, entity type and created time.
      assertEquals(0, entity.getConfigs().size());
      assertEquals(0, entity.getMetrics().size());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntityWithUserAndFlowInfo() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?" +
          "userid=user1&flowname=flow1&flowrunid=1");
      Response resp = getResponse(client, uri);
      TimelineEntity entity = resp.readEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals((Long) 1425016502000L, entity.getCreatedTime());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntityCustomFields() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      // Fields are case insensitive.
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?" +
          "fields=CONFIGS,Metrics,info");
      Response resp = getResponse(client, uri);
      TimelineEntity entity = resp.readEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals(3, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      assertTrue(entity.getInfo().containsKey(TimelineReaderUtils.UID_KEY),
          "UID should be present");
      // Includes UID.
      assertEquals(3, entity.getInfo().size());
      // No events will be returned as events are not part of fields.
      assertEquals(0, entity.getEvents().size());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntityAllFields() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?fields=ALL");
      Response resp = getResponse(client, uri);
      TimelineEntity entity = resp.readEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals(3, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      assertTrue(entity.getInfo().containsKey(TimelineReaderUtils.UID_KEY),
          "UID should be present");
      // Includes UID.
      assertEquals(3, entity.getInfo().size());
      assertEquals(2, entity.getEvents().size());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntityNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_10");
      verifyHttpResponse(client, uri, Response.Status.NOT_FOUND);
    } finally {
      client.close();
    }
  }

  @Test
  void testQueryWithoutCluster() throws Exception {
    Client client = createClient().
        register(TimelineEntityReader.class).
        register(TimelineEntitySetReader.class);

    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entities/app/id_1");
      Response resp = getResponse(client, uri);
      TimelineEntity entity = resp.readEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entities/app");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(4, entities.size());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntities() throws Exception {
    Client client = createClient().
        register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(4, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_2")) &&
          entities.contains(newEntity("app", "id_3")) &&
          entities.contains(newEntity("app", "id_4")),
          "Entities id_1, id_2, id_3 and id_4 should have been" +
              " present in response");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesWithLimit() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?limit=2");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.readEntity(new GenericType<Set<TimelineEntity>>(){
          });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      // Entities returned are based on most recent created time.
      assertTrue(entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_4")),
          "Entities with id_1 and id_4 should have been present " +
              "in response based on entity created time.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?limit=3");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){
      });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      // Even though 2 entities out of 4 have same created time, one entity
      // is left out due to limit
      assertEquals(3, entities.size());
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesBasedOnCreatedTime() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "createdtimestart=1425016502030&createdtimeend=1425016502060");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.readEntity(new GenericType<Set<TimelineEntity>>(){
          });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_4")),
          "Entity with id_4 should have been present in response.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?createdtimeend" +
          "=1425016502010");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){
      });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(3, entities.size());
      assertFalse(entities.contains(newEntity("app", "id_4")),
          "Entity with id_4 should not have been present in response.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?createdtimestart=" +
          "1425016502010");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){
      });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_4")),
          "Entity with id_4 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesByRelations() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?relatesto=" +
          "flow:flow1");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.readEntity(new GenericType<Set<TimelineEntity>>(){
          });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_1")),
          "Entity with id_1 should have been present in response.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?isrelatedto=" +
          "type1:tid1_2,type2:tid2_1%60");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){
      });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_1")),
          "Entity with id_1 should have been present in response.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?isrelatedto=" +
          "type1:tid1_1:tid1_2,type2:tid2_1%60");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){
      });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_1")),
          "Entity with id_1 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesByConfigFilters() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "conffilters=config_1%20eq%20123%20AND%20config_3%20eq%20abc");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_3")),
          "Entity with id_3 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesByInfoFilters() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "infofilters=info2%20eq%203.5");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_3")),
          "Entity with id_3 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesByMetricFilters() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "metricfilters=metric3%20ge%200");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_2")),
          "Entities with id_1 and id_2 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesByEventFilters() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "eventfilters=event_2,event_4");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue(entities.contains(newEntity("app", "id_3")),
          "Entity with id_3 should have been present in response.");
    } finally {
      client.close();
    }
  }

  @Test
  void testGetEntitiesNoMatch() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "metricfilters=metric7%20ge%200&isrelatedto=type1:tid1_1:tid1_2," +
          "type2:tid2_1%60&relatesto=flow:flow1&eventfilters=event_2,event_4" +
          "&infofilters=info2%20eq%203.5&createdtimestart=1425016502030&" +
          "createdtimeend=1425016502060");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.close();
    }
  }

  @Test
  void testInvalidValuesHandling() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?flowrunid=a23b");
      verifyHttpResponse(client, uri, Response.Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app/id_1?flowrunid=2ab15");
      verifyHttpResponse(client, uri, Response.Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?limit=#$561av");
      verifyHttpResponse(client, uri, Response.Status.BAD_REQUEST);
    } finally {
      client.close();
    }
  }

  @Test
  void testGetAppAttempts() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "entities/YARN_APPLICATION_ATTEMPT");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>() {});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      int totalEntities = entities.size();
      assertEquals(2, totalEntities);
      assertTrue(entities.contains(
          newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), "app-attempt-1")),
          "Entity with app-attempt-2 should have been present in response.");
      assertTrue(entities.contains(
          newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), "app-attempt-2")),
          "Entity with app-attempt-2 should have been present in response.");

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/appattempts");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>() {
      });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
      assertNotNull(entities);
      int retrievedEntity = entities.size();
      assertEquals(2, retrievedEntity);
      assertTrue(entities.contains(
          newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), "app-attempt-1")),
          "Entity with app-attempt-2 should have been present in response.");
      assertTrue(entities.contains(
          newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(), "app-attempt-2")),
          "Entity with app-attempt-2 should have been present in response.");
      assertEquals(totalEntities, retrievedEntity);
    } finally {
      client.close();
    }
  }

  @Test
  void testGetAppAttempt() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/entities/"
          + "YARN_APPLICATION_ATTEMPT/app-attempt-1");
      Response resp = getResponse(client, uri);
      TimelineEntity entities1 = resp.readEntity(new GenericType<TimelineEntity>() {});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities1);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/appattempts/app-attempt-1");
      resp = getResponse(client, uri);
      TimelineEntity entities2 = resp.readEntity(new GenericType<TimelineEntity>() {});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
      assertNotNull(entities2);

      assertEquals(entities1, entities2);

    } finally {
      client.close();
    }
  }

  @Test
  void testGetContainers() throws Exception {
    Client client = createClient().register(TimelineEntitySetReader.class);
    try {
      // total 3 containers in a application.
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/entities/YARN_CONTAINER");
      Response resp = getResponse(client, uri);
      Set<TimelineEntity> entities = resp.readEntity(new GenericType<Set<TimelineEntity>>() {});
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities);
      int totalEntities = entities.size();
      assertEquals(3, totalEntities);
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_1_1")),
          "Entity with container_1_1 should have been present in response.");
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_1")),
          "Entity with container_2_1 should have been present in response.");
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_2")),
          "Entity with container_2_2 should have been present in response.");

      // for app-attempt1 1 container has run
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "appattempts/app-attempt-1/containers");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>() {});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
      assertNotNull(entities);
      int retrievedEntity = entities.size();
      assertEquals(1, retrievedEntity);
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_1_1")),
          "Entity with container_1_1 should have been present in response.");

      // for app-attempt2 2 containers has run
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "appattempts/app-attempt-2/containers");
      resp = getResponse(client, uri);
      entities = resp.readEntity(new GenericType<Set<TimelineEntity>>() {
      });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
      assertNotNull(entities);
      retrievedEntity += entities.size();
      assertEquals(2, entities.size());
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_1")),
          "Entity with container_2_1 should have been present in response.");
      assertTrue(
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_2")),
          "Entity with container_2_2 should have been present in response.");

      assertEquals(totalEntities, retrievedEntity);

    } finally {
      client.close();
    }
  }

  @Test
  void testGetContainer() throws Exception {
    Client client = createClient().register(TimelineEntityReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "entities/YARN_CONTAINER/container_2_2");
      Response resp = getResponse(client, uri);
      TimelineEntity entities1 =
          resp.readEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON + ";" + JettyUtils.UTF_8,
          resp.getMediaType().toString());
      assertNotNull(entities1);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/containers/container_2_2");
      resp = getResponse(client, uri);
      TimelineEntity entities2 =
          resp.readEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
      assertNotNull(entities2);

      assertEquals(entities1, entities2);

    } finally {
      client.close();
    }
  }

  @Test
  void testHealthCheck() throws Exception {
    Client client = createClient().register(TimelineHealthReader.class);
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/health");
      Response resp = getResponse(client, uri);
      TimelineHealth timelineHealth =
          resp.readEntity(new GenericType<TimelineHealth>() {
          });
      assertEquals(200, resp.getStatus());
      assertEquals(TimelineHealth.TimelineHealthStatus.RUNNING,
          timelineHealth.getHealthStatus());
    } finally {
      client.close();
    }
  }
}