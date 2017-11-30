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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.api.records.timeline.TimelineAbout;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TestFileSystemTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

public class TestTimelineReaderWebServices {

  private static final String ROOT_DIR = new File("target",
      TestTimelineReaderWebServices.class.getSimpleName()).getAbsolutePath();

  private int serverPort;
  private TimelineReaderServer server;

  @BeforeClass
  public static void setup() throws Exception {
    TestFileSystemTimelineReaderImpl.initializeDataDirectory(ROOT_DIR);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @Before
  public void init() throws Exception {
    try {
      Configuration config = new YarnConfiguration();
      config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      config.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
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
      Assert.fail("Web server failed to start");
    }
  }

  @After
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
      Status expectedStatus) {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertNotNull(resp);
    assertEquals(resp.getStatusInfo().getStatusCode(),
        expectedStatus.getStatusCode());
  }

  private static Client createClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    return new Client(new URLConnectionClientHandler(
        new DummyURLConnectionFactory()), cfg);
  }

  private static ClientResponse getResponse(Client client, URI uri)
      throws Exception {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() !=
            ClientResponse.Status.OK.getStatusCode()) {
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
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
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
    Client client = createClient();
    try {
      ClientResponse resp = getResponse(client, uri);
      TimelineAbout about = resp.getEntity(TimelineAbout.class);
      Assert.assertNotNull(about);
      Assert.assertEquals("Timeline Reader API", about.getAbout());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityDefaultView() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals((Long)1425016502000L, entity.getCreatedTime());
      // Default view i.e. when no fields are specified, entity contains only
      // entity id, entity type and created time.
      assertEquals(0, entity.getConfigs().size());
      assertEquals(0, entity.getMetrics().size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityWithUserAndFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?" +
          "userid=user1&flowname=flow1&flowrunid=1");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals((Long)1425016502000L, entity.getCreatedTime());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityCustomFields() throws Exception {
    Client client = createClient();
    try {
      // Fields are case insensitive.
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?" +
          "fields=CONFIGS,Metrics,info");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals(3, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      assertTrue("UID should be present",
          entity.getInfo().containsKey(TimelineReaderUtils.UID_KEY));
      // Includes UID.
      assertEquals(3, entity.getInfo().size());
      // No events will be returned as events are not part of fields.
      assertEquals(0, entity.getEvents().size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityAllFields() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_1?" +
          "fields=ALL");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());
      assertEquals(3, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      assertTrue("UID should be present",
          entity.getInfo().containsKey(TimelineReaderUtils.UID_KEY));
      // Includes UID.
      assertEquals(3, entity.getInfo().size());
      assertEquals(2, entity.getEvents().size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app/id_10");
      verifyHttpResponse(client, uri, Status.NOT_FOUND);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testQueryWithoutCluster() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entities/app/id_1");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("id_1", entity.getId());
      assertEquals("app", entity.getType());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/apps/app1/entities/app");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(4, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntities() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(4, entities.size());
      assertTrue("Entities id_1, id_2, id_3 and id_4 should have been" +
          " present in response",
          entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_2")) &&
          entities.contains(newEntity("app", "id_3")) &&
          entities.contains(newEntity("app", "id_4")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesWithLimit() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?limit=2");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      // Entities returned are based on most recent created time.
      assertTrue("Entities with id_1 and id_4 should have been present " +
          "in response based on entity created time.",
          entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_4")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?limit=3");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      // Even though 2 entities out of 4 have same created time, one entity
      // is left out due to limit
      assertEquals(3, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesBasedOnCreatedTime() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "createdtimestart=1425016502030&createdtimeend=1425016502060");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_4 should have been present in response.",
          entities.contains(newEntity("app", "id_4")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?createdtimeend" +
          "=1425016502010");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(3, entities.size());
      assertFalse("Entity with id_4 should not have been present in response.",
          entities.contains(newEntity("app", "id_4")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?createdtimestart=" +
          "1425016502010");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_4 should have been present in response.",
          entities.contains(newEntity("app", "id_4")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByRelations() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?relatesto=" +
          "flow:flow1");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_1 should have been present in response.",
          entities.contains(newEntity("app", "id_1")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?isrelatedto=" +
          "type1:tid1_2,type2:tid2_1%60");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_1 should have been present in response.",
          entities.contains(newEntity("app", "id_1")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?isrelatedto=" +
          "type1:tid1_1:tid1_2,type2:tid2_1%60");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_1 should have been present in response.",
          entities.contains(newEntity("app", "id_1")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByConfigFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "conffilters=config_1%20eq%20123%20AND%20config_3%20eq%20abc");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_3 should have been present in response.",
          entities.contains(newEntity("app", "id_3")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByInfoFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "infofilters=info2%20eq%203.5");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_3 should have been present in response.",
          entities.contains(newEntity("app", "id_3")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByMetricFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "metricfilters=metric3%20ge%200");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      assertTrue("Entities with id_1 and id_2 should have been present" +
          " in response.",
          entities.contains(newEntity("app", "id_1")) &&
          entities.contains(newEntity("app", "id_2")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByEventFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "eventfilters=event_2,event_4");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Entity with id_3 should have been present in response.",
          entities.contains(newEntity("app", "id_3")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesNoMatch() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?" +
          "metricfilters=metric7%20ge%200&isrelatedto=type1:tid1_1:tid1_2,"+
          "type2:tid2_1%60&relatesto=flow:flow1&eventfilters=event_2,event_4" +
          "&infofilters=info2%20eq%203.5&createdtimestart=1425016502030&" +
          "createdtimeend=1425016502060");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testInvalidValuesHandling() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/app1/entities/app?flowrunid=a23b");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app/id_1?flowrunid=2ab15");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/apps/app1/entities/app?limit=#$561av");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetAppAttempts() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "entities/YARN_APPLICATION_ATTEMPT");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>() {
          });
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      int totalEntities = entities.size();
      assertEquals(2, totalEntities);
      assertTrue(
          "Entity with app-attempt-2 should have been present in response.",
          entities.contains(
              newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
                  "app-attempt-1")));
      assertTrue(
          "Entity with app-attempt-2 should have been present in response.",
          entities.contains(
              newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
                  "app-attempt-2")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/appattempts");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>() {
      });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      int retrievedEntity = entities.size();
      assertEquals(2, retrievedEntity);
      assertTrue(
          "Entity with app-attempt-2 should have been present in response.",
          entities.contains(
              newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
                  "app-attempt-1")));
      assertTrue(
          "Entity with app-attempt-2 should have been present in response.",
          entities.contains(
              newEntity(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
                  "app-attempt-2")));

      assertEquals(totalEntities, retrievedEntity);

    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetAppAttempt() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/entities/"
          + "YARN_APPLICATION_ATTEMPT/app-attempt-1");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entities1 =
          resp.getEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities1);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/appattempts/app-attempt-1");
      resp = getResponse(client, uri);
      TimelineEntity entities2 =
          resp.getEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities2);

      assertEquals(entities1, entities2);

    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetContainers() throws Exception {
    Client client = createClient();
    try {
      // total 3 containers in a application.
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/entities/YARN_CONTAINER");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>() {
          });
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities);
      int totalEntities = entities.size();
      assertEquals(3, totalEntities);
      assertTrue(
          "Entity with container_1_1 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_1_1")));
      assertTrue(
          "Entity with container_2_1 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_1")));
      assertTrue(
          "Entity with container_2_2 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_2")));

      // for app-attempt1 1 container has run
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "appattempts/app-attempt-1/containers");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>() {
      });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      int retrievedEntity = entities.size();
      assertEquals(1, retrievedEntity);
      assertTrue(
          "Entity with container_1_1 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_1_1")));

      // for app-attempt2 2 containers has run
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "appattempts/app-attempt-2/containers");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>() {
      });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      retrievedEntity += entities.size();
      assertEquals(2, entities.size());
      assertTrue(
          "Entity with container_2_1 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_1")));
      assertTrue(
          "Entity with container_2_2 should have been present in response.",
          entities.contains(newEntity(
              TimelineEntityType.YARN_CONTAINER.toString(), "container_2_2")));

      assertEquals(totalEntities, retrievedEntity);

    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetContainer() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/"
          + "entities/YARN_CONTAINER/container_2_2");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entities1 =
          resp.getEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
          resp.getType().toString());
      assertNotNull(entities1);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/app1/containers/container_2_2");
      resp = getResponse(client, uri);
      TimelineEntity entities2 =
          resp.getEntity(new GenericType<TimelineEntity>() {
          });
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities2);

      assertEquals(entities1, entities2);

    } finally {
      client.destroy();
    }
  }
}