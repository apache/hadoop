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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineSchemaCreator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
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

public class TestTimelineReaderWebServicesHBaseStorage {
  private int serverPort;
  private TimelineReaderServer server;
  private static HBaseTestingUtility util;
  private static long ts = System.currentTimeMillis();
  private static long dayTs =
      TimelineStorageUtils.getTopOfTheDayTimestamp(ts);

  @BeforeClass
  public static void setup() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
    TimelineSchemaCreator.createAllTables(util.getConfiguration(), false);
    loadData();
  }

  private static void loadData() throws Exception {
    String cluster = "cluster1";
    String user = "user1";
    String flow = "flow_name";
    String flowVersion = "CF7022C10F1354";
    Long runid = 1002345678919L;
    Long runid1 = 1002345678920L;

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "application_1111111111_1111";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    Long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);
    entity.addConfig("cfg2", "value1");

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    metricValues.put(ts - 100000, 2);
    metricValues.put(ts - 80000, 40);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);

    m1 = new TimelineMetric();
    m1.setId("HDFS_BYTES_READ");
    metricValues = new HashMap<Long, Number>();
    metricValues.put(ts - 100000, 31);
    metricValues.put(ts - 80000, 57);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity.addMetrics(metrics);

    TimelineEvent event = new TimelineEvent();
    event.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event.setTimestamp(cTime);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    TimelineEvent event11 = new TimelineEvent();
    event11.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
    Long expTs = 1425019501000L;
    event11.setTimestamp(expTs);
    entity.addEvent(event11);

    te.addEntity(entity);

    // write another application with same metric to this flow
    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    id = "application_1111111111_2222";
    type = TimelineEntityType.YARN_APPLICATION.toString();
    entity1.setId(id);
    entity1.setType(type);
    cTime = 1425016501000L;
    entity1.setCreatedTime(cTime);
    entity1.addConfig("cfg1", "value1");
    // add metrics
    metrics.clear();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP_SLOT_MILLIS");
    metricValues = new HashMap<Long, Number>();
    metricValues.put(ts - 100000, 5L);
    metricValues.put(ts - 80000, 101L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);
    entity1.addMetrics(metrics);
    TimelineEvent event1 = new TimelineEvent();
    event1.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event1.setTimestamp(cTime);
    event1.addInfo(expKey, expVal);
    entity1.addEvent(event1);
    te1.addEntity(entity1);

    String flow2 = "flow_name2";
    String flowVersion2 = "CF7022C10F1454";
    Long runid2 = 2102356789046L;
    TimelineEntities te3 = new TimelineEntities();
    TimelineEntity entity3 = new TimelineEntity();
    id = "application_11111111111111_2223";
    entity3.setId(id);
    entity3.setType(type);
    cTime = 1425016501037L;
    entity3.setCreatedTime(cTime);
    TimelineEvent event2 = new TimelineEvent();
    event2.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event2.setTimestamp(cTime);
    event2.addInfo("foo_event", "test");
    entity3.addEvent(event2);
    te3.addEntity(entity3);

    TimelineEntities te4 = new TimelineEntities();
    TimelineEntity entity4 = new TimelineEntity();
    id = "application_1111111111_2224";
    entity4.setId(id);
    entity4.setType(type);
    cTime = 1425016501034L;
    entity4.setCreatedTime(cTime);
    TimelineEvent event4 = new TimelineEvent();
    event4.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event4.setTimestamp(cTime);
    event4.addInfo("foo_event", "test");
    entity4.addEvent(event4);
    te4.addEntity(entity4);

    TimelineEntities te5 = new TimelineEntities();
    TimelineEntity entity5 = new TimelineEntity();
    entity5.setId("entity1");
    entity5.setType("type1");
    entity5.setCreatedTime(1425016501034L);
    te5.addEntity(entity5);
    TimelineEntity entity6 = new TimelineEntity();
    entity6.setId("entity2");
    entity6.setType("type1");
    entity6.setCreatedTime(1425016501034L);
    te5.addEntity(entity6);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.write(cluster, user, flow, flowVersion, runid, entity.getId(), te);
      hbi.write(cluster, user, flow, flowVersion, runid, entity1.getId(), te1);
      hbi.write(cluster, user, flow, flowVersion, runid1, entity4.getId(), te4);
      hbi.write(cluster, user, flow2,
          flowVersion2, runid2, entity3.getId(), te3);
      hbi.write(cluster, user, flow, flowVersion, runid,
          "application_1111111111_1111", te5);
      hbi.flush();
    } finally {
      hbi.close();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void init() throws Exception {
    try {
      Configuration config = util.getConfiguration();
      config.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      config.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      config.set(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          "localhost:0");
      config.set(YarnConfiguration.RM_CLUSTER_ID, "cluster1");
      config.set(YarnConfiguration.TIMELINE_SERVICE_READER_CLASS,
          "org.apache.hadoop.yarn.server.timelineservice.storage." +
              "HBaseTimelineReaderImpl");
      config.setInt("hfile.format.version", 3);
      server = new TimelineReaderServer();
      server.init(config);
      server.start();
      serverPort = server.getWebServerPort();
    } catch (Exception e) {
      Assert.fail("Web server failed to start");
    }
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
        resp.getClientResponseStatus() != ClientResponse.Status.OK) {
      String msg = new String();
      if (resp != null) {
        msg = resp.getClientResponseStatus().toString();
      }
      throw new IOException("Incorrect response from timeline reader. " +
          "Status=" + msg);
    }
    return resp;
  }

  private static class DummyURLConnectionFactory
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url) throws IOException {
      try {
        return (HttpURLConnection)url.openConnection();
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  private static TimelineEntity newEntity(String type, String id) {
    TimelineEntity entity = new TimelineEntity();
    entity.setIdentifier(new TimelineEntity.Identifier(type, id));
    return entity;
  }

  private static TimelineMetric newMetric(TimelineMetric.Type type,
      String id, long ts, Number value) {
    TimelineMetric metric = new TimelineMetric(type);
    metric.setId(id);
    metric.addValue(ts, value);
    return metric;
  }

  private static boolean verifyMetricValues(Map<Long, Number> m1,
      Map<Long, Number> m2) {
    for (Map.Entry<Long, Number> entry : m1.entrySet()) {
      if (!m2.containsKey(entry.getKey())) {
        return false;
      }
      if (m2.get(entry.getKey()).equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static boolean verifyMetrics(
      TimelineMetric m, TimelineMetric... metrics) {
    for (TimelineMetric metric : metrics) {
      if (!metric.equals(m)) {
        continue;
      }
      if (!verifyMetricValues(metric.getValues(), m.getValues())) {
        continue;
      }
      return true;
    }
    return false;
  }

  private static void verifyHttpResponse(Client client, URI uri,
      Status status) {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
        .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertNotNull(resp);
    assertTrue("Response from server should have been " + status,
        resp.getClientResponseStatus().equals(status));
  }

  @Test
  public void testGetFlowRun() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919");
      ClientResponse resp = getResponse(client, uri);
      FlowRunEntity entity = resp.getEntity(FlowRunEntity.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 141L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/runs/1002345678919");
      resp = getResponse(client, uri);
      entity = resp.getEntity(FlowRunEntity.class);
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 141L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }
    } finally {
      client.destroy();
    }
  }


  @Test
  public void testGetFlowRuns() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowRunEntity> entities =
          resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            ((entity.getId().equals("user1@flow_name/1002345678919")) &&
            (entity.getRunId() == 1002345678919L) &&
            (entity.getStartTime() == 1425016501000L)) ||
            ((entity.getId().equals("user1@flow_name/1002345678920")) &&
            (entity.getRunId() == 1002345678920L) &&
            (entity.getStartTime() == 1425016501034L)));
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "clusters/cluster1/users/user1/flows/flow_name/runs?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            entity.getId().equals("user1@flow_name/1002345678920") &&
            entity.getRunId() == 1002345678920L &&
            entity.getStartTime() == 1425016501034L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimestart=1425016501030");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            entity.getId().equals("user1@flow_name/1002345678920") &&
            entity.getRunId() == 1002345678920L &&
            entity.getStartTime() == 1425016501034L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimestart=1425016500999&createdtimeend=1425016501035");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            ((entity.getId().equals("user1@flow_name/1002345678919")) &&
            (entity.getRunId() == 1002345678919L) &&
            (entity.getStartTime() == 1425016501000L)) ||
            ((entity.getId().equals("user1@flow_name/1002345678920")) &&
            (entity.getRunId() == 1002345678920L) &&
            (entity.getStartTime() == 1425016501034L)));
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimeend=1425016501030");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
             entity.getId().equals("user1@flow_name/1002345678919") &&
             entity.getRunId() == 1002345678919L &&
             entity.getStartTime() == 1425016501000L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "fields=metrics");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            ((entity.getId().equals("user1@flow_name/1002345678919")) &&
            (entity.getRunId() == 1002345678919L) &&
            (entity.getStartTime() == 1425016501000L) &&
            (entity.getMetrics().size() == 2)) ||
            ((entity.getId().equals("user1@flow_name/1002345678920")) &&
            (entity.getRunId() == 1002345678920L) &&
            (entity.getStartTime() == 1425016501034L) &&
            (entity.getMetrics().size() == 0)));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByUID() throws Exception {
    Client client = createClient();
    try {
      // Query all flows.
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flows");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowActivityEntity> flowEntities =
          resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(flowEntities);
      assertEquals(2, flowEntities.size());
      List<String> listFlowUIDs = new ArrayList<String>();
      for (FlowActivityEntity entity : flowEntities) {
        String flowUID =
            (String)entity.getInfo().get(TimelineReaderManager.UID_KEY);
        listFlowUIDs.add(flowUID);
        assertEquals(TimelineUIDConverter.FLOW_UID.encodeUID(
            new TimelineReaderContext(entity.getCluster(), entity.getUser(),
            entity.getFlowName(), null, null, null, null)), flowUID);
        assertTrue((entity.getId().endsWith("@flow_name") &&
            entity.getFlowRuns().size() == 2) ||
            (entity.getId().endsWith("@flow_name2") &&
            entity.getFlowRuns().size() == 1));
      }

      // Query flowruns based on UID returned in query above.
      List<String> listFlowRunUIDs = new ArrayList<String>();
      for (String flowUID : listFlowUIDs) {
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/flow-uid/" + flowUID + "/runs");
        resp = getResponse(client, uri);
        Set<FlowRunEntity> frEntities =
            resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
        assertNotNull(frEntities);
        for (FlowRunEntity entity : frEntities) {
          String flowRunUID =
              (String)entity.getInfo().get(TimelineReaderManager.UID_KEY);
          listFlowRunUIDs.add(flowRunUID);
          assertEquals(TimelineUIDConverter.FLOWRUN_UID.encodeUID(
              new TimelineReaderContext("cluster1", entity.getUser(),
              entity.getName(), entity.getRunId(), null, null, null)),
              flowRunUID);
        }
      }
      assertEquals(3, listFlowRunUIDs.size());

      // Query single flowrun based on UIDs' returned in query to get flowruns.
      for (String flowRunUID : listFlowRunUIDs) {
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/run-uid/" + flowRunUID);
        resp = getResponse(client, uri);
        FlowRunEntity entity = resp.getEntity(FlowRunEntity.class);
        assertNotNull(entity);
      }

      // Query apps based on UIDs' returned in query to get flowruns.
      List<String> listAppUIDs = new ArrayList<String>();
      for (String flowRunUID : listFlowRunUIDs) {
        TimelineReaderContext context =
            TimelineUIDConverter.FLOWRUN_UID.decodeUID(flowRunUID);
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/run-uid/" + flowRunUID + "/apps");
        resp = getResponse(client, uri);
        Set<TimelineEntity> appEntities =
            resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
        assertNotNull(appEntities);
        for (TimelineEntity entity : appEntities) {
          String appUID =
              (String)entity.getInfo().get(TimelineReaderManager.UID_KEY);
          listAppUIDs.add(appUID);
          assertEquals(TimelineUIDConverter.APPLICATION_UID.encodeUID(
              new TimelineReaderContext(context.getClusterId(),
              context.getUserId(), context.getFlowName(),
              context.getFlowRunId(), entity.getId(), null, null)), appUID);
        }
      }
      assertEquals(4, listAppUIDs.size());

      // Query single app based on UIDs' returned in query to get apps.
      for (String appUID : listAppUIDs) {
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/app-uid/" + appUID);
        resp = getResponse(client, uri);
        TimelineEntity entity = resp.getEntity(TimelineEntity.class);
        assertNotNull(entity);
      }

      // Query entities based on UIDs' returned in query to get apps and
      // a specific entity type(in this case type1).
      List<String> listEntityUIDs = new ArrayList<String>();
      for (String appUID : listAppUIDs) {
        TimelineReaderContext context =
            TimelineUIDConverter.APPLICATION_UID.decodeUID(appUID);
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/app-uid/" + appUID + "/entities/type1");
        resp = getResponse(client, uri);
        Set<TimelineEntity> entities =
            resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
        assertNotNull(entities);
        for (TimelineEntity entity : entities) {
          String entityUID =
              (String)entity.getInfo().get(TimelineReaderManager.UID_KEY);
          listEntityUIDs.add(entityUID);
          assertEquals(TimelineUIDConverter.GENERIC_ENTITY_UID.encodeUID(
              new TimelineReaderContext(context.getClusterId(),
              context.getUserId(), context.getFlowName(),
              context.getFlowRunId(), context.getAppId(), "type1",
              entity.getId())), entityUID);
        }
      }
      assertEquals(2, listEntityUIDs.size());

      // Query single entity based on UIDs' returned in query to get entities.
      for (String entityUID : listEntityUIDs) {
        uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
            "timeline/entity-uid/" + entityUID);
        resp = getResponse(client, uri);
        TimelineEntity entity = resp.getEntity(TimelineEntity.class);
        assertNotNull(entity);
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flow-uid/dummy:flow/runs");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/run-uid/dummy:flowrun");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      // Run Id is not a numerical value.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/run-uid/some:dummy:flow:123v456");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/run-uid/dummy:flowrun/apps");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/app-uid/dummy:app");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/app-uid/dummy:app/entities/type1");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/entity-uid/dummy:entity");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testUIDQueryWithAndWithoutFlowContextInfo() throws Exception {
    Client client = createClient();
    try {
      String appUIDWithFlowInfo =
          "cluster1!user1!flow_name!1002345678919!application_1111111111_1111";
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"+
          "timeline/app-uid/" + appUIDWithFlowInfo);
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity appEntity1 = resp.getEntity(TimelineEntity.class);
      assertNotNull(appEntity1);
      assertEquals(
          TimelineEntityType.YARN_APPLICATION.toString(), appEntity1.getType());
      assertEquals("application_1111111111_1111", appEntity1.getId());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "app-uid/" + appUIDWithFlowInfo + "/entities/type1");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities1 =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities1);
      assertEquals(2, entities1.size());
      for (TimelineEntity entity : entities1) {
        assertNotNull(entity.getInfo());
        assertEquals(1, entity.getInfo().size());
        String uid =
            (String) entity.getInfo().get(TimelineReaderManager.UID_KEY);
        assertNotNull(uid);
        assertTrue(uid.equals(appUIDWithFlowInfo + "!type1!entity1") ||
            uid.equals(appUIDWithFlowInfo + "!type1!entity2"));
      }

      String appUIDWithoutFlowInfo = "cluster1!application_1111111111_1111";
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/"+
          "app-uid/" + appUIDWithoutFlowInfo);
      resp = getResponse(client, uri);;
      TimelineEntity appEntity2 = resp.getEntity(TimelineEntity.class);
      assertNotNull(appEntity2);
      assertEquals(
          TimelineEntityType.YARN_APPLICATION.toString(), appEntity2.getType());
      assertEquals("application_1111111111_1111", appEntity2.getId());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/" +
          "app-uid/" + appUIDWithoutFlowInfo + "/entities/type1");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities2 =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities2);
      assertEquals(2, entities2.size());
      for (TimelineEntity entity : entities2) {
        assertNotNull(entity.getInfo());
        assertEquals(1, entity.getInfo().size());
        String uid =
            (String) entity.getInfo().get(TimelineReaderManager.UID_KEY);
        assertNotNull(uid);
        assertTrue(uid.equals(appUIDWithoutFlowInfo + "!type1!entity1") ||
            uid.equals(appUIDWithoutFlowInfo + "!type1!entity2"));
      }

      String entityUIDWithFlowInfo = appUIDWithFlowInfo + "!type1!entity1";
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/"+
          "entity-uid/" + entityUIDWithFlowInfo);
      resp = getResponse(client, uri);;
      TimelineEntity singleEntity1 = resp.getEntity(TimelineEntity.class);
      assertNotNull(singleEntity1);
      assertEquals("type1", singleEntity1.getType());
      assertEquals("entity1", singleEntity1.getId());

      String entityUIDWithoutFlowInfo =
          appUIDWithoutFlowInfo + "!type1!entity1";
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/timeline/"+
          "entity-uid/" + entityUIDWithoutFlowInfo);
      resp = getResponse(client, uri);;
      TimelineEntity singleEntity2 = resp.getEntity(TimelineEntity.class);
      assertNotNull(singleEntity2);
      assertEquals("type1", singleEntity2.getType());
      assertEquals("entity1", singleEntity2.getId());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testUIDNotProperlyEscaped() throws Exception {
    Client client = createClient();
    try {
      String appUID =
          "cluster1!user*1!flow_name!1002345678919!application_1111111111_1111";
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/"+
          "timeline/app-uid/" + appUID);
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlows() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowActivityEntity> entities =
          resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowActivityEntity entity : entities) {
        assertTrue((entity.getId().endsWith("@flow_name") &&
            entity.getFlowRuns().size() == 2) ||
            (entity.getId().endsWith("@flow_name2") &&
            entity.getFlowRuns().size() == 1));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flows/");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
              "timeline/clusters/cluster1/flows?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());

      long firstFlowActivity =
          TimelineStorageUtils.getTopOfTheDayTimestamp(1425016501000L);

      DateFormat fmt = TimelineReaderWebServices.DATE_FORMAT.get();
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange="
          + fmt.format(firstFlowActivity) + "-"
          + fmt.format(dayTs));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowActivityEntity entity : entities) {
        assertTrue((entity.getId().endsWith("@flow_name") &&
            entity.getFlowRuns().size() == 2) ||
            (entity.getId().endsWith("@flow_name2") &&
            entity.getFlowRuns().size() == 1));
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=" +
          fmt.format(dayTs + (4*86400000L)));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=-" +
          fmt.format(dayTs));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=" +
           fmt.format(firstFlowActivity) + "-");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150711:20150714");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150714-20150711");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=2015071129-20150712");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150711-2015071243");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetApp() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111?" +
          "userid=user1&fields=ALL&flowname=flow_name&flowrunid=1002345678919");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_1111", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.TIME_SERIES,
          "HDFS_BYTES_READ", ts - 100000, 31L);
      m1.addValue(ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.TIME_SERIES,
          "MAP_SLOT_MILLIS", ts - 100000, 2L);
      m2.addValue(ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
              "timeline/apps/application_1111111111_2222?userid=user1" +
              "&fields=metrics&flowname=flow_name&flowrunid=1002345678919");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_2222", entity.getId());
      assertEquals(1, entity.getMetrics().size());
      TimelineMetric m3 = newMetric(TimelineMetric.Type.TIME_SERIES,
         "MAP_SLOT_MILLIS", ts - 100000, 5L);
      m2.addValue(ts - 80000, 101L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m3));
      }
    } finally {
        client.destroy();
    }
  }

  @Test
  public void testGetAppWithoutFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111?" +
          "fields=ALL");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_1111", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.TIME_SERIES,
          "HDFS_BYTES_READ", ts - 100000, 31L);
      m1.addValue(ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.TIME_SERIES,
          "MAP_SLOT_MILLIS", ts - 100000, 2L);
      m2.addValue(ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityWithoutFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity1");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity1", entity.getId());
      assertEquals("type1", entity.getType());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesWithoutFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRunApps() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps?fields=ALL");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue("Unexpected app in result",
            (entity.getId().equals("application_1111111111_1111") &&
            entity.getMetrics().size() == 2) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getMetrics().size() == 1));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/runs/1002345678919/apps");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/runs/1002345678919/" +
          "apps?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowApps() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "fields=ALL");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue("Unexpected app in result",
            (entity.getId().equals("application_1111111111_1111") &&
            entity.getMetrics().size() == 2) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getMetrics().size() == 1) ||
            (entity.getId().equals("application_1111111111_2224") &&
            entity.getMetrics().size() == 0));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/apps");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/apps?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowAppsFilters() throws Exception {
    Client client = createClient();
    try {
      String entityType = TimelineEntityType.YARN_APPLICATION.toString();
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "eventfilters=" + ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Unexpected app in result", entities.contains(
          newEntity(entityType, "application_1111111111_1111")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "metricfilters=HDFS_BYTES_READ");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Unexpected app in result", entities.contains(
          newEntity(entityType, "application_1111111111_1111")));

      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "conffilters=cfg1:value1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Unexpected app in result", entities.contains(
          newEntity(entityType, "application_1111111111_2222")));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRunNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678929");
      verifyHttpResponse(client, uri, Status.NOT_FOUND);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowsNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster2/flows");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowActivityEntity> entities =
          resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetAppNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1378");
      verifyHttpResponse(client, uri, Status.NOT_FOUND);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRunAppsNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster2/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowAppsNotPresent() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/clusters/cluster2/users/user1/flows/flow_name55/apps");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.destroy();
    }
  }

  @After
  public void stop() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
