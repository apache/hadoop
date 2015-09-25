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
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

public class TestTimelineReaderWebServicesFlowRun {
  private int serverPort;
  private TimelineReaderServer server;
  private static HBaseTestingUtility util;
  private static long ts = System.currentTimeMillis();

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
    String id = "flowRunMetrics_test";
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    entity.setId(id);
    entity.setType(type);
    Long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

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
    Long expTs = 1436512802000L;
    event.setTimestamp(expTs);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);
    entity.addEvent(event);
    te.addEntity(entity);

    // write another application with same metric to this flow
    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    id = "flowRunMetrics_test";
    type = TimelineEntityType.YARN_APPLICATION.toString();
    entity1.setId(id);
    entity1.setType(type);
    cTime = 1425016501000L;
    entity1.setCreatedTime(cTime);
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
    te1.addEntity(entity1);

    String flow2 = "flow_name2";
    String flowVersion2 = "CF7022C10F1454";
    Long runid2 = 2102356789046L;
    TimelineEntities te3 = new TimelineEntities();
    TimelineEntity entity3 = new TimelineEntity();
    id = "flowRunMetrics_test1";
    entity3.setId(id);
    entity3.setType(type);
    cTime = 1425016501030L;
    entity3.setCreatedTime(cTime);
    TimelineEvent event2 = new TimelineEvent();
    event2.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
    event2.setTimestamp(1436512802030L);
    event2.addInfo("foo_event", "test");
    entity3.addEvent(event2);
    te3.addEntity(entity3);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      String appName = "application_11111111111111_1111";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      appName = "application_11111111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te1);
      hbi.write(cluster, user, flow, flowVersion, runid1, appName, te);
      appName = "application_11111111111111_2223";
      hbi.write(cluster, user, flow2, flowVersion2, runid2, appName, te3);
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

  private static TimelineMetric newMetric(String id, long ts, Number value) {
    TimelineMetric metric = new TimelineMetric();
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

  @Test
  public void testGetFlowRun() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flowrun/cluster1/flow_name/1002345678919?userid=user1");
      ClientResponse resp = getResponse(client, uri);
      FlowRunEntity entity = resp.getEntity(FlowRunEntity.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getType());
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      TimelineMetric m1 = newMetric("HDFS_BYTES_READ", ts - 80000, 57L);
      TimelineMetric m2 = newMetric("MAP_SLOT_MILLIS", ts - 80000, 141L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flowrun/flow_name/1002345678919?userid=user1");
      resp = getResponse(client, uri);
      entity = resp.getEntity(FlowRunEntity.class);
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(2, entity.getMetrics().size());
      m1 = newMetric("HDFS_BYTES_READ", ts - 80000, 57L);
      m2 = newMetric("MAP_SLOT_MILLIS", ts - 80000, 141L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlows() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + serverPort + "/ws/v2/" +
          "timeline/flows/cluster1");
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
              "timeline/flows/cluster1?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
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
