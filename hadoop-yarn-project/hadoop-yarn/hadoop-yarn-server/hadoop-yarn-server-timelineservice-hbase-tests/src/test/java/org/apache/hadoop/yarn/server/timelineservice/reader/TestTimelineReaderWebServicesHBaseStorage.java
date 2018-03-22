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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.GenericType;

/**
 * Test TimelineReder Web Service REST API's.
 */
public class TestTimelineReaderWebServicesHBaseStorage
    extends AbstractTimelineReaderHBaseTestBase {
  private static long ts = System.currentTimeMillis();
  private static long dayTs =
      HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(ts);
  private static String doAsUser = "remoteuser";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    setup();
    loadData();
    initialize();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    tearDown();
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
    Map<Long, Number> metricValues =
        ImmutableMap.of(ts - 100000, (Number)2, ts - 90000, 7, ts - 80000, 40);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    m1 = new TimelineMetric();
    m1.setId("MAP1_SLOT_MILLIS");
    metricValues =
        ImmutableMap.of(ts - 100000, (Number)2, ts - 90000, 9, ts - 80000, 40);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    m1 = new TimelineMetric();
    m1.setId("HDFS_BYTES_READ");
    metricValues = ImmutableMap.of(ts - 100000, (Number)31, ts - 80000, 57);
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
    metrics.clear();
    m2 = new TimelineMetric();
    m2.setId("MAP_SLOT_MILLIS");
    metricValues = ImmutableMap.of(ts - 100000, (Number)5L, ts - 80000, 101L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues);
    metrics.add(m2);
    entity4.addMetrics(metrics);
    te4.addEntity(entity4);

    TimelineEntities userEntities = new TimelineEntities();
    TimelineEntity entity5 = new TimelineEntity();
    entity5.setId("entity1");
    entity5.setType("type1");
    entity5.setCreatedTime(1425016501034L);
    // add some config entries
    entity5.addConfigs(ImmutableMap.of("config_param1", "value1",
        "config_param2", "value2", "cfg_param1", "value3"));
    entity5.addInfo(ImmutableMap.of("info1", (Object)"cluster1",
        "info2", 2.0, "info3", 35000, "info4", 36000));
    metrics = new HashSet<>();
    m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    metricValues = ImmutableMap.of(ts - 100000, (Number)2, ts - 80000, 40);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    m1 = new TimelineMetric();
    m1.setId("HDFS_BYTES_READ");
    metricValues = ImmutableMap.of(ts - 100000, (Number)31, ts - 80000, 57);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity5.addMetrics(metrics);
    TimelineEvent event51 = new TimelineEvent();
    event51.setId("event1");
    event51.setTimestamp(cTime);
    entity5.addEvent(event51);
    TimelineEvent event52 = new TimelineEvent();
    event52.setId("event2");
    event52.setTimestamp(cTime);
    entity5.addEvent(event52);
    TimelineEvent event53 = new TimelineEvent();
    event53.setId("event3");
    event53.setTimestamp(cTime);
    entity5.addEvent(event53);
    TimelineEvent event54 = new TimelineEvent();
    event54.setId("event4");
    event54.setTimestamp(cTime);
    entity5.addEvent(event54);
    Map<String, Set<String>> isRelatedTo1 = new HashMap<String, Set<String>>();
    isRelatedTo1.put("type2",
        Sets.newHashSet("entity21", "entity22", "entity23", "entity24"));
    isRelatedTo1.put("type4", Sets.newHashSet("entity41", "entity42"));
    isRelatedTo1.put("type1", Sets.newHashSet("entity14", "entity15"));
    isRelatedTo1.put("type3",
        Sets.newHashSet("entity31", "entity35", "entity32", "entity33"));
    entity5.addIsRelatedToEntities(isRelatedTo1);
    Map<String, Set<String>> relatesTo1 = new HashMap<String, Set<String>>();
    relatesTo1.put("type2",
        Sets.newHashSet("entity21", "entity22", "entity23", "entity24"));
    relatesTo1.put("type4", Sets.newHashSet("entity41", "entity42"));
    relatesTo1.put("type1", Sets.newHashSet("entity14", "entity15"));
    relatesTo1.put("type3",
        Sets.newHashSet("entity31", "entity35", "entity32", "entity33"));
    entity5.addRelatesToEntities(relatesTo1);
    userEntities.addEntity(entity5);

    TimelineEntity entity6 = new TimelineEntity();
    entity6.setId("entity2");
    entity6.setType("type1");
    entity6.setCreatedTime(1425016501034L);
    entity6.addConfigs(ImmutableMap.of("cfg_param3", "value1",
        "configuration_param2", "value2", "config_param1", "value3"));
    entity6.addInfo(ImmutableMap.of("info1", (Object)"cluster2",
        "info2", 2.0, "info4", 35000));
    metrics = new HashSet<>();
    m1 = new TimelineMetric();
    m1.setId("MAP1_SLOT_MILLIS");
    metricValues = ImmutableMap.of(ts - 100000, (Number)12, ts - 80000, 140);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    m1 = new TimelineMetric();
    m1.setId("HDFS_BYTES_READ");
    metricValues = ImmutableMap.of(ts - 100000, (Number)78, ts - 80000, 157);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    m1 = new TimelineMetric();
    m1.setId("MAP11_SLOT_MILLIS");
    m1.setType(Type.SINGLE_VALUE);
    m1.addValue(ts - 100000, 122);
    metrics.add(m1);
    entity6.addMetrics(metrics);
    TimelineEvent event61 = new TimelineEvent();
    event61.setId("event1");
    event61.setTimestamp(cTime);
    entity6.addEvent(event61);
    TimelineEvent event62 = new TimelineEvent();
    event62.setId("event5");
    event62.setTimestamp(cTime);
    entity6.addEvent(event62);
    TimelineEvent event63 = new TimelineEvent();
    event63.setId("event3");
    event63.setTimestamp(cTime);
    entity6.addEvent(event63);
    TimelineEvent event64 = new TimelineEvent();
    event64.setId("event6");
    event64.setTimestamp(cTime);
    entity6.addEvent(event64);
    Map<String, Set<String>> isRelatedTo2 = new HashMap<String, Set<String>>();
    isRelatedTo2.put("type2",
        Sets.newHashSet("entity21", "entity22", "entity23", "entity24"));
    isRelatedTo2.put("type5", Sets.newHashSet("entity51", "entity52"));
    isRelatedTo2.put("type6", Sets.newHashSet("entity61", "entity66"));
    isRelatedTo2.put("type3", Sets.newHashSet("entity31"));
    entity6.addIsRelatedToEntities(isRelatedTo2);
    Map<String, Set<String>> relatesTo2 = new HashMap<String, Set<String>>();
    relatesTo2.put("type2",
        Sets.newHashSet("entity21", "entity22", "entity23", "entity24"));
    relatesTo2.put("type5", Sets.newHashSet("entity51", "entity52"));
    relatesTo2.put("type6", Sets.newHashSet("entity61", "entity66"));
    relatesTo2.put("type3", Sets.newHashSet("entity31"));
    entity6.addRelatesToEntities(relatesTo2);
    userEntities.addEntity(entity6);

    for (long i = 1; i <= 10; i++) {
      TimelineEntity userEntity = new TimelineEntity();
      userEntity.setType("entitytype");
      userEntity.setId("entityid-" + i);
      userEntity.setIdPrefix(11 - i);
      userEntity.setCreatedTime(ts);
      userEntities.addEntity(userEntity);
    }

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = getHBaseTestingUtility().getConfiguration();
    UserGroupInformation remoteUser =
        UserGroupInformation.createRemoteUser(doAsUser);
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, entity.getId()), te, remoteUser);
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, entity1.getId()), te1, remoteUser);
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid1, entity4.getId()), te4, remoteUser);
      hbi.write(new TimelineCollectorContext(cluster, user, flow2, flowVersion2,
          runid2, entity3.getId()), te3, remoteUser);
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, "application_1111111111_1111"), userEntities, remoteUser);
      writeApplicationEntities(hbi, ts);
      hbi.flush();
    } finally {
      if (hbi != null) {
        hbi.close();
      }
    }
  }

  static void writeApplicationEntities(HBaseTimelineWriterImpl hbi,
      long timestamp) throws IOException {
    int count = 1;
    for (long i = 1; i <= 3; i++) {
      for (int j = 1; j <= 5; j++) {
        TimelineEntities te = new TimelineEntities();
        ApplicationId appId =
            BuilderUtils.newApplicationId(timestamp, count++);
        ApplicationEntity appEntity = new ApplicationEntity();
        appEntity.setId(
            HBaseTimelineSchemaUtils.convertApplicationIdToString(appId));
        appEntity.setCreatedTime(timestamp);

        TimelineEvent created = new TimelineEvent();
        created.setId(ApplicationMetricsConstants.CREATED_EVENT_TYPE);
        created.setTimestamp(timestamp);
        appEntity.addEvent(created);
        TimelineEvent finished = new TimelineEvent();
        finished.setId(ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
        finished.setTimestamp(timestamp + i * j);

        appEntity.addEvent(finished);
        te.addEntity(appEntity);
        hbi.write(new TimelineCollectorContext("cluster1", "user1", "flow1",
            "CF7022C10F1354", i, appEntity.getId()), te,
            UserGroupInformation.createRemoteUser("user1"));
      }
    }
  }

  private static TimelineEntity newEntity(String type, String id) {
    TimelineEntity entity = new TimelineEntity();
    entity.setIdentifier(new TimelineEntity.Identifier(type, id));
    return entity;
  }

  private static TimelineMetric newMetric(TimelineMetric.Type type,
      String id, long t, Number value) {
    TimelineMetric metric = new TimelineMetric(type);
    metric.setId(id);
    metric.addValue(t, value);
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919");
      ClientResponse resp = getResponse(client, uri);
      FlowRunEntity entity = resp.getEntity(FlowRunEntity.class);
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(3, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 141L);
      TimelineMetric m3 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP1_SLOT_MILLIS", ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2, m3));
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/runs/1002345678919");
      resp = getResponse(client, uri);
      entity = resp.getEntity(FlowRunEntity.class);
      assertNotNull(entity);
      assertEquals("user1@flow_name/1002345678919", entity.getId());
      assertEquals(3, entity.getMetrics().size());
      m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 141L);
      m3 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP1_SLOT_MILLIS", ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2, m3));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRuns() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowRunEntity> entities =
          resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
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

      uri =
          URI.create("http://localhost:" + getServerPort() + "/ws/v2/timeline/"
              + "clusters/cluster1/users/user1/flows/flow_name/runs?limit=1");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            entity.getId().equals("user1@flow_name/1002345678920") &&
            entity.getRunId() == 1002345678920L &&
            entity.getStartTime() == 1425016501034L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimestart=1425016501030");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            entity.getId().equals("user1@flow_name/1002345678920") &&
            entity.getRunId() == 1002345678920L &&
            entity.getStartTime() == 1425016501034L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimestart=1425016500999&createdtimeend=1425016501035");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
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

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "createdtimeend=1425016501030");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
             entity.getId().equals("user1@flow_name/1002345678919") &&
             entity.getRunId() == 1002345678919L &&
             entity.getStartTime() == 1425016501000L);
        assertEquals(0, entity.getMetrics().size());
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "fields=metrics");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (FlowRunEntity entity : entities) {
        assertTrue("Id, run id or start time does not match.",
            ((entity.getId().equals("user1@flow_name/1002345678919")) &&
            (entity.getRunId() == 1002345678919L) &&
            (entity.getStartTime() == 1425016501000L) &&
            (entity.getMetrics().size() == 3)) ||
            ((entity.getId().equals("user1@flow_name/1002345678920")) &&
            (entity.getRunId() == 1002345678920L) &&
            (entity.getStartTime() == 1425016501034L) &&
            (entity.getMetrics().size() == 1)));
      }

      // fields as CONFIGS will lead to a HTTP 400 as it makes no sense for
      // flow runs.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "fields=CONFIGS");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRunsMetricsToRetrieve() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "metricstoretrieve=MAP_,HDFS_");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowRunEntity> entities =
          resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      int metricCnt = 0;
      for (FlowRunEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP_") ||
              metric.getId().startsWith("HDFS_"));
        }
      }
      assertEquals(3, metricCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs?" +
          "metricstoretrieve=!(MAP_,HDFS_)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(2, entities.size());
      metricCnt = 0;
      for (FlowRunEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP1_"));
        }
      }
      assertEquals(1, metricCnt);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesByUID() throws Exception {
    Client client = createClient();
    try {
      // Query all flows.
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/flows");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowActivityEntity> flowEntities =
          resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertNotNull(flowEntities);
      assertEquals(3, flowEntities.size());
      List<String> listFlowUIDs = new ArrayList<String>();
      for (FlowActivityEntity entity : flowEntities) {
        String flowUID =
            (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
        listFlowUIDs.add(flowUID);
        assertEquals(TimelineUIDConverter.FLOW_UID.encodeUID(
            new TimelineReaderContext(entity.getCluster(), entity.getUser(),
            entity.getFlowName(), null, null, null, null)), flowUID);
        assertTrue((entity.getId().endsWith("@flow_name") &&
            entity.getFlowRuns().size() == 2) ||
            (entity.getId().endsWith("@flow_name2") &&
                entity.getFlowRuns().size() == 1)
            || (entity.getId().endsWith("@flow1")
                && entity.getFlowRuns().size() == 3));
      }

      // Query flowruns based on UID returned in query above.
      List<String> listFlowRunUIDs = new ArrayList<String>();
      for (String flowUID : listFlowUIDs) {
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
            "timeline/flow-uid/" + flowUID + "/runs");
        resp = getResponse(client, uri);
        Set<FlowRunEntity> frEntities =
            resp.getEntity(new GenericType<Set<FlowRunEntity>>(){});
        assertNotNull(frEntities);
        for (FlowRunEntity entity : frEntities) {
          String flowRunUID =
              (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
          listFlowRunUIDs.add(flowRunUID);
          assertEquals(TimelineUIDConverter.FLOWRUN_UID.encodeUID(
              new TimelineReaderContext("cluster1", entity.getUser(),
              entity.getName(), entity.getRunId(), null, null, null)),
              flowRunUID);
        }
      }
      assertEquals(6, listFlowRunUIDs.size());

      // Query single flowrun based on UIDs' returned in query to get flowruns.
      for (String flowRunUID : listFlowRunUIDs) {
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
            "timeline/run-uid/" + flowRunUID + "/apps");
        resp = getResponse(client, uri);
        Set<TimelineEntity> appEntities =
            resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
        assertNotNull(appEntities);
        for (TimelineEntity entity : appEntities) {
          String appUID =
              (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
          listAppUIDs.add(appUID);
          assertEquals(TimelineUIDConverter.APPLICATION_UID.encodeUID(
              new TimelineReaderContext(context.getClusterId(),
              context.getUserId(), context.getFlowName(),
              context.getFlowRunId(), entity.getId(), null, null)), appUID);
        }
      }
      assertEquals(19, listAppUIDs.size());

      // Query single app based on UIDs' returned in query to get apps.
      for (String appUID : listAppUIDs) {
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
            "timeline/app-uid/" + appUID + "/entities/type1");
        resp = getResponse(client, uri);
        Set<TimelineEntity> entities =
            resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
        assertNotNull(entities);
        for (TimelineEntity entity : entities) {
          String entityUID =
              (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
          listEntityUIDs.add(entityUID);
          assertEquals(TimelineUIDConverter.GENERIC_ENTITY_UID.encodeUID(
              new TimelineReaderContext(context.getClusterId(),
              context.getUserId(), context.getFlowName(),
                  context.getFlowRunId(), context.getAppId(), "type1",
                  entity.getIdPrefix(),
              entity.getId())), entityUID);
        }
      }
      assertEquals(2, listEntityUIDs.size());

      // Query single entity based on UIDs' returned in query to get entities.
      for (String entityUID : listEntityUIDs) {
        uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
            "timeline/entity-uid/" + entityUID);
        resp = getResponse(client, uri);
        TimelineEntity entity = resp.getEntity(TimelineEntity.class);
        assertNotNull(entity);
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/flow-uid/dummy:flow/runs");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/run-uid/dummy:flowrun");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      // Run Id is not a numerical value.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/run-uid/some:dummy:flow:123v456");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/run-uid/dummy:flowrun/apps");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/app-uid/dummy:app");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/app-uid/dummy:app/entities/type1");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/"+
          "timeline/app-uid/" + appUIDWithFlowInfo);
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity appEntity1 = resp.getEntity(TimelineEntity.class);
      assertNotNull(appEntity1);
      assertEquals(
          TimelineEntityType.YARN_APPLICATION.toString(), appEntity1.getType());
      assertEquals("application_1111111111_1111", appEntity1.getId());

      uri =
          URI.create("http://localhost:" + getServerPort() + "/ws/v2/timeline/"
              + "app-uid/" + appUIDWithFlowInfo + "/entities/type1");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities1 =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities1);
      assertEquals(2, entities1.size());
      for (TimelineEntity entity : entities1) {
        assertNotNull(entity.getInfo());
        assertEquals(2, entity.getInfo().size());
        String uid =
            (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
        assertNotNull(uid);
        assertTrue(uid.equals(appUIDWithFlowInfo + "!type1!0!entity1")
            || uid.equals(appUIDWithFlowInfo + "!type1!0!entity2"));
      }

      String appUIDWithoutFlowInfo = "cluster1!application_1111111111_1111";
      uri = URI.create("http://localhost:" + getServerPort()
          + "/ws/v2/timeline/" + "app-uid/" + appUIDWithoutFlowInfo);
      resp = getResponse(client, uri);
      TimelineEntity appEntity2 = resp.getEntity(TimelineEntity.class);
      assertNotNull(appEntity2);
      assertEquals(
          TimelineEntityType.YARN_APPLICATION.toString(), appEntity2.getType());
      assertEquals("application_1111111111_1111", appEntity2.getId());

      uri =
          URI.create("http://localhost:" + getServerPort() + "/ws/v2/timeline/"
              + "app-uid/" + appUIDWithoutFlowInfo + "/entities/type1");
      resp = getResponse(client, uri);
      Set<TimelineEntity> entities2 =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities2);
      assertEquals(2, entities2.size());
      for (TimelineEntity entity : entities2) {
        assertNotNull(entity.getInfo());
        assertEquals(2, entity.getInfo().size());
        String uid =
            (String) entity.getInfo().get(TimelineReaderUtils.UID_KEY);
        assertNotNull(uid);
        assertTrue(uid.equals(appUIDWithoutFlowInfo + "!type1!0!entity1")
            || uid.equals(appUIDWithoutFlowInfo + "!type1!0!entity2"));
      }

      String entityUIDWithFlowInfo = appUIDWithFlowInfo + "!type1!0!entity1";
      uri = URI.create("http://localhost:" + getServerPort()
          + "/ws/v2/timeline/" + "entity-uid/" + entityUIDWithFlowInfo);
      resp = getResponse(client, uri);
      TimelineEntity singleEntity1 = resp.getEntity(TimelineEntity.class);
      assertNotNull(singleEntity1);
      assertEquals("type1", singleEntity1.getType());
      assertEquals("entity1", singleEntity1.getId());

      String entityUIDWithoutFlowInfo =
          appUIDWithoutFlowInfo + "!type1!0!entity1";
      uri = URI.create("http://localhost:" + getServerPort()
          + "/ws/v2/timeline/" + "entity-uid/" + entityUIDWithoutFlowInfo);
      resp = getResponse(client, uri);
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/"+
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows");

      verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
          new String[] {"flow1", "flow_name", "flow_name2"});

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/flows/");
      verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
          new String[] {"flow1", "flow_name", "flow_name2"});

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
              "timeline/clusters/cluster1/flows?limit=1");
      verifyFlowEntites(client, uri, 1, new int[] {3},
          new String[] {"flow1"});

      long firstFlowActivity =
          HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(1425016501000L);

      DateFormat fmt = TimelineReaderWebServices.DATE_FORMAT.get();
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange="
          + fmt.format(firstFlowActivity) + "-"
          + fmt.format(dayTs));
      verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
          new String[] {"flow1", "flow_name", "flow_name2"});

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=" +
          fmt.format(dayTs + (4*86400000L)));
      verifyFlowEntites(client, uri, 0, new int[] {}, new String[] {});

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=-" +
          fmt.format(dayTs));
      verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
          new String[] {"flow1", "flow_name", "flow_name2"});

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=" +
           fmt.format(firstFlowActivity) + "-");
      verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
          new String[] {"flow1", "flow_name", "flow_name2"});

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150711:20150714");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150714-20150711");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=2015071129-20150712");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/flows?daterange=20150711-2015071243");
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowsForPagination() throws Exception {
    Client client = createClient();
    int noOfEntities = 3;
    int limit = 2;
    try {
      String flowURI = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/flows";
      URI uri = URI.create(flowURI);
      List<FlowActivityEntity> flowEntites =
          verifyFlowEntites(client, uri, 3, new int[] {3, 2, 1},
              new String[] {"flow1", "flow_name", "flow_name2"});
      FlowActivityEntity fEntity1 = flowEntites.get(0);
      FlowActivityEntity fEntity3 = flowEntites.get(noOfEntities - 1);

      uri = URI.create(flowURI + "?limit=" + limit);
      flowEntites = verifyFlowEntites(client, uri, limit);
      assertEquals(fEntity1, flowEntites.get(0));
      FlowActivityEntity fEntity2 = flowEntites.get(limit - 1);

      uri = URI
          .create(flowURI + "?limit=" + limit + "&fromid="
              + fEntity2.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      flowEntites = verifyFlowEntites(client, uri, noOfEntities - limit + 1);
      assertEquals(fEntity2, flowEntites.get(0));
      assertEquals(fEntity3, flowEntites.get(noOfEntities - limit));

      uri = URI
          .create(flowURI + "?limit=" + limit + "&fromid="
              + fEntity3.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      flowEntites = verifyFlowEntites(client, uri, 1);
      assertEquals(fEntity3, flowEntites.get(0));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetApp() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111?" +
          "userid=user1&fields=ALL&flowname=flow_name&flowrunid=1002345678919");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_1111", entity.getId());
      assertEquals(3, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 40L);
      TimelineMetric m3 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP1_SLOT_MILLIS", ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2, m3));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
              "timeline/apps/application_1111111111_2222?userid=user1" +
              "&fields=metrics&flowname=flow_name&flowrunid=1002345678919");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_2222", entity.getId());
      assertEquals(1, entity.getMetrics().size());
      TimelineMetric m4 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 101L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m4));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetAppWithoutFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111?" +
          "fields=ALL");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_1111", entity.getId());
      assertEquals(1, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "HDFS_BYTES_READ", ts - 80000, 57L);
      TimelineMetric m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP_SLOT_MILLIS", ts - 80000, 40L);
      TimelineMetric m3 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
          "MAP1_SLOT_MILLIS", ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2, m3));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111?" +
          "fields=ALL&metricslimit=10");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("application_1111111111_1111", entity.getId());
      assertEquals(1, entity.getConfigs().size());
      assertEquals(3, entity.getMetrics().size());
      m1 = newMetric(TimelineMetric.Type.TIME_SERIES, "HDFS_BYTES_READ",
          ts - 100000, 31L);
      m1.addValue(ts - 80000, 57L);
      m2 = newMetric(TimelineMetric.Type.TIME_SERIES, "MAP_SLOT_MILLIS",
          ts - 100000, 2L);
      m2.addValue(ts - 80000, 40L);
      m3 = newMetric(TimelineMetric.Type.TIME_SERIES, "MAP1_SLOT_MILLIS",
          ts - 100000, 2L);
      m3.addValue(ts - 80000, 40L);
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue(verifyMetrics(metric, m1, m2, m3));
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntityWithoutFlowInfo() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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

  /**
   * Tests if specific configs and metrics are retrieve for getEntities call.
   */
  @Test
  public void testGetEntitiesDataToRetrieve() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?confstoretrieve=cfg_");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      int cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        for (String configKey : entity.getConfigs().keySet()) {
          assertTrue(configKey.startsWith("cfg_"));
        }
      }
      assertEquals(2, cfgCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?confstoretrieve=cfg_,config_");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        for (String configKey : entity.getConfigs().keySet()) {
          assertTrue(configKey.startsWith("cfg_") ||
              configKey.startsWith("config_"));
        }
      }
      assertEquals(5, cfgCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?confstoretrieve=!(cfg_,config_)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        for (String configKey : entity.getConfigs().keySet()) {
          assertTrue(configKey.startsWith("configuration_"));
        }
      }
      assertEquals(1, cfgCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricstoretrieve=MAP_");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      int metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP_"));
        }
      }
      assertEquals(1, metricCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricstoretrieve=MAP1_,HDFS_");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP1_") ||
              metric.getId().startsWith("HDFS_"));
        }
      }
      assertEquals(3, metricCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricstoretrieve=!(MAP1_,HDFS_)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP_") ||
              metric.getId().startsWith("MAP11_"));
        }
      }
      assertEquals(2, metricCnt);
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesConfigFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=config_param1%20eq%20value1%20OR%20" +
          "config_param1%20eq%20value3");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=config_param1%20eq%20value1%20AND" +
          "%20configuration_param2%20eq%20value2");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      // conffilters=(config_param1 eq value1 AND configuration_param2 eq
      // value2) OR (config_param1 eq value3 AND cfg_param3 eq value1)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=(config_param1%20eq%20value1%20AND" +
          "%20configuration_param2%20eq%20value2)%20OR%20(config_param1%20eq" +
          "%20value3%20AND%20cfg_param3%20eq%20value1)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      int cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        assertEquals("entity2", entity.getId());
      }
      assertEquals(0, cfgCnt);

      // conffilters=(config_param1 eq value1 AND configuration_param2 eq
      // value2) OR (config_param1 eq value3 AND cfg_param3 eq value1)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=(config_param1%20eq%20value1%20AND" +
          "%20configuration_param2%20eq%20value2)%20OR%20(config_param1%20eq" +
          "%20value3%20AND%20cfg_param3%20eq%20value1)&fields=CONFIGS");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        assertEquals("entity2", entity.getId());
      }
      assertEquals(3, cfgCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=(config_param1%20eq%20value1%20AND" +
          "%20configuration_param2%20eq%20value2)%20OR%20(config_param1%20eq" +
          "%20value3%20AND%20cfg_param3%20eq%20value1)&confstoretrieve=cfg_," +
          "configuration_");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      cfgCnt = 0;
      for (TimelineEntity entity : entities) {
        cfgCnt += entity.getConfigs().size();
        assertEquals("entity2", entity.getId());
        for (String configKey : entity.getConfigs().keySet()) {
          assertTrue(configKey.startsWith("cfg_") ||
              configKey.startsWith("configuration_"));
        }
      }
      assertEquals(2, cfgCnt);

      // Test for behavior when compare op is ne(not equals) vs ene
      // (exists and not equals). configuration_param2 does not exist for
      // entity1. For ne, both entity1 and entity2 will be returned. For ene,
      // only entity2 will be returned as we are checking for existence too.
      // conffilters=configuration_param2 ne value3
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=configuration_param2%20ne%20value3");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }
      // conffilters=configuration_param2 ene value3
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?conffilters=configuration_param2%20ene%20value3");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity2", entity.getId());
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesInfoFilters() throws Exception {
    Client client = createClient();
    try {
      // infofilters=info1 eq cluster1 OR info1 eq cluster2
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=info1%20eq%20cluster1%20OR%20info1%20eq" +
          "%20cluster2");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      // infofilters=info1 eq cluster1 AND info4 eq 35000
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=info1%20eq%20cluster1%20AND%20info4%20" +
          "eq%2035000");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      // infofilters=info4 eq 35000 OR info4 eq 36000
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=info4%20eq%2035000%20OR%20info4%20eq" +
          "%2036000");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      // infofilters=(info1 eq cluster1 AND info4 eq 35000) OR
      // (info1 eq cluster2 AND info2 eq 2.0)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=(info1%20eq%20cluster1%20AND%20info4%20" +
          "eq%2035000)%20OR%20(info1%20eq%20cluster2%20AND%20info2%20eq%202.0" +
          ")");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      int infoCnt = 0;
      for (TimelineEntity entity : entities) {
        infoCnt += entity.getInfo().size();
        assertEquals("entity2", entity.getId());
      }
      // Includes UID and FROM_ID in info field even if fields not specified as
      // INFO.
      assertEquals(2, infoCnt);

      // infofilters=(info1 eq cluster1 AND info4 eq 35000) OR
      // (info1 eq cluster2 AND info2 eq 2.0)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=(info1%20eq%20cluster1%20AND%20info4%20" +
          "eq%2035000)%20OR%20(info1%20eq%20cluster2%20AND%20info2%20eq%20" +
          "2.0)&fields=INFO");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      infoCnt = 0;
      for (TimelineEntity entity : entities) {
        infoCnt += entity.getInfo().size();
        assertEquals("entity2", entity.getId());
      }
      // Includes UID and FROM_ID in info field.
      assertEquals(5, infoCnt);

      // Test for behavior when compare op is ne(not equals) vs ene
      // (exists and not equals). info3 does not exist for entity2. For ne,
      // both entity1 and entity2 will be returned. For ene, only entity2 will
      // be returned as we are checking for existence too.
      // infofilters=info3 ne 39000
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=info3%20ne%2039000");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }
      // infofilters=info3 ene 39000
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?infofilters=info3%20ene%2039000");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity1", entity.getId());
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesMetricFilters() throws Exception {
    Client client = createClient();
    try {
      // metricfilters=HDFS_BYTES_READ lt 60 OR HDFS_BYTES_READ eq 157
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=HDFS_BYTES_READ%20lt%2060%20OR%20" +
          "HDFS_BYTES_READ%20eq%20157");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      // metricfilters=HDFS_BYTES_READ lt 60 AND MAP_SLOT_MILLIS gt 40
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=HDFS_BYTES_READ%20lt%2060%20AND%20" +
          "MAP_SLOT_MILLIS%20gt%2040");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      // metricfilters=(HDFS_BYTES_READ lt 60 AND MAP_SLOT_MILLIS gt 40) OR
      // (MAP1_SLOT_MILLIS ge 140 AND MAP11_SLOT_MILLIS le 122)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=(HDFS_BYTES_READ%20lt%2060%20AND%20" +
          "MAP_SLOT_MILLIS%20gt%2040)%20OR%20(MAP1_SLOT_MILLIS%20ge" +
          "%20140%20AND%20MAP11_SLOT_MILLIS%20le%20122)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      int metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        assertEquals("entity2", entity.getId());
      }
      assertEquals(0, metricCnt);

      // metricfilters=(HDFS_BYTES_READ lt 60 AND MAP_SLOT_MILLIS gt 40) OR
      // (MAP1_SLOT_MILLIS ge 140 AND MAP11_SLOT_MILLIS le 122)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=(HDFS_BYTES_READ%20lt%2060%20AND%20" +
          "MAP_SLOT_MILLIS%20gt%2040)%20OR%20(MAP1_SLOT_MILLIS%20ge" +
          "%20140%20AND%20MAP11_SLOT_MILLIS%20le%20122)&fields=METRICS");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        assertEquals("entity2", entity.getId());
      }
      assertEquals(3, metricCnt);

      // metricfilters=(HDFS_BYTES_READ lt 60 AND MAP_SLOT_MILLIS gt 40) OR
      // (MAP1_SLOT_MILLIS ge 140 AND MAP11_SLOT_MILLIS le 122)
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=(HDFS_BYTES_READ%20lt%2060%20AND%20" +
          "MAP_SLOT_MILLIS%20gt%2040)%20OR%20(MAP1_SLOT_MILLIS%20ge" +
          "%20140%20AND%20MAP11_SLOT_MILLIS%20le%20122)&metricstoretrieve=" +
          "!(HDFS)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        assertEquals("entity2", entity.getId());
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP1"));
          assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
        }
      }
      assertEquals(2, metricCnt);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=(HDFS_BYTES_READ%20lt%2060%20AND%20" +
          "MAP_SLOT_MILLIS%20gt%2040)%20OR%20(MAP1_SLOT_MILLIS%20ge" +
          "%20140%20AND%20MAP11_SLOT_MILLIS%20le%20122)&metricstoretrieve=" +
          "!(HDFS)&metricslimit=10");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      metricCnt = 0;
      for (TimelineEntity entity : entities) {
        metricCnt += entity.getMetrics().size();
        assertEquals("entity2", entity.getId());
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getId().startsWith("MAP1"));
          if (metric.getId().equals("MAP1_SLOT_MILLIS")) {
            assertEquals(2, metric.getValues().size());
            assertEquals(TimelineMetric.Type.TIME_SERIES, metric.getType());
          } else if (metric.getId().equals("MAP11_SLOT_MILLIS")) {
            assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
          } else {
            fail("Unexpected metric id");
          }
        }
      }
      assertEquals(2, metricCnt);

      // Test for behavior when compare op is ne(not equals) vs ene
      // (exists and not equals). MAP11_SLOT_MILLIS does not exist for
      // entity1. For ne, both entity1 and entity2 will be returned. For ene,
      // only entity2 will be returned as we are checking for existence too.
      // metricfilters=MAP11_SLOT_MILLIS ne 100
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=MAP11_SLOT_MILLIS%20ne%20100");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }
      // metricfilters=MAP11_SLOT_MILLIS ene 100
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?metricfilters=MAP11_SLOT_MILLIS%20ene%20100");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity2", entity.getId());
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetEntitiesEventFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?eventfilters=event1,event3");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?eventfilters=!(event1,event3)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      // eventfilters=!(event1,event3) OR event5,event6
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?eventfilters=!(event1,event3)%20OR%20event5,event6");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity2", entity.getId());
      }

      //  eventfilters=(!(event1,event3) OR event5,event6) OR
      // (event1,event2 AND (event3,event4))
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?eventfilters=(!(event1,event3)%20OR%20event5," +
          "event6)%20OR%20(event1,event2%20AND%20(event3,event4))");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
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
  public void testGetEntitiesRelationFilters() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?isrelatedto=type3:entity31,type2:entity21:entity22");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      uri = URI.create("http://localhost:" + getServerPort() +
          "/ws/v2/timeline/" +
          "clusters/cluster1/apps/application_1111111111_1111/entities/type1" +
          "?isrelatedto=!(type3:entity31,type2:entity21:entity22)");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(0, entities.size());

      // isrelatedto=!(type3:entity31,type2:entity21:entity22)OR type5:entity51,
      // type6:entity61:entity66
      uri = URI.create("http://localhost:" + getServerPort() +
          "/ws/v2/timeline/" +
          "clusters/cluster1/apps/application_1111111111_1111/entities/type1" +
          "?isrelatedto=!(type3:entity31,type2:entity21:entity22)%20OR%20" +
          "type5:entity51,type6:entity61:entity66");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity2", entity.getId());
      }

      // isrelatedto=(!(type3:entity31,type2:entity21:entity22)OR type5:
      // entity51,type6:entity61:entity66) OR (type1:entity14,type2:entity21:
      // entity22 AND (type3:entity32:entity35,type4:entity42))
      uri = URI.create("http://localhost:" + getServerPort() +
          "/ws/v2/timeline/" +
          "clusters/cluster1/apps/application_1111111111_1111/entities/type1" +
          "?isrelatedto=(!(type3:entity31,type2:entity21:entity22)%20OR%20" +
          "type5:entity51,type6:entity61:entity66)%20OR%20(type1:entity14," +
          "type2:entity21:entity22%20AND%20(type3:entity32:entity35,"+
          "type4:entity42))");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue(entity.getId().equals("entity1") ||
            entity.getId().equals("entity2"));
      }

      // relatesto=!(type3:entity31,type2:entity21:entity22)OR type5:entity51,
      // type6:entity61:entity66
      uri = URI.create("http://localhost:" + getServerPort() +
          "/ws/v2/timeline/" +
          "clusters/cluster1/apps/application_1111111111_1111/entities/type1" +
          "?relatesto=!%20(type3:entity31,type2:entity21:entity22%20)%20OR%20" +
          "type5:entity51,type6:entity61:entity66");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      for (TimelineEntity entity : entities) {
        assertEquals("entity2", entity.getId());
      }

      // relatesto=(!(type3:entity31,type2:entity21:entity22)OR type5:entity51,
      // type6:entity61:entity66) OR (type1:entity14,type2:entity21:entity22 AND
      // (type3:entity32:entity35 , type4:entity42))
      uri =
          URI.create("http://localhost:" + getServerPort() +
          "/ws/v2/timeline/" +
          "clusters/cluster1/apps/application_1111111111_1111/entities/type1" +
          "?relatesto=(!(%20type3:entity31,type2:entity21:entity22)%20OR%20" +
          "type5:entity51,type6:entity61:entity66%20)%20OR%20(type1:entity14," +
          "type2:entity21:entity22%20AND%20(type3:entity32:entity35%20,%20"+
          "type4:entity42))");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
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

  private static void verifyMetricCount(TimelineEntity entity,
      int expectedMetricsCnt, int expectedMeticsValCnt) {
    int metricsValCnt = 0;
    for (TimelineMetric m : entity.getMetrics()) {
      metricsValCnt += m.getValues().size();
    }
    assertEquals(expectedMetricsCnt, entity.getMetrics().size());
    assertEquals(expectedMeticsValCnt, metricsValCnt);
  }

  private static void verifyMetricsCount(Set<TimelineEntity> entities,
      int expectedMetricsCnt, int expectedMeticsValCnt) {
    int metricsCnt = 0;
    int metricsValCnt = 0;
    for (TimelineEntity entity : entities) {
      metricsCnt += entity.getMetrics().size();
      for (TimelineMetric m : entity.getMetrics()) {
        metricsValCnt += m.getValues().size();
      }
    }
    assertEquals(expectedMetricsCnt, metricsCnt);
    assertEquals(expectedMeticsValCnt, metricsValCnt);
  }

  @Test
  public void testGetEntitiesMetricsTimeRange() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 90000) + "&metricstimeend=" + (ts - 80000));
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 4, 4);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 100000) + "&metricstimeend=" + (ts - 80000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 5, 9);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 100000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 5, 9);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricslimit=100&metricstimeend=" +
          (ts - 90000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 5, 5);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricstimestart=" +
          (ts - 100000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 5, 5);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?fields=ALL&metricstimestart=" +
          (ts - 100000) + "&metricstimeend=" + (ts - 80000));
      resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      verifyMetricCount(entity, 3, 3);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?fields=ALL&metricslimit=5&metricstimestart=" +
          (ts - 100000) + "&metricstimeend=" + (ts - 80000));
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      verifyMetricCount(entity, 3, 5);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 80000) + "&metricstimeend=" + (ts - 90000));
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }

  /**
   * Tests if specific configs and metrics are retrieve for getEntity call.
   */
  @Test
  public void testGetEntityDataToRetrieve() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?confstoretrieve=cfg_,configuration_");
      ClientResponse resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity2", entity.getId());
      assertEquals("type1", entity.getType());
      assertEquals(2, entity.getConfigs().size());
      for (String configKey : entity.getConfigs().keySet()) {
        assertTrue(configKey.startsWith("configuration_") ||
            configKey.startsWith("cfg_"));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?confstoretrieve=!(cfg_,configuration_)");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity2", entity.getId());
      assertEquals("type1", entity.getType());
      assertEquals(1, entity.getConfigs().size());
      for (String configKey : entity.getConfigs().keySet()) {
        assertTrue(configKey.startsWith("config_"));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?metricstoretrieve=MAP1_,HDFS_");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity2", entity.getId());
      assertEquals("type1", entity.getType());
      assertEquals(2, entity.getMetrics().size());
      for (TimelineMetric  metric : entity.getMetrics()) {
        assertTrue(metric.getId().startsWith("MAP1_") ||
            metric.getId().startsWith("HDFS_"));
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?metricstoretrieve=!(MAP1_,HDFS_)");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity2", entity.getId());
      assertEquals("type1", entity.getType());
      assertEquals(1, entity.getMetrics().size());
      for (TimelineMetric  metric : entity.getMetrics()) {
        assertTrue(metric.getId().startsWith("MAP11_"));
        assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
        assertEquals(1, metric.getValues().size());
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/apps/application_1111111111_1111/" +
          "entities/type1/entity2?metricstoretrieve=!(MAP1_,HDFS_)&" +
          "metricslimit=5");
      resp = getResponse(client, uri);
      entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      assertEquals("entity2", entity.getId());
      assertEquals("type1", entity.getType());
      assertEquals(1, entity.getMetrics().size());
      for (TimelineMetric  metric : entity.getMetrics()) {
        assertTrue(metric.getId().startsWith("MAP11_"));
        assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
      }
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetFlowRunApps() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
            entity.getMetrics().size() == 3) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getMetrics().size() == 1));
        for (TimelineMetric metric : entity.getMetrics()) {
          assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
          assertEquals(1, metric.getValues().size());
        }
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
              "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
              "1002345678919/apps?fields=ALL&metricslimit=2");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue("Unexpected app in result",
            (entity.getId().equals("application_1111111111_1111") &&
            entity.getMetrics().size() == 3) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getMetrics().size() == 1));
        for (TimelineMetric metric : entity.getMetrics()) {
          assertTrue(metric.getValues().size() <= 2);
          assertEquals(TimelineMetric.Type.TIME_SERIES, metric.getType());
        }
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/runs/1002345678919/apps");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
            entity.getConfigs().size() == 1 &&
            entity.getConfigs().equals(ImmutableMap.of("cfg2", "value1"))) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getConfigs().size() == 1 &&
            entity.getConfigs().equals(ImmutableMap.of("cfg1", "value1"))) ||
            (entity.getId().equals("application_1111111111_2224") &&
            entity.getConfigs().size() == 0));
        for (TimelineMetric metric : entity.getMetrics()) {
          if (entity.getId().equals("application_1111111111_1111")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
                "HDFS_BYTES_READ", ts - 80000, 57L);
            TimelineMetric m2 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
                "MAP_SLOT_MILLIS", ts - 80000, 40L);
            TimelineMetric m3 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
                "MAP1_SLOT_MILLIS", ts - 80000, 40L);
            assertTrue(verifyMetrics(metric, m1, m2, m3));
          } else if (entity.getId().equals("application_1111111111_2222")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
                "MAP_SLOT_MILLIS", ts - 80000, 101L);
            assertTrue(verifyMetrics(metric, m1));
          } else if (entity.getId().equals("application_1111111111_2224")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.SINGLE_VALUE,
                "MAP_SLOT_MILLIS", ts - 80000, 101L);
            assertTrue(verifyMetrics(metric, m1));
          }
        }
      }

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "fields=ALL&metricslimit=6");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());
      for (TimelineEntity entity : entities) {
        assertTrue("Unexpected app in result",
            (entity.getId().equals("application_1111111111_1111") &&
            entity.getConfigs().size() == 1 &&
            entity.getConfigs().equals(ImmutableMap.of("cfg2", "value1"))) ||
            (entity.getId().equals("application_1111111111_2222") &&
            entity.getConfigs().size() == 1 &&
            entity.getConfigs().equals(ImmutableMap.of("cfg1", "value1"))) ||
            (entity.getId().equals("application_1111111111_2224") &&
            entity.getConfigs().size() == 0));
        for (TimelineMetric metric : entity.getMetrics()) {
          if (entity.getId().equals("application_1111111111_1111")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.TIME_SERIES,
                "HDFS_BYTES_READ", ts - 80000, 57L);
            m1.addValue(ts - 100000, 31L);
            TimelineMetric m2 = newMetric(TimelineMetric.Type.TIME_SERIES,
                "MAP_SLOT_MILLIS", ts - 80000, 40L);
            m2.addValue(ts - 100000, 2L);
            TimelineMetric m3 = newMetric(TimelineMetric.Type.TIME_SERIES,
                "MAP1_SLOT_MILLIS", ts - 80000, 40L);
            m3.addValue(ts - 100000, 2L);
            assertTrue(verifyMetrics(metric, m1, m2, m3));
          } else if (entity.getId().equals("application_1111111111_2222")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.TIME_SERIES,
                "MAP_SLOT_MILLIS", ts - 80000, 101L);
            m1.addValue(ts - 100000, 5L);
            assertTrue(verifyMetrics(metric, m1));
          } else if (entity.getId().equals("application_1111111111_2224")) {
            TimelineMetric m1 = newMetric(TimelineMetric.Type.TIME_SERIES,
                "MAP_SLOT_MILLIS", ts - 80000, 101L);
            m1.addValue(ts - 100000, 5L);
            assertTrue(verifyMetrics(metric, m1));
          }
        }
      }

      // Query without specifying cluster ID.
      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/users/user1/flows/flow_name/apps");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "eventfilters=" + ApplicationMetricsConstants.FINISHED_EVENT_TYPE);
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Unexpected app in result", entities.contains(
          newEntity(entityType, "application_1111111111_1111")));

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "metricfilters=HDFS_BYTES_READ%20ge%200");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertTrue("Unexpected app in result", entities.contains(
          newEntity(entityType, "application_1111111111_1111")));

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/apps?" +
          "conffilters=cfg1%20eq%20value1");
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster2/flows");
      ClientResponse resp = getResponse(client, uri);
      Set<FlowActivityEntity> entities =
          resp.getEntity(new GenericType<Set<FlowActivityEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster2/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
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
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster2/users/user1/flows/flow_name55/apps");
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertEquals(MediaType.APPLICATION_JSON_TYPE + "; charset=utf-8",
          resp.getType().toString());
      assertNotNull(entities);
      assertEquals(0, entities.size());
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGenericEntitiesForPagination() throws Exception {
    Client client = createClient();
    try {
      String resourceUri = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/apps/application_1111111111_1111/"
          + "entities/entitytype";
      verifyEntitiesForPagination(client, resourceUri);
      resourceUri = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/users/" + doAsUser
          + "/entities/entitytype";
      verifyEntitiesForPagination(client, resourceUri);
    } finally {
      client.destroy();
    }
  }

  private void verifyEntitiesForPagination(Client client, String resourceUri)
      throws Exception {
    int limit = 10;
    String queryParam = "?limit=" + limit;
    URI uri = URI.create(resourceUri + queryParam);

    ClientResponse resp = getResponse(client, uri);
    List<TimelineEntity> entities =
        resp.getEntity(new GenericType<List<TimelineEntity>>() {
        });
    // verify for entity-10 to entity-1 in descending order.
    verifyPaginatedEntites(entities, limit, limit);

    limit = 4;
    queryParam = "?limit=" + limit;
    uri = URI.create(resourceUri + queryParam);
    resp = getResponse(client, uri);
    entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
    });
    // verify for entity-10 to entity-7 in descending order.
    TimelineEntity entity = verifyPaginatedEntites(entities, limit, 10);

    queryParam = "?limit=" + limit + "&fromid="
        + entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
    uri = URI.create(resourceUri + queryParam);
    resp = getResponse(client, uri);
    entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
    });
    // verify for entity-7 to entity-4 in descending order.
    entity = verifyPaginatedEntites(entities, limit, 7);

    queryParam = "?limit=" + limit + "&fromid="
        + entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
    uri = URI.create(resourceUri + queryParam);
    resp = getResponse(client, uri);
    entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
    });
    // verify for entity-4 to entity-1 in descending order.
    entity = verifyPaginatedEntites(entities, limit, 4);

    queryParam = "?limit=" + limit + "&fromid="
        + entity.getInfo().get(TimelineReaderUtils.FROMID_KEY);
    uri = URI.create(resourceUri + queryParam);
    resp = getResponse(client, uri);
    entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
    });
    // always entity-1 will be retrieved
    entity = verifyPaginatedEntites(entities, 1, 1);
  }

  private TimelineEntity verifyPaginatedEntites(List<TimelineEntity> entities,
      int limit, int startFrom) {
    assertNotNull(entities);
    assertEquals(limit, entities.size());
    TimelineEntity entity = null;
    for (TimelineEntity timelineEntity : entities) {
      assertEquals("entitytype", timelineEntity.getType());
      assertEquals("entityid-" + startFrom, timelineEntity.getId());
      assertEquals(11 - startFrom--, timelineEntity.getIdPrefix());
      entity = timelineEntity;
    }
    return entity;
  }

  private List<FlowActivityEntity> verifyFlowEntites(Client client, URI uri,
      int noOfEntities,
      int[] a, String[] flowsInSequence) throws Exception {
    ClientResponse resp = getResponse(client, uri);
    List<FlowActivityEntity> entities =
        resp.getEntity(new GenericType<List<FlowActivityEntity>>() {
        });
    assertNotNull(entities);
    assertEquals(noOfEntities, entities.size());
    assertEquals(noOfEntities, flowsInSequence.length);
    assertEquals(noOfEntities, a.length);
    int count = 0;
    for (FlowActivityEntity timelineEntity : entities) {
      assertEquals(flowsInSequence[count],
          timelineEntity.getInfo().get("SYSTEM_INFO_FLOW_NAME"));
      assertEquals(a[count++], timelineEntity.getFlowRuns().size());
    }
    return entities;
  }

  @Test
  public void testForFlowAppsPagination() throws Exception {
    Client client = createClient();
    try {
      // app entities stored is 15 during initialization.
      int totalAppEntities = 15;
      String resourceUri = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/users/user1/flows/flow1/apps";
      URI uri = URI.create(resourceUri);
      ClientResponse resp = getResponse(client, uri);
      List<TimelineEntity> entities =
          resp.getEntity(new GenericType<List<TimelineEntity>>() {
          });
      assertNotNull(entities);
      assertEquals(totalAppEntities, entities.size());
      TimelineEntity entity1 = entities.get(0);
      TimelineEntity entity15 = entities.get(totalAppEntities - 1);

      int limit = 10;
      String queryParam = "?limit=" + limit;
      uri = URI.create(resourceUri + queryParam);
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(limit, entities.size());
      assertEquals(entity1, entities.get(0));
      TimelineEntity entity10 = entities.get(limit - 1);

      uri =
          URI.create(resourceUri + queryParam + "&fromid="
              + entity10.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(6, entities.size());
      assertEquals(entity10, entities.get(0));
      assertEquals(entity15, entities.get(5));

    } finally {
      client.destroy();
    }
  }

  @Test
  public void testForFlowRunAppsPagination() throws Exception {
    Client client = createClient();
    try {
      // app entities stored is 15 during initialization.
      int totalAppEntities = 5;
      String resourceUri = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/users/user1/flows/flow1/runs/1/apps";
      URI uri = URI.create(resourceUri);
      ClientResponse resp = getResponse(client, uri);
      List<TimelineEntity> entities =
          resp.getEntity(new GenericType<List<TimelineEntity>>() {
          });
      assertNotNull(entities);
      assertEquals(totalAppEntities, entities.size());
      TimelineEntity entity1 = entities.get(0);
      TimelineEntity entity5 = entities.get(totalAppEntities - 1);

      int limit = 3;
      String queryParam = "?limit=" + limit;
      uri = URI.create(resourceUri + queryParam);
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(limit, entities.size());
      assertEquals(entity1, entities.get(0));
      TimelineEntity entity3 = entities.get(limit - 1);

      uri =
          URI.create(resourceUri + queryParam + "&fromid="
              + entity3.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(3, entities.size());
      assertEquals(entity3, entities.get(0));
      assertEquals(entity5, entities.get(2));

    } finally {
      client.destroy();
    }
  }

  @Test
  public void testForFlowRunsPagination() throws Exception {
    Client client = createClient();
    try {
      // app entities stored is 15 during initialization.
      int totalRuns = 3;
      String resourceUri = "http://localhost:" + getServerPort() + "/ws/v2/"
          + "timeline/clusters/cluster1/users/user1/flows/flow1/runs";
      URI uri = URI.create(resourceUri);
      ClientResponse resp = getResponse(client, uri);
      List<TimelineEntity> entities =
          resp.getEntity(new GenericType<List<TimelineEntity>>() {
          });
      assertNotNull(entities);
      assertEquals(totalRuns, entities.size());
      TimelineEntity entity1 = entities.get(0);
      TimelineEntity entity3 = entities.get(totalRuns - 1);

      int limit = 2;
      String queryParam = "?limit=" + limit;
      uri = URI.create(resourceUri + queryParam);
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(limit, entities.size());
      assertEquals(entity1, entities.get(0));
      TimelineEntity entity2 = entities.get(limit - 1);

      uri = URI.create(resourceUri + queryParam + "&fromid="
          + entity2.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(limit, entities.size());
      assertEquals(entity2, entities.get(0));
      assertEquals(entity3, entities.get(1));

      uri = URI.create(resourceUri + queryParam + "&fromid="
          + entity3.getInfo().get(TimelineReaderUtils.FROMID_KEY));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<List<TimelineEntity>>() {
      });
      assertNotNull(entities);
      assertEquals(1, entities.size());
      assertEquals(entity3, entities.get(0));
    } finally {
      client.destroy();
    }
  }

  @Test
  public void testGetAppsMetricsRange() throws Exception {
    Client client = createClient();
    try {
      URI uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 200000) + "&metricstimeend=" + (ts - 100000));
      ClientResponse resp = getResponse(client, uri);
      Set<TimelineEntity> entities =
          resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 4, 4);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps?fields=ALL&metricslimit=100");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 4, 10);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/" +
          "apps?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 200000) + "&metricstimeend=" + (ts - 100000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());
      verifyMetricsCount(entities, 5, 5);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/" +
          "apps?fields=ALL&metricslimit=100");
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(3, entities.size());
      verifyMetricsCount(entities, 5, 12);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 200000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 4, 10);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/runs/" +
          "1002345678919/apps?fields=ALL&metricslimit=100&metricstimeend=" +
          (ts - 100000));
      resp = getResponse(client, uri);
      entities = resp.getEntity(new GenericType<Set<TimelineEntity>>(){});
      assertNotNull(entities);
      assertEquals(2, entities.size());
      verifyMetricsCount(entities, 4, 4);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/apps/application_1111111111_1111?userid=user1&fields=ALL" +
          "&flowname=flow_name&flowrunid=1002345678919&metricslimit=100" +
          "&metricstimestart=" +(ts - 200000) + "&metricstimeend=" +
          (ts - 100000));
      resp = getResponse(client, uri);
      TimelineEntity entity = resp.getEntity(TimelineEntity.class);
      assertNotNull(entity);
      verifyMetricCount(entity, 3, 3);

      uri = URI.create("http://localhost:" + getServerPort() + "/ws/v2/" +
          "timeline/clusters/cluster1/users/user1/flows/flow_name/" +
          "apps?fields=ALL&metricslimit=100&metricstimestart=" +
          (ts - 100000) + "&metricstimeend=" + (ts - 200000));
      verifyHttpResponse(client, uri, Status.BAD_REQUEST);
    } finally {
      client.destroy();
    }
  }
}
