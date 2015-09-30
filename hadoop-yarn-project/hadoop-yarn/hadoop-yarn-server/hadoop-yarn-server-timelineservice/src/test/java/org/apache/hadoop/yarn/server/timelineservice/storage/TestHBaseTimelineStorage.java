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

package org.apache.hadoop.yarn.server.timelineservice.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineWriterUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Various tests to test writing entities to HBase and reading them back from
 * it.
 *
 * It uses a single HBase mini-cluster for all tests which is a little more
 * realistic, and helps test correctness in the presence of other data.
 *
 * Each test uses a different cluster name to be able to handle its own data
 * even if other records exist in the table. Use a different cluster name if
 * you add a new test.
 */
public class TestHBaseTimelineStorage {

  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    createSchema();
  }

  private static void createSchema() throws IOException {
    TimelineSchemaCreator.createAllTables(util.getConfiguration(), false);
  }

  @Test
  public void testWriteApplicationToHBase() throws Exception {
    TimelineEntities te = new TimelineEntities();
    ApplicationEntity entity = new ApplicationEntity();
    String appId = "application_1000178881110_2002";
    entity.setId(appId);
    long cTime = 1425016501000L;
    long mTime = 1425026901000L;
    entity.setCreatedTime(cTime);
    entity.setModifiedTime(mTime);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap = new HashMap<String, Object>();
    infoMap.put("infoMapKey1", "infoMapValue1");
    infoMap.put("infoMapKey2", 10);
    entity.addInfo(infoMap);

    // add the isRelatedToEntity info
    String key = "task";
    String value = "is_related_to_entity_id_here";
    Set<String> isRelatedToSet = new HashSet<String>();
    isRelatedToSet.add(value);
    Map<String, Set<String>> isRelatedTo = new HashMap<String, Set<String>>();
    isRelatedTo.put(key, isRelatedToSet);
    entity.setIsRelatedToEntities(isRelatedTo);

    // add the relatesTo info
    key = "container";
    value = "relates_to_entity_id_here";
    Set<String> relatesToSet = new HashSet<String>();
    relatesToSet.add(value);
    value = "relates_to_entity_id_here_Second";
    relatesToSet.add(value);
    Map<String, Set<String>> relatesTo = new HashMap<String, Set<String>>();
    relatesTo.put(key, relatesToSet);
    entity.setRelatesToEntities(relatesTo);

    // add some config entries
    Map<String, String> conf = new HashMap<String, String>();
    conf.put("config_param1", "value1");
    conf.put("config_param2", "value2");
    entity.addConfigs(conf);

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = System.currentTimeMillis();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity.addMetrics(metrics);

    te.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    HBaseTimelineReaderImpl hbr = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      String cluster = "cluster_test_write_app";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      hbi.write(cluster, user, flow, flowVersion, runid, appId, te);
      hbi.stop();

      // retrieve the row
      byte[] rowKey =
          ApplicationRowKey.getRowKey(cluster, user, flow, runid, appId);
      Get get = new Get(rowKey);
      get.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      Result result = new ApplicationTable().getResult(c1, conn, get);

      assertTrue(result != null);
      assertEquals(16, result.size());

      // check the row key
      byte[] row1 = result.getRow();
      assertTrue(isApplicationRowKeyCorrect(row1, cluster, user, flow, runid,
          appId));

      // check info column family
      String id1 = ApplicationColumn.ID.readResult(result).toString();
      assertEquals(appId, id1);

      Number val =
          (Number) ApplicationColumn.CREATED_TIME.readResult(result);
      long cTime1 = val.longValue();
      assertEquals(cTime1, cTime);

      val = (Number) ApplicationColumn.MODIFIED_TIME.readResult(result);
      long mTime1 = val.longValue();
      assertEquals(mTime1, mTime);

      Map<String, Object> infoColumns =
          ApplicationColumnPrefix.INFO.readResults(result);
      assertEquals(infoMap, infoColumns);

      // Remember isRelatedTo is of type Map<String, Set<String>>
      for (String isRelatedToKey : isRelatedTo.keySet()) {
        Object isRelatedToValue =
            ApplicationColumnPrefix.IS_RELATED_TO.readResult(result,
                isRelatedToKey);
        String compoundValue = isRelatedToValue.toString();
        // id7?id9?id6
        Set<String> isRelatedToValues =
            new HashSet<String>(Separator.VALUES.splitEncoded(compoundValue));
        assertEquals(isRelatedTo.get(isRelatedToKey).size(),
            isRelatedToValues.size());
        for (String v : isRelatedTo.get(isRelatedToKey)) {
          assertTrue(isRelatedToValues.contains(v));
        }
      }

      // RelatesTo
      for (String relatesToKey : relatesTo.keySet()) {
        String compoundValue =
            ApplicationColumnPrefix.RELATES_TO.readResult(result,
                relatesToKey).toString();
        // id3?id4?id5
        Set<String> relatesToValues =
            new HashSet<String>(Separator.VALUES.splitEncoded(compoundValue));
        assertEquals(relatesTo.get(relatesToKey).size(),
            relatesToValues.size());
        for (String v : relatesTo.get(relatesToKey)) {
          assertTrue(relatesToValues.contains(v));
        }
      }

      // Configuration
      Map<String, Object> configColumns =
          ApplicationColumnPrefix.CONFIG.readResults(result);
      assertEquals(conf, configColumns);

      NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
          ApplicationColumnPrefix.METRIC.readResultsWithTimestamps(result);

      NavigableMap<Long, Number> metricMap = metricsResult.get(m1.getId());
      assertEquals(metricValues, metricMap);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = hbr.getEntity(user, cluster, flow, runid, appId,
          entity.getType(), entity.getId(),
          EnumSet.of(TimelineReader.Field.ALL));
      assertNotNull(e1);

      // verify attributes
      assertEquals(appId, e1.getId());
      assertEquals(TimelineEntityType.YARN_APPLICATION.toString(),
          e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      assertEquals(mTime, e1.getModifiedTime());
      Map<String, Object> infoMap2 = e1.getInfo();
      assertEquals(infoMap, infoMap2);

      Map<String, Set<String>> isRelatedTo2 = e1.getIsRelatedToEntities();
      assertEquals(isRelatedTo, isRelatedTo2);

      Map<String, Set<String>> relatesTo2 = e1.getRelatesToEntities();
      assertEquals(relatesTo, relatesTo2);

      Map<String, String> conf2 = e1.getConfigs();
      assertEquals(conf, conf2);

      Set<TimelineMetric> metrics2 = e1.getMetrics();
      assertEquals(metrics, metrics2);
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        assertEquals(metricValues, metricValues2);
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
      if (hbr != null) {
        hbr.stop();
        hbr.close();
      }
    }
  }

  @Test
  public void testWriteEntityToHBase() throws Exception {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "hello";
    String type = "world";
    entity.setId(id);
    entity.setType(type);
    long cTime = 1425016501000L;
    long mTime = 1425026901000L;
    entity.setCreatedTime(cTime);
    entity.setModifiedTime(mTime);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap = new HashMap<String, Object>();
    infoMap.put("infoMapKey1", "infoMapValue1");
    infoMap.put("infoMapKey2", 10);
    entity.addInfo(infoMap);

    // add the isRelatedToEntity info
    String key = "task";
    String value = "is_related_to_entity_id_here";
    Set<String> isRelatedToSet = new HashSet<String>();
    isRelatedToSet.add(value);
    Map<String, Set<String>> isRelatedTo = new HashMap<String, Set<String>>();
    isRelatedTo.put(key, isRelatedToSet);
    entity.setIsRelatedToEntities(isRelatedTo);

    // add the relatesTo info
    key = "container";
    value = "relates_to_entity_id_here";
    Set<String> relatesToSet = new HashSet<String>();
    relatesToSet.add(value);
    value = "relates_to_entity_id_here_Second";
    relatesToSet.add(value);
    Map<String, Set<String>> relatesTo = new HashMap<String, Set<String>>();
    relatesTo.put(key, relatesToSet);
    entity.setRelatesToEntities(relatesTo);

    // add some config entries
    Map<String, String> conf = new HashMap<String, String>();
    conf.put("config_param1", "value1");
    conf.put("config_param2", "value2");
    entity.addConfigs(conf);

    // add metrics
    Set<TimelineMetric> metrics = new HashSet<>();
    TimelineMetric m1 = new TimelineMetric();
    m1.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricValues = new HashMap<Long, Number>();
    long ts = System.currentTimeMillis();
    metricValues.put(ts - 120000, 100000000);
    metricValues.put(ts - 100000, 200000000);
    metricValues.put(ts - 80000, 300000000);
    metricValues.put(ts - 60000, 400000000);
    metricValues.put(ts - 40000, 50000000000L);
    metricValues.put(ts - 20000, 60000000000L);
    m1.setType(Type.TIME_SERIES);
    m1.setValues(metricValues);
    metrics.add(m1);
    entity.addMetrics(metrics);

    te.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    HBaseTimelineReaderImpl hbr = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      String cluster = "cluster_test_write_entity";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = "some app name";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.stop();

      // scan the table and see that entity exists
      Scan s = new Scan();
      byte[] startRow =
          EntityRowKey.getRowKeyPrefix(cluster, user, flow, runid, appName);
      s.setStartRow(startRow);
      s.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      ResultScanner scanner = new EntityTable().getResultScanner(c1, conn, s);

      int rowCount = 0;
      int colCount = 0;
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          byte[] row1 = result.getRow();
          assertTrue(isRowKeyCorrect(row1, cluster, user, flow, runid, appName,
              entity));

          // check info column family
          String id1 = EntityColumn.ID.readResult(result).toString();
          assertEquals(id, id1);

          String type1 = EntityColumn.TYPE.readResult(result).toString();
          assertEquals(type, type1);

          Number val = (Number) EntityColumn.CREATED_TIME.readResult(result);
          long cTime1 = val.longValue();
          assertEquals(cTime1, cTime);

          val = (Number) EntityColumn.MODIFIED_TIME.readResult(result);
          long mTime1 = val.longValue();
          assertEquals(mTime1, mTime);

          Map<String, Object> infoColumns =
              EntityColumnPrefix.INFO.readResults(result);
          assertEquals(infoMap, infoColumns);

          // Remember isRelatedTo is of type Map<String, Set<String>>
          for (String isRelatedToKey : isRelatedTo.keySet()) {
            Object isRelatedToValue =
                EntityColumnPrefix.IS_RELATED_TO.readResult(result,
                    isRelatedToKey);
            String compoundValue = isRelatedToValue.toString();
            // id7?id9?id6
            Set<String> isRelatedToValues =
                new HashSet<String>(
                    Separator.VALUES.splitEncoded(compoundValue));
            assertEquals(isRelatedTo.get(isRelatedToKey).size(),
                isRelatedToValues.size());
            for (String v : isRelatedTo.get(isRelatedToKey)) {
              assertTrue(isRelatedToValues.contains(v));
            }
          }

          // RelatesTo
          for (String relatesToKey : relatesTo.keySet()) {
            String compoundValue =
                EntityColumnPrefix.RELATES_TO.readResult(result, relatesToKey)
                    .toString();
            // id3?id4?id5
            Set<String> relatesToValues =
                new HashSet<String>(
                    Separator.VALUES.splitEncoded(compoundValue));
            assertEquals(relatesTo.get(relatesToKey).size(),
                relatesToValues.size());
            for (String v : relatesTo.get(relatesToKey)) {
              assertTrue(relatesToValues.contains(v));
            }
          }

          // Configuration
          Map<String, Object> configColumns =
              EntityColumnPrefix.CONFIG.readResults(result);
          assertEquals(conf, configColumns);

          NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
              EntityColumnPrefix.METRIC.readResultsWithTimestamps(result);

          NavigableMap<Long, Number> metricMap = metricsResult.get(m1.getId());
          assertEquals(metricValues, metricMap);
        }
      }
      assertEquals(1, rowCount);
      assertEquals(17, colCount);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = hbr.getEntity(user, cluster, flow, runid, appName,
          entity.getType(), entity.getId(),
          EnumSet.of(TimelineReader.Field.ALL));
      Set<TimelineEntity> es1 = hbr.getEntities(user, cluster, flow, runid,
          appName, entity.getType(), null, null, null, null, null, null, null,
          null, null, null, null, EnumSet.of(TimelineReader.Field.ALL));
      assertNotNull(e1);
      assertEquals(1, es1.size());

      // verify attributes
      assertEquals(id, e1.getId());
      assertEquals(type, e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      assertEquals(mTime, e1.getModifiedTime());
      Map<String, Object> infoMap2 = e1.getInfo();
      assertEquals(infoMap, infoMap2);

      Map<String, Set<String>> isRelatedTo2 = e1.getIsRelatedToEntities();
      assertEquals(isRelatedTo, isRelatedTo2);

      Map<String, Set<String>> relatesTo2 = e1.getRelatesToEntities();
      assertEquals(relatesTo, relatesTo2);

      Map<String, String> conf2 = e1.getConfigs();
      assertEquals(conf, conf2);

      Set<TimelineMetric> metrics2 = e1.getMetrics();
      assertEquals(metrics, metrics2);
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        assertEquals(metricValues, metricValues2);
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
      if (hbr != null) {
        hbr.stop();
        hbr.close();
      }
    }
  }

  private boolean isRowKeyCorrect(byte[] rowKey, String cluster, String user,
      String flow, long runid, String appName, TimelineEntity te) {

    EntityRowKey key = EntityRowKey.parseRowKey(rowKey);

    assertEquals(user, key.getUserId());
    assertEquals(cluster, key.getClusterId());
    assertEquals(flow, key.getFlowId());
    assertEquals(runid, key.getFlowRunId());
    assertEquals(appName, key.getAppId());
    assertEquals(te.getType(), key.getEntityType());
    assertEquals(te.getId(), key.getEntityId());
    return true;
  }

  private boolean isApplicationRowKeyCorrect(byte[] rowKey, String cluster,
      String user, String flow, long runid, String appName) {

    ApplicationRowKey key = ApplicationRowKey.parseRowKey(rowKey);

    assertEquals(cluster, key.getClusterId());
    assertEquals(user, key.getUserId());
    assertEquals(flow, key.getFlowId());
    assertEquals(runid, key.getFlowRunId());
    assertEquals(appName, key.getAppId());
    return true;
  }

  @Test
  public void testEvents() throws IOException {
    TimelineEvent event = new TimelineEvent();
    String eventId = ApplicationMetricsConstants.CREATED_EVENT_TYPE;
    event.setId(eventId);
    long expTs = 1436512802000L;
    event.setTimestamp(expTs);
    String expKey = "foo_event";
    Object expVal = "test";
    event.addInfo(expKey, expVal);

    final TimelineEntity entity = new ApplicationEntity();
    entity.setId(ApplicationId.newInstance(0, 1).toString());
    entity.addEvent(event);

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    HBaseTimelineReaderImpl hbr = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      String cluster = "cluster_test_events";
      String user = "user2";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName = "application_123465899910_1001";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, entities);
      hbi.stop();

      // retrieve the row
      byte[] rowKey =
          ApplicationRowKey.getRowKey(cluster, user, flow, runid, appName);
      Get get = new Get(rowKey);
      get.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      Result result = new ApplicationTable().getResult(c1, conn, get);

      assertTrue(result != null);

      // check the row key
      byte[] row1 = result.getRow();
      assertTrue(isApplicationRowKeyCorrect(row1, cluster, user, flow, runid,
          appName));

      Map<?, Object> eventsResult =
          ApplicationColumnPrefix.EVENT.
              readResultsHavingCompoundColumnQualifiers(result);
      // there should be only one event
      assertEquals(1, eventsResult.size());
      for (Map.Entry<?, Object> e : eventsResult.entrySet()) {
        // the qualifier is a compound key
        // hence match individual values
        byte[][] karr = (byte[][])e.getKey();
        assertEquals(3, karr.length);
        assertEquals(eventId, Bytes.toString(karr[0]));
        assertEquals(TimelineWriterUtils.invert(expTs), Bytes.toLong(karr[1]));
        assertEquals(expKey, Bytes.toString(karr[2]));
        Object value = e.getValue();
        // there should be only one timestamp and value
        assertEquals(expVal, value.toString());
      }

      // read the timeline entity using the reader this time
      TimelineEntity e1 = hbr.getEntity(user, cluster, flow, runid, appName,
          entity.getType(), entity.getId(),
          EnumSet.of(TimelineReader.Field.ALL));
      TimelineEntity e2 = hbr.getEntity(user, cluster, null, null, appName,
          entity.getType(), entity.getId(),
          EnumSet.of(TimelineReader.Field.ALL));
      assertNotNull(e1);
      assertNotNull(e2);
      assertEquals(e1, e2);

      // check the events
      NavigableSet<TimelineEvent> events = e1.getEvents();
      // there should be only one event
      assertEquals(1, events.size());
      for (TimelineEvent e : events) {
        assertEquals(eventId, e.getId());
        assertEquals(expTs, e.getTimestamp());
        Map<String,Object> info = e.getInfo();
        assertEquals(1, info.size());
        for (Map.Entry<String, Object> infoEntry : info.entrySet()) {
          assertEquals(expKey, infoEntry.getKey());
          assertEquals(expVal, infoEntry.getValue());
        }
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
      if (hbr != null) {
        hbr.stop();
        hbr.close();
      }
    }
  }

  @Test
  public void testEventsWithEmptyInfo() throws IOException {
    TimelineEvent event = new TimelineEvent();
    String eventId = "foo_event_id";
    event.setId(eventId);
    long expTs = 1436512802000L;
    event.setTimestamp(expTs);

    final TimelineEntity entity = new TimelineEntity();
    entity.setId("attempt_1329348432655_0001_m_000008_18");
    entity.setType("FOO_ATTEMPT");
    entity.addEvent(event);

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    HBaseTimelineReaderImpl hbr = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      String cluster = "cluster_test_empty_eventkey";
      String user = "user_emptyeventkey";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName = "some app name";
      byte[] startRow =
          EntityRowKey.getRowKeyPrefix(cluster, user, flow, runid, appName);
      hbi.write(cluster, user, flow, flowVersion, runid, appName, entities);
      hbi.stop();
      // scan the table and see that entity exists
      Scan s = new Scan();
      s.setStartRow(startRow);
      s.addFamily(EntityColumnFamily.INFO.getBytes());
      Connection conn = ConnectionFactory.createConnection(c1);
      ResultScanner scanner = new EntityTable().getResultScanner(c1, conn, s);

      int rowCount = 0;
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;

          // check the row key
          byte[] row1 = result.getRow();
          assertTrue(isRowKeyCorrect(row1, cluster, user, flow, runid, appName,
              entity));

          Map<?, Object> eventsResult =
              EntityColumnPrefix.EVENT.
                  readResultsHavingCompoundColumnQualifiers(result);
          // there should be only one event
          assertEquals(1, eventsResult.size());
          for (Map.Entry<?, Object> e : eventsResult.entrySet()) {
            // the qualifier is a compound key
            // hence match individual values
            byte[][] karr = (byte[][])e.getKey();
            assertEquals(3, karr.length);
            assertEquals(eventId, Bytes.toString(karr[0]));
            assertEquals(TimelineWriterUtils.invert(expTs),
                Bytes.toLong(karr[1]));
            // key must be empty
            assertEquals(0, karr[2].length);
            Object value = e.getValue();
            // value should be empty
            assertEquals("", value.toString());
          }
        }
      }
      assertEquals(1, rowCount);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = hbr.getEntity(user, cluster, flow, runid, appName,
          entity.getType(), entity.getId(),
          EnumSet.of(TimelineReader.Field.ALL));
      Set<TimelineEntity> es1 = hbr.getEntities(user, cluster, flow, runid,
          appName, entity.getType(), null, null, null, null, null, null, null,
          null, null, null, null, EnumSet.of(TimelineReader.Field.ALL));
      assertNotNull(e1);
      assertEquals(1, es1.size());

      // check the events
      NavigableSet<TimelineEvent> events = e1.getEvents();
      // there should be only one event
      assertEquals(1, events.size());
      for (TimelineEvent e : events) {
        assertEquals(eventId, e.getId());
        assertEquals(expTs, e.getTimestamp());
        Map<String,Object> info = e.getInfo();
        assertTrue(info == null || info.isEmpty());
      }
    } finally {
      hbi.stop();
      hbi.close();
      hbr.stop();;
      hbr.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
