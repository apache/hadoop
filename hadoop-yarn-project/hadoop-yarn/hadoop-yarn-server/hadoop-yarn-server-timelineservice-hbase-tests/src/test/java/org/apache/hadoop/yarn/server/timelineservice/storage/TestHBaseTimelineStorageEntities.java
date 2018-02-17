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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnRWHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnNameConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationRowKeyPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.subapplication.SubApplicationTableRW;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
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
public class TestHBaseTimelineStorageEntities {

  private static HBaseTestingUtility util;
  private HBaseTimelineReaderImpl reader;
  private static final long CURRENT_TIME = System.currentTimeMillis();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    DataGeneratorForTest.createSchema(util.getConfiguration());
    DataGeneratorForTest.loadEntities(util, CURRENT_TIME);
  }

  @Before
  public void init() throws Exception {
    reader = new HBaseTimelineReaderImpl();
    reader.init(util.getConfiguration());
    reader.start();
  }

  @After
  public void stop() throws Exception {
    if (reader != null) {
      reader.stop();
      reader.close();
    }
  }

  private static void matchMetrics(Map<Long, Number> m1, Map<Long, Number> m2) {
    assertEquals(m1.size(), m2.size());
    for (Map.Entry<Long, Number> entry : m2.entrySet()) {
      Number val = m1.get(entry.getKey());
      assertNotNull(val);
      assertEquals(val.longValue(), entry.getValue().longValue());
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
    Long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

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
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_write_entity";
      String user = "user1";
      String subAppUser = "subAppUser1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = HBaseTimelineSchemaUtils.convertApplicationIdToString(
          ApplicationId.newInstance(System.currentTimeMillis() + 9000000L, 1)
      );
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te,
          UserGroupInformation.createRemoteUser(subAppUser));
      hbi.stop();

      // scan the table and see that entity exists
      Scan s = new Scan();
      byte[] startRow =
          new EntityRowKeyPrefix(cluster, user, flow, runid, appName)
              .getRowKeyPrefix();
      s.setStartRow(startRow);
      s.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      ResultScanner scanner = new EntityTableRW().getResultScanner(c1, conn, s);

      int rowCount = 0;
      int colCount = 0;
      KeyConverter<String> stringKeyConverter = new StringKeyConverter();
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          byte[] row1 = result.getRow();
          assertTrue(isRowKeyCorrect(row1, cluster, user, flow, runid, appName,
              entity));

          // check info column family
          String id1 =
              ColumnRWHelper.readResult(result, EntityColumn.ID).toString();
          assertEquals(id, id1);

          String type1 =
              ColumnRWHelper.readResult(result, EntityColumn.TYPE).toString();
          assertEquals(type, type1);

          Long cTime1 = (Long)
              ColumnRWHelper.readResult(result, EntityColumn.CREATED_TIME);
          assertEquals(cTime1, cTime);

          Map<String, Object> infoColumns = ColumnRWHelper.readResults(
              result, EntityColumnPrefix.INFO, new StringKeyConverter());
          assertEquals(infoMap, infoColumns);

          // Remember isRelatedTo is of type Map<String, Set<String>>
          for (Map.Entry<String, Set<String>> isRelatedToEntry : isRelatedTo
              .entrySet()) {
            Object isRelatedToValue = ColumnRWHelper.readResult(result,
                EntityColumnPrefix.IS_RELATED_TO, isRelatedToEntry.getKey());
            String compoundValue = isRelatedToValue.toString();
            // id7?id9?id6
            Set<String> isRelatedToValues =
                new HashSet<String>(
                    Separator.VALUES.splitEncoded(compoundValue));
            assertEquals(isRelatedTo.get(isRelatedToEntry.getKey()).size(),
                isRelatedToValues.size());
            for (String v : isRelatedToEntry.getValue()) {
              assertTrue(isRelatedToValues.contains(v));
            }
          }

          // RelatesTo
          for (Map.Entry<String, Set<String>> relatesToEntry : relatesTo
              .entrySet()) {
            String compoundValue = ColumnRWHelper.readResult(result,
                EntityColumnPrefix.RELATES_TO, relatesToEntry.getKey())
                .toString();
            // id3?id4?id5
            Set<String> relatesToValues =
                new HashSet<String>(
                    Separator.VALUES.splitEncoded(compoundValue));
            assertEquals(relatesTo.get(relatesToEntry.getKey()).size(),
                relatesToValues.size());
            for (String v : relatesToEntry.getValue()) {
              assertTrue(relatesToValues.contains(v));
            }
          }

          // Configuration
          Map<String, Object> configColumns = ColumnRWHelper.readResults(
              result, EntityColumnPrefix.CONFIG, stringKeyConverter);
          assertEquals(conf, configColumns);

          NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
              ColumnRWHelper.readResultsWithTimestamps(
                  result, EntityColumnPrefix.METRIC, stringKeyConverter);

          NavigableMap<Long, Number> metricMap = metricsResult.get(m1.getId());
          matchMetrics(metricValues, metricMap);
        }
      }
      assertEquals(1, rowCount);
      assertEquals(16, colCount);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL),
          Integer.MAX_VALUE, null, null));
      Set<TimelineEntity> es1 = reader.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), null),
          new TimelineEntityFilters.Builder().build(),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL),
          Integer.MAX_VALUE, null, null));
      assertNotNull(e1);
      assertEquals(1, es1.size());

      // verify attributes
      assertEquals(id, e1.getId());
      assertEquals(type, e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      Map<String, Object> infoMap2 = e1.getInfo();
      // fromid key is added by storage. Remove it for comparison.
      infoMap2.remove("FROM_ID");
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
        matchMetrics(metricValues, metricValues2);
      }

      e1 = reader.getEntity(new TimelineReaderContext(cluster, user, flow,
          runid, appName, entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
          null, null));
      assertNotNull(e1);
      assertEquals(id, e1.getId());
      assertEquals(type, e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      infoMap2 = e1.getInfo();
      // fromid key is added by storage. Remove it for comparision.
      infoMap2.remove("FROM_ID");
      assertEquals(infoMap, infoMap2);
      assertEquals(isRelatedTo, e1.getIsRelatedToEntities());
      assertEquals(relatesTo, e1.getRelatesToEntities());
      assertEquals(conf, e1.getConfigs());
      for (TimelineMetric metric : e1.getMetrics()) {
        assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
        assertEquals(1, metric.getValues().size());
        assertTrue(metric.getValues().containsKey(ts - 20000));
        assertEquals(metricValues.get(ts - 20000),
            metric.getValues().get(ts - 20000));
      }

      // verify for sub application table entities.
      verifySubApplicationTableEntities(cluster, user, flow, flowVersion, runid,
          appName, subAppUser, c1, entity, id, type, infoMap, isRelatedTo,
          relatesTo, conf, metricValues, metrics, cTime, m1);
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  private void verifySubApplicationTableEntities(String cluster, String user,
      String flow, String flowVersion, Long runid, String appName,
      String subAppUser, Configuration c1, TimelineEntity entity, String id,
      String type, Map<String, Object> infoMap,
      Map<String, Set<String>> isRelatedTo, Map<String, Set<String>> relatesTo,
      Map<String, String> conf, Map<Long, Number> metricValues,
      Set<TimelineMetric> metrics, Long cTime, TimelineMetric m1)
      throws IOException {
    Scan s = new Scan();
    // read from SubApplicationTableRW
    byte[] startRow = new SubApplicationRowKeyPrefix(cluster, subAppUser, null,
        null, null, null).getRowKeyPrefix();
    s.setStartRow(startRow);
    s.setMaxVersions(Integer.MAX_VALUE);
    Connection conn = ConnectionFactory.createConnection(c1);
    ResultScanner scanner =
        new SubApplicationTableRW().getResultScanner(c1, conn, s);

    int rowCount = 0;
    int colCount = 0;
    KeyConverter<String> stringKeyConverter = new StringKeyConverter();
    for (Result result : scanner) {
      if (result != null && !result.isEmpty()) {
        rowCount++;
        colCount += result.size();
        byte[] row1 = result.getRow();
        assertTrue(verifyRowKeyForSubApplication(row1, subAppUser, cluster,
            user, entity));

        // check info column family
        String id1 = ColumnRWHelper.readResult(result, SubApplicationColumn.ID)
            .toString();
        assertEquals(id, id1);

        String type1 = ColumnRWHelper.readResult(result,
            SubApplicationColumn.TYPE).toString();
        assertEquals(type, type1);

        Long cTime1 = (Long) ColumnRWHelper.readResult(result,
            SubApplicationColumn.CREATED_TIME);
        assertEquals(cTime1, cTime);

        Map<String, Object> infoColumns = ColumnRWHelper.readResults(
            result, SubApplicationColumnPrefix.INFO, new StringKeyConverter());
        assertEquals(infoMap, infoColumns);

        // Remember isRelatedTo is of type Map<String, Set<String>>
        for (Map.Entry<String, Set<String>> isRelatedToEntry : isRelatedTo
            .entrySet()) {
          Object isRelatedToValue = ColumnRWHelper.readResult(
              result, SubApplicationColumnPrefix.IS_RELATED_TO,
              isRelatedToEntry.getKey());
          String compoundValue = isRelatedToValue.toString();
          // id7?id9?id6
          Set<String> isRelatedToValues =
              new HashSet<String>(Separator.VALUES.splitEncoded(compoundValue));
          assertEquals(isRelatedTo.get(isRelatedToEntry.getKey()).size(),
              isRelatedToValues.size());
          for (String v : isRelatedToEntry.getValue()) {
            assertTrue(isRelatedToValues.contains(v));
          }
        }

        // RelatesTo
        for (Map.Entry<String, Set<String>> relatesToEntry : relatesTo
            .entrySet()) {
          String compoundValue = ColumnRWHelper.readResult(result,
              SubApplicationColumnPrefix.RELATES_TO, relatesToEntry.getKey())
              .toString();
          // id3?id4?id5
          Set<String> relatesToValues =
              new HashSet<String>(Separator.VALUES.splitEncoded(compoundValue));
          assertEquals(relatesTo.get(relatesToEntry.getKey()).size(),
              relatesToValues.size());
          for (String v : relatesToEntry.getValue()) {
            assertTrue(relatesToValues.contains(v));
          }
        }

        // Configuration
        Map<String, Object> configColumns = ColumnRWHelper.readResults(
            result, SubApplicationColumnPrefix.CONFIG, stringKeyConverter);
        assertEquals(conf, configColumns);

        NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
            ColumnRWHelper.readResultsWithTimestamps(
                result, SubApplicationColumnPrefix.METRIC, stringKeyConverter);

        NavigableMap<Long, Number> metricMap = metricsResult.get(m1.getId());
        matchMetrics(metricValues, metricMap);
      }
    }
    assertEquals(1, rowCount);
    assertEquals(16, colCount);
  }

  private boolean isRowKeyCorrect(byte[] rowKey, String cluster, String user,
      String flow, Long runid, String appName, TimelineEntity te) {

    EntityRowKey key = EntityRowKey.parseRowKey(rowKey);

    assertEquals(user, key.getUserId());
    assertEquals(cluster, key.getClusterId());
    assertEquals(flow, key.getFlowName());
    assertEquals(runid, key.getFlowRunId());
    assertEquals(appName, key.getAppId());
    assertEquals(te.getType(), key.getEntityType());
    assertEquals(te.getId(), key.getEntityId());
    return true;
  }

  @Test
  public void testEventsWithEmptyInfo() throws IOException {
    TimelineEvent event = new TimelineEvent();
    String eventId = "foo_ev e  nt_id";
    event.setId(eventId);
    Long expTs = 1436512802000L;
    event.setTimestamp(expTs);

    final TimelineEntity entity = new TimelineEntity();
    entity.setId("attempt_1329348432655_0001_m_000008_18");
    entity.setType("FOO_ATTEMPT");
    entity.addEvent(event);

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_empty_eventkey";
      String user = "user_emptyeventkey";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName = HBaseTimelineSchemaUtils.convertApplicationIdToString(
          ApplicationId.newInstance(System.currentTimeMillis() + 9000000L, 1));
      byte[] startRow =
          new EntityRowKeyPrefix(cluster, user, flow, runid, appName)
              .getRowKeyPrefix();
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), entities,
          UserGroupInformation.createRemoteUser(user));
      hbi.stop();
      // scan the table and see that entity exists
      Scan s = new Scan();
      s.setStartRow(startRow);
      s.addFamily(EntityColumnFamily.INFO.getBytes());
      Connection conn = ConnectionFactory.createConnection(c1);
      ResultScanner scanner = new EntityTableRW().getResultScanner(c1, conn, s);

      int rowCount = 0;
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;

          // check the row key
          byte[] row1 = result.getRow();
          assertTrue(isRowKeyCorrect(row1, cluster, user, flow, runid, appName,
              entity));

          Map<EventColumnName, Object> eventsResult =
              ColumnRWHelper.readResults(result,
                  EntityColumnPrefix.EVENT, new EventColumnNameConverter());
          // there should be only one event
          assertEquals(1, eventsResult.size());
          for (Map.Entry<EventColumnName, Object> e : eventsResult.entrySet()) {
            EventColumnName eventColumnName = e.getKey();
            // the qualifier is a compound key
            // hence match individual values
            assertEquals(eventId, eventColumnName.getId());
            assertEquals(expTs, eventColumnName.getTimestamp());
            // key must be empty
            assertNull(eventColumnName.getInfoKey());
            Object value = e.getValue();
            // value should be empty
            assertEquals("", value.toString());
          }
        }
      }
      assertEquals(1, rowCount);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
          null, null));
      Set<TimelineEntity> es1 = reader.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), null),
          new TimelineEntityFilters.Builder().build(),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
          null, null));
      assertNotNull(e1);
      assertEquals(1, es1.size());

      // check the events
      NavigableSet<TimelineEvent> events = e1.getEvents();
      // there should be only one event
      assertEquals(1, events.size());
      for (TimelineEvent e : events) {
        assertEquals(eventId, e.getId());
        assertEquals(expTs, Long.valueOf(e.getTimestamp()));
        Map<String, Object> info = e.getInfo();
        assertTrue(info == null || info.isEmpty());
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  @Test
  public void testEventsEscapeTs() throws IOException {
    TimelineEvent event = new TimelineEvent();
    String eventId = ApplicationMetricsConstants.CREATED_EVENT_TYPE;
    event.setId(eventId);
    long expTs = 1463567041056L;
    event.setTimestamp(expTs);
    String expKey = "f==o o_e ve\tnt";
    Object expVal = "test";
    event.addInfo(expKey, expVal);

    final TimelineEntity entity = new ApplicationEntity();
    entity.setId(
        HBaseTimelineSchemaUtils.convertApplicationIdToString(
            ApplicationId.newInstance(0, 1)));
    entity.addEvent(event);

    TimelineEntities entities = new TimelineEntities();
    entities.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);
      hbi.start();
      String cluster = "clus!ter_\ttest_ev  ents";
      String user = "user2";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName = "application_123465899910_2001";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), entities,
          UserGroupInformation.createRemoteUser(user));
      hbi.stop();

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
          null, null));
      assertNotNull(e1);
      // check the events
      NavigableSet<TimelineEvent> events = e1.getEvents();
      // there should be only one event
      assertEquals(1, events.size());
      for (TimelineEvent e : events) {
        assertEquals(eventId, e.getId());
        assertEquals(expTs, e.getTimestamp());
        Map<String, Object> info = e.getInfo();
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
    }
  }

  @Test
  public void testReadEntities() throws Exception {
    TimelineEntity entity = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertNotNull(entity);
    assertEquals(3, entity.getConfigs().size());
    assertEquals(1, entity.getIsRelatedToEntities().size());
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world",
        null), new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(3, entities.size());
    int cfgCnt = 0;
    int metricCnt = 0;
    int infoCnt = 0;
    int eventCnt = 0;
    int relatesToCnt = 0;
    int isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      cfgCnt += (timelineEntity.getConfigs() == null) ? 0 :
          timelineEntity.getConfigs().size();
      metricCnt += (timelineEntity.getMetrics() == null) ? 0 :
          timelineEntity.getMetrics().size();
      infoCnt += (timelineEntity.getInfo() == null) ? 0 :
          timelineEntity.getInfo().size();
      eventCnt += (timelineEntity.getEvents() == null) ? 0 :
          timelineEntity.getEvents().size();
      relatesToCnt += (timelineEntity.getRelatesToEntities() == null) ? 0 :
          timelineEntity.getRelatesToEntities().size();
      isRelatedToCnt += (timelineEntity.getIsRelatedToEntities() == null) ? 0 :
          timelineEntity.getIsRelatedToEntities().size();
    }
    assertEquals(5, cfgCnt);
    assertEquals(3, metricCnt);
    assertEquals(8, infoCnt);
    assertEquals(4, eventCnt);
    assertEquals(4, relatesToCnt);
    assertEquals(4, isRelatedToCnt);
  }

  @Test
  public void testFilterEntitiesByCreatedTime() throws Exception {
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().createdTimeBegin(1425016502000L)
            .createTimeEnd(1425016502040L).build(),
        new TimelineDataToRetrieve());
    assertEquals(3, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("hello") && !entity.getId().equals("hello1") &&
          !entity.getId().equals("hello2")) {
        Assert.fail("Entities with ids' hello, hello1 and hello2 should be" +
            " present");
      }
    }
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().createdTimeBegin(1425016502015L)
            .build(),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("hello1") &&
          !entity.getId().equals("hello2")) {
        Assert.fail("Entities with ids' hello1 and hello2 should be present");
      }
    }
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world",  null),
        new TimelineEntityFilters.Builder().createTimeEnd(1425016502015L)
            .build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("hello")) {
        Assert.fail("Entity with id hello should be present");
      }
    }
  }

  @Test
  public void testReadEntitiesRelationsAndEventFiltersDefaultView()
      throws Exception {
    TimelineFilterList eventFilter = new TimelineFilterList();
    eventFilter.addFilter(new TimelineExistsFilter(TimelineCompareOp.NOT_EQUAL,
        "end_event"));
    TimelineFilterList relatesTo = new TimelineFilterList(Operator.OR);
    relatesTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container2",
        new HashSet<Object>(Arrays.asList("relatesto7"))));
    relatesTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    TimelineFilterList isRelatedTo = new TimelineFilterList();
    isRelatedTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3"))));
    isRelatedTo.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.NOT_EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto5"))));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(relatesTo)
            .isRelatedTo(isRelatedTo).eventFilters(eventFilter).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    int eventCnt = 0;
    int isRelatedToCnt = 0;
    int relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity id should have been hello2");
      }
    }
    assertEquals(0, eventCnt);
    assertEquals(0, isRelatedToCnt);
    assertEquals(0, relatesToCnt);
  }

  @Test
  public void testReadEntitiesEventFilters() throws Exception {
    TimelineFilterList ef = new TimelineFilterList();
    ef.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "update_event"));
    ef.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef).build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(1, entities.size());
    int eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity id should have been hello2");
      }
    }
    assertEquals(1, eventCnt);

    TimelineFilterList ef1 = new TimelineFilterList();
    ef1.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "update_event"));
    ef1.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef1).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity id should have been hello2");
      }
    }
    assertEquals(0, eventCnt);

    TimelineFilterList ef2 = new TimelineFilterList();
    ef2.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef2).build(),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("hello") &&
          !timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity ids' should have been hello and hello2");
      }
    }
    assertEquals(0, eventCnt);

    TimelineFilterList ef3 = new TimelineFilterList();
    ef3.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "update_event"));
    ef3.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "dummy_event"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef3).build(),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "update_event"));
    list1.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "dummy_event"));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "start_event"));
    TimelineFilterList ef4 = new TimelineFilterList(Operator.OR, list1, list2);
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef4).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("hello")) {
        Assert.fail("Entity id should have been hello");
      }
    }
    assertEquals(0, eventCnt);

    TimelineFilterList ef5 = new TimelineFilterList();
    ef5.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "update_event"));
    ef5.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().eventFilters(ef5).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("hello")) {
        Assert.fail("Entity id should have been hello");
      }
    }
    assertEquals(0, eventCnt);
  }

  @Test
  public void testReadEntitiesIsRelatedTo() throws Exception {
    TimelineFilterList irt = new TimelineFilterList(Operator.OR);
    irt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task",
        new HashSet<Object>(Arrays.asList("relatedto1"))));
    irt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task2",
        new HashSet<Object>(Arrays.asList("relatedto4"))));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt).build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(2, entities.size());
    int isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("hello") &&
          !timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity ids' should have been hello and hello1");
      }
    }
    assertEquals(3, isRelatedToCnt);

    TimelineFilterList irt1 = new TimelineFilterList();
    irt1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3"))));
    irt1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.NOT_EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto5"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt1).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity id should have been hello2");
      }
    }
    assertEquals(0, isRelatedToCnt);

    TimelineFilterList irt2 = new TimelineFilterList(Operator.OR);
    irt2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task",
        new HashSet<Object>(Arrays.asList("relatedto1"))));
    irt2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task2",
        new HashSet<Object>(Arrays.asList("relatedto4"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt2).build(),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("hello") &&
          !timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity ids' should have been hello and hello1");
      }
    }
    assertEquals(0, isRelatedToCnt);

    TimelineFilterList irt3 = new TimelineFilterList();
    irt3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3", "relatedto5"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt3).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity id should have been hello1");
      }
    }
    assertEquals(0, isRelatedToCnt);

    TimelineFilterList irt4 = new TimelineFilterList();
    irt4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3"))));
    irt4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "dummy_task",
        new HashSet<Object>(Arrays.asList("relatedto5"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt4).build(),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList irt5 = new TimelineFilterList();
    irt5.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3", "relatedto7"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt5).build(),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task",
        new HashSet<Object>(Arrays.asList("relatedto1"))));
    list1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "dummy_task",
        new HashSet<Object>(Arrays.asList("relatedto4"))));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task2",
        new HashSet<Object>(Arrays.asList("relatedto4"))));
    TimelineFilterList irt6 = new TimelineFilterList(Operator.OR, list1, list2);
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().isRelatedTo(irt6).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity id should have been hello1");
      }
    }
    assertEquals(0, isRelatedToCnt);
  }

  @Test
  public void testReadEntitiesRelatesTo() throws Exception {
    TimelineFilterList rt = new TimelineFilterList(Operator.OR);
    rt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container2",
        new HashSet<Object>(Arrays.asList("relatesto7"))));
    rt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt).build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(2, entities.size());
    int relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello") &&
          !timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity ids' should have been hello and hello2");
      }
    }
    assertEquals(3, relatesToCnt);

    TimelineFilterList rt1 = new TimelineFilterList();
    rt1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1"))));
    rt1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.NOT_EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto3"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt1).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity id should have been hello1");
      }
    }
    assertEquals(0, relatesToCnt);

    TimelineFilterList rt2 = new TimelineFilterList(Operator.OR);
    rt2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container2",
        new HashSet<Object>(Arrays.asList("relatesto7"))));
    rt2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt2).build(),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello") &&
          !timelineEntity.getId().equals("hello2")) {
        Assert.fail("Entity ids' should have been hello and hello2");
      }
    }
    assertEquals(0, relatesToCnt);

    TimelineFilterList rt3 = new TimelineFilterList();
    rt3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1", "relatesto3"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt3).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello")) {
        Assert.fail("Entity id should have been hello");
      }
    }
    assertEquals(0, relatesToCnt);

    TimelineFilterList rt4 = new TimelineFilterList();
    rt4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1"))));
    rt4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "dummy_container",
        new HashSet<Object>(Arrays.asList("relatesto5"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt4).build(),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList rt5 = new TimelineFilterList();
    rt5.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatedto1", "relatesto8"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt5).build(),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container2",
        new HashSet<Object>(Arrays.asList("relatesto7"))));
    list1.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "dummy_container",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    TimelineFilterList rt6 = new TimelineFilterList(Operator.OR, list1, list2);
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt6).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello")) {
        Assert.fail("Entity id should have been hello");
      }
    }
    assertEquals(0, relatesToCnt);

    TimelineFilterList list3 = new TimelineFilterList();
    list3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1"))));
    list3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    TimelineFilterList list4 = new TimelineFilterList();
    list4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1"))));
    list4.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto2"))));
    TimelineFilterList combinedList =
        new TimelineFilterList(Operator.OR, list3, list4);
    TimelineFilterList rt7 = new TimelineFilterList(Operator.AND, combinedList,
        new TimelineKeyValuesFilter(
        TimelineCompareOp.NOT_EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto3"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().relatesTo(rt7).build(),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("hello1")) {
        Assert.fail("Entity id should have been hello1");
      }
    }
    assertEquals(0, relatesToCnt);
  }

  @Test
  public void testReadEntitiesDefaultView() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve());
    assertNotNull(e1);
    assertEquals(1, e1.getInfo().size());
    assertTrue(e1.getConfigs().isEmpty() &&
        e1.getMetrics().isEmpty() && e1.getIsRelatedToEntities().isEmpty() &&
        e1.getRelatesToEntities().isEmpty());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve());
    assertEquals(3, es1.size());
    for (TimelineEntity e : es1) {
      assertTrue(e.getConfigs().isEmpty() &&
          e.getMetrics().isEmpty() && e.getIsRelatedToEntities().isEmpty() &&
          e.getRelatesToEntities().isEmpty());
      assertEquals(1, e.getInfo().size());
    }
  }

  @Test
  public void testReadEntitiesByFields() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.INFO, Field.CONFIGS), null, null, null));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(0, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.IS_RELATED_TO,
        Field.METRICS), null, null, null));
    assertEquals(3, es1.size());
    int metricsCnt = 0;
    int isRelatedToCnt = 0;
    int infoCnt = 0;
    for (TimelineEntity entity : es1) {
      metricsCnt += entity.getMetrics().size();
      isRelatedToCnt += entity.getIsRelatedToEntities().size();
      infoCnt += entity.getInfo().size();
    }
    assertEquals(3, infoCnt);
    assertEquals(4, isRelatedToCnt);
    assertEquals(3, metricsCnt);
  }

  @Test
  public void testReadEntitiesConfigPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve(list, null, null, null, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getConfigs().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(list, null, null, null, null, null));
    int cfgCnt = 0;
    for (TimelineEntity entity : es1) {
      cfgCnt += entity.getConfigs().size();
      for (String confKey : entity.getConfigs().keySet()) {
        assertTrue("Config key returned should start with cfg_",
            confKey.startsWith("cfg_"));
      }
    }
    assertEquals(3, cfgCnt);
  }

  @Test
  public void testReadEntitiesConfigFilters() throws Exception {
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value1"));
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param2", "value2"));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value3"));
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_param2", "value2"));
    TimelineFilterList confFilterList =
        new TimelineFilterList(Operator.OR, list1, list2);
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null, null, null));
    assertEquals(2, entities.size());
    int cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(5, cfgCnt);

    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(2, entities.size());
    cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(5, cfgCnt);

    TimelineFilterList confFilterList1 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "cfg_param1", "value1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList1)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null, null, null));
    assertEquals(1, entities.size());
    cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(3, cfgCnt);

    TimelineFilterList confFilterList2 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "cfg_param1", "value1"),
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "config_param2", "value2"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList2)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList3 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "dummy_config", "value1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList3)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList4 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_config", "value1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
            1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList4)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
            null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList5 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_config", "value1", false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList5)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null, null, null));
    assertEquals(3, entities.size());
  }

  @Test
  public void testReadEntitiesConfigFilterPrefix() throws Exception {
    TimelineFilterList confFilterList = new TimelineFilterList();
    confFilterList.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value1"));
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList)
            .build(),
        new TimelineDataToRetrieve(list, null, null, null, null, null));
    assertEquals(1, entities.size());
    int cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
      for (String confKey : entity.getConfigs().keySet()) {
        assertTrue("Config key returned should start with cfg_",
            confKey.startsWith("cfg_"));
      }
    }
    assertEquals(2, cfgCnt);
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value1"));
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param2", "value2"));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value3"));
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "config_param2", "value2"));
    TimelineFilterList confFilterList1 =
        new TimelineFilterList(Operator.OR, list1, list2);
    TimelineFilterList confsToRetrieve =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "config_"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().configFilters(confFilterList1)
            .build(),
        new TimelineDataToRetrieve(confsToRetrieve, null, null, null, null,
        null));
    assertEquals(2, entities.size());
    cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
      for (String confKey : entity.getConfigs().keySet()) {
        assertTrue("Config key returned should start with config_",
            confKey.startsWith("config_"));
      }
    }
    assertEquals(2, cfgCnt);
  }

  @Test
  public void testReadEntitiesMetricPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve(null, list, null, null, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getMetrics().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(null, list, null, null, null, null));
    int metricCnt = 0;
    for (TimelineEntity entity : es1) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue("Metric Id returned should start with MAP1_",
            metric.getId().startsWith("MAP1_"));
      }
    }
    assertEquals(2, metricCnt);
  }

  @Test
  public void testReadEntitiesMetricTimeRange() throws Exception {
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        100, null, null));
    assertEquals(3, entities.size());
    int metricTimeSeriesCnt = 0;
    int metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric m : entity.getMetrics()) {
        metricTimeSeriesCnt += m.getValues().size();
      }
    }
    assertEquals(3, metricCnt);
    assertEquals(13, metricTimeSeriesCnt);

    entities = reader.getEntities(new TimelineReaderContext("cluster1", "user1",
        "some_flow_name", 1002345678919L, "application_1231111111_1111",
        "world", null), new TimelineEntityFilters.Builder().build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        100, CURRENT_TIME - 40000, CURRENT_TIME));
    assertEquals(3, entities.size());
    metricCnt = 0;
    metricTimeSeriesCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric m : entity.getMetrics()) {
        for (Long ts : m.getValues().keySet()) {
          assertTrue(ts >= CURRENT_TIME - 40000 && ts <= CURRENT_TIME);
        }
        metricTimeSeriesCnt += m.getValues().size();
      }
    }
    assertEquals(3, metricCnt);
    assertEquals(5, metricTimeSeriesCnt);

    TimelineEntity entity = reader.getEntity(new TimelineReaderContext(
        "cluster1", "user1", "some_flow_name", 1002345678919L,
        "application_1231111111_1111", "world", "hello"),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS), 100,
        CURRENT_TIME - 40000, CURRENT_TIME));
    assertNotNull(entity);
    assertEquals(2, entity.getMetrics().size());
    metricTimeSeriesCnt = 0;
    for (TimelineMetric m : entity.getMetrics()) {
      for (Long ts : m.getValues().keySet()) {
        assertTrue(ts >= CURRENT_TIME - 40000 && ts <= CURRENT_TIME);
      }
      metricTimeSeriesCnt += m.getValues().size();
    }
    assertEquals(3, metricTimeSeriesCnt);
  }

  @Test
  public void testReadEntitiesMetricFilters() throws Exception {
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "MAP1_SLOT_MILLIS", 50000000900L));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "MAP_SLOT_MILLIS", 80000000000L));
    list2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.EQUAL, "MAP1_BYTES", 50));
    TimelineFilterList metricFilterList =
        new TimelineFilterList(Operator.OR, list1, list2);
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(2, entities.size());
    int metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(3, metricCnt);

    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null,
        null, null));
    assertEquals(2, entities.size());
    metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(3, metricCnt);

    TimelineFilterList metricFilterList1 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.LESS_OR_EQUAL, "MAP_SLOT_MILLIS", 80000000000L),
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "MAP1_BYTES", 30));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList1)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(1, entities.size());
    metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(2, metricCnt);

    TimelineFilterList metricFilterList2 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "MAP_SLOT_MILLIS", 40000000000L),
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "MAP1_BYTES", 30));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList2)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList3 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.EQUAL, "dummy_metric", 5));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList3)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList4 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_metric", 5));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList4)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList5 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_metric", 5, false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList5)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null, null, null));
    assertEquals(3, entities.size());
  }

  @Test
  public void testReadEntitiesMetricFilterPrefix() throws Exception {
    TimelineFilterList metricFilterList = new TimelineFilterList();
    metricFilterList.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "MAP1_SLOT_MILLIS", 0L));
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList)
            .build(),
        new TimelineDataToRetrieve(null, list, null, null, null, null));
    assertEquals(1, entities.size());
    int metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue("Metric Id returned should start with MAP1_",
            metric.getId().startsWith("MAP1_"));
      }
    }
    assertEquals(1, metricCnt);

    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "MAP1_SLOT_MILLIS", 50000000900L));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.LESS_THAN, "MAP_SLOT_MILLIS", 80000000000L));
    list2.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.EQUAL, "MAP1_BYTES", 50));
    TimelineFilterList metricFilterList1 =
        new TimelineFilterList(Operator.OR, list1, list2);
    TimelineFilterList metricsToRetrieve = new TimelineFilterList(Operator.OR,
        new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList1)
            .build(),
        new TimelineDataToRetrieve(
        null, metricsToRetrieve, EnumSet.of(Field.METRICS), null, null, null));
    assertEquals(2, entities.size());
    metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric metric : entity.getMetrics()) {
        assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
        assertEquals(1, metric.getValues().size());
        assertTrue("Metric Id returned should start with MAP1_",
            metric.getId().startsWith("MAP1_"));
      }
    }
    assertEquals(2, metricCnt);

    entities = reader.getEntities(new TimelineReaderContext("cluster1", "user1",
        "some_flow_name", 1002345678919L, "application_1231111111_1111",
        "world", null),
        new TimelineEntityFilters.Builder().metricFilters(metricFilterList1)
            .build(),
        new TimelineDataToRetrieve(null, metricsToRetrieve,
        EnumSet.of(Field.METRICS), Integer.MAX_VALUE, null, null));
    assertEquals(2, entities.size());
    metricCnt = 0;
    int metricValCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric metric : entity.getMetrics()) {
        metricValCnt += metric.getValues().size();
        assertTrue("Metric Id returned should start with MAP1_",
            metric.getId().startsWith("MAP1_"));
      }
    }
    assertEquals(2, metricCnt);
    assertEquals(7, metricValCnt);
  }

  @Test
  public void testReadEntitiesInfoFilters() throws Exception {
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "infoMapKey3", 71.4));
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "infoMapKey1", "infoMapValue2"));
    TimelineFilterList list2 = new TimelineFilterList();
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "infoMapKey1", "infoMapValue1"));
    list2.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "infoMapKey2", 10));
    TimelineFilterList infoFilterList =
        new TimelineFilterList(Operator.OR, list1, list2);
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList).build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(2, entities.size());
    int infoCnt = 0;
    for (TimelineEntity entity : entities) {
      infoCnt += entity.getInfo().size();
    }
    assertEquals(7, infoCnt);

    TimelineFilterList infoFilterList1 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey1", "infoMapValue1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList1)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(1, entities.size());
    infoCnt = 0;
    for (TimelineEntity entity : entities) {
      infoCnt += entity.getInfo().size();
    }
    assertEquals(4, infoCnt);

    TimelineFilterList infoFilterList2 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey1", "infoMapValue2"),
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey3", 71.4));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList2)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList3 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "dummy_info", "some_value"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList3)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList4 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_info", "some_value"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList4)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList5 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_info", "some_value", false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111", "world", null),
        new TimelineEntityFilters.Builder().infoFilters(infoFilterList5)
            .build(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null,
        null, null));
    assertEquals(3, entities.size());
  }

  @Test(timeout = 90000)
  public void testListTypesInApp() throws Exception {
    Set<String> types = reader.getEntityTypes(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
            1002345678919L, "application_1231111111_1111", null, null));
    assertEquals(4, types.size());

    types = reader.getEntityTypes(
        new TimelineReaderContext("cluster1", null, null,
            null, "application_1231111111_1111", null, null));
    assertEquals(4, types.size());

    types = reader.getEntityTypes(
        new TimelineReaderContext("cluster1", null, null,
            null, "application_1231111111_1112", null, null));
    assertEquals(4, types.size());

    types = reader.getEntityTypes(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
            1002345678919L, "application_1231111111_1113", null, null));
    assertEquals(0, types.size());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  private boolean verifyRowKeyForSubApplication(byte[] rowKey, String suAppUser,
      String cluster, String user, TimelineEntity te) {
    SubApplicationRowKey key = SubApplicationRowKey.parseRowKey(rowKey);
    assertEquals(suAppUser, key.getSubAppUserId());
    assertEquals(cluster, key.getClusterId());
    assertEquals(te.getType(), key.getEntityType());
    assertEquals(te.getId(), key.getEntityId());
    assertEquals(user, key.getUserId());
    return true;
  }
}
