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
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetricOperation;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineExistsFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValueFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineKeyValuesFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnName;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.EventColumnNameConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.KeyConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.StringKeyConverter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for apps stored in TimelineStorage.
 */
public class TestHBaseTimelineStorageApps {

  private static HBaseTestingUtility util;
  private HBaseTimelineReaderImpl reader;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    createSchema();
    DataGeneratorForTest.loadApps(util);
  }

  private static void createSchema() throws IOException {
    TimelineSchemaCreator.createAllTables(util.getConfiguration(), false);
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

  private boolean isApplicationRowKeyCorrect(byte[] rowKey, String cluster,
      String user, String flow, Long runid, String appName) {

    ApplicationRowKey key = ApplicationRowKey.parseRowKey(rowKey);

    assertEquals(cluster, key.getClusterId());
    assertEquals(user, key.getUserId());
    assertEquals(flow, key.getFlowName());
    assertEquals(runid, key.getFlowRunId());
    assertEquals(appName, key.getAppId());
    return true;
  }

  @Test
  public void testWriteNullApplicationToHBase() throws Exception {
    TimelineEntities te = new TimelineEntities();
    ApplicationEntity entity = new ApplicationEntity();
    String appId = "application_1000178881110_2002";
    entity.setId(appId);
    long cTime = 1425016501000L;
    entity.setCreatedTime(cTime);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap = new HashMap<String, Object>();
    infoMap.put("in fo M apK  ey1", "infoMapValue1");
    infoMap.put("infoMapKey2", 10);
    entity.addInfo(infoMap);

    te.addEntity(entity);
    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_check_null_application";
      String user = "user1check_null_application";
      //set the flow name to null
      String flow = null;
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      hbi.write(cluster, user, flow, flowVersion, runid, appId, te);
      hbi.stop();

      // retrieve the row
      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes(cluster));
      scan.setStopRow(Bytes.toBytes(cluster + "1"));
      Connection conn = ConnectionFactory.createConnection(c1);
      ResultScanner resultScanner = new ApplicationTable()
          .getResultScanner(c1, conn, scan);

      assertTrue(resultScanner != null);
      // try to iterate over results
      int count = 0;
      for (Result rr = resultScanner.next(); rr != null;
          rr = resultScanner.next()) {
        count++;
      }
      // there should be no rows written
      // no exceptions thrown during write
      assertEquals(0, count);
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  @Test
  public void testWriteApplicationToHBase() throws Exception {
    TimelineEntities te = new TimelineEntities();
    ApplicationEntity entity = new ApplicationEntity();
    String appId = "application_1000178881110_2002";
    entity.setId(appId);
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

    // add aggregated metrics
    TimelineEntity aggEntity = new TimelineEntity();
    String type = TimelineEntityType.YARN_APPLICATION.toString();
    aggEntity.setId(appId);
    aggEntity.setType(type);
    long cTime2 = 1425016502000L;
    aggEntity.setCreatedTime(cTime2);

    TimelineMetric aggMetric = new TimelineMetric();
    aggMetric.setId("MEM_USAGE");
    Map<Long, Number> aggMetricValues = new HashMap<Long, Number>();
    long aggTs = ts;
    aggMetricValues.put(aggTs - 120000, 102400000L);
    aggMetric.setType(Type.SINGLE_VALUE);
    aggMetric.setRealtimeAggregationOp(TimelineMetricOperation.SUM);
    aggMetric.setValues(aggMetricValues);
    Set<TimelineMetric> aggMetrics = new HashSet<>();
    aggMetrics.add(aggMetric);
    entity.addMetrics(aggMetrics);
    te.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_write_app";
      String user = "user1";
      String flow = "s!ome_f\tlow  _n am!e";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      hbi.write(cluster, user, flow, flowVersion, runid, appId, te);

      // Write entity again, this time without created time.
      entity = new ApplicationEntity();
      appId = "application_1000178881110_2002";
      entity.setId(appId);
      // add the info map in Timeline Entity
      Map<String, Object> infoMap1 = new HashMap<>();
      infoMap1.put("infoMapKey3", "infoMapValue1");
      entity.addInfo(infoMap1);
      te = new TimelineEntities();
      te.addEntity(entity);
      hbi.write(cluster, user, flow, flowVersion, runid, appId, te);
      hbi.stop();

      infoMap.putAll(infoMap1);
      // retrieve the row
      ApplicationRowKey applicationRowKey =
          new ApplicationRowKey(cluster, user, flow, runid, appId);
      byte[] rowKey = applicationRowKey.getRowKey();
      Get get = new Get(rowKey);
      get.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      Result result = new ApplicationTable().getResult(c1, conn, get);

      assertTrue(result != null);
      assertEquals(17, result.size());

      // check the row key
      byte[] row1 = result.getRow();
      assertTrue(isApplicationRowKeyCorrect(row1, cluster, user, flow, runid,
          appId));

      // check info column family
      String id1 = ApplicationColumn.ID.readResult(result).toString();
      assertEquals(appId, id1);

      Long cTime1 =
          (Long) ApplicationColumn.CREATED_TIME.readResult(result);
      assertEquals(cTime, cTime1);

      Map<String, Object> infoColumns =
          ApplicationColumnPrefix.INFO.readResults(result,
              new StringKeyConverter());
      assertEquals(infoMap, infoColumns);

      // Remember isRelatedTo is of type Map<String, Set<String>>
      for (Map.Entry<String, Set<String>> isRelatedToEntry : isRelatedTo
          .entrySet()) {
        Object isRelatedToValue =
            ApplicationColumnPrefix.IS_RELATED_TO.readResult(result,
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
        String compoundValue =
            ApplicationColumnPrefix.RELATES_TO.readResult(result,
                relatesToEntry.getKey()).toString();
        // id3?id4?id5
        Set<String> relatesToValues =
            new HashSet<String>(Separator.VALUES.splitEncoded(compoundValue));
        assertEquals(relatesTo.get(relatesToEntry.getKey()).size(),
            relatesToValues.size());
        for (String v : relatesToEntry.getValue()) {
          assertTrue(relatesToValues.contains(v));
        }
      }

      KeyConverter<String> stringKeyConverter = new StringKeyConverter();
      // Configuration
      Map<String, Object> configColumns =
          ApplicationColumnPrefix.CONFIG
              .readResults(result, stringKeyConverter);
      assertEquals(conf, configColumns);

      NavigableMap<String, NavigableMap<Long, Number>> metricsResult =
          ApplicationColumnPrefix.METRIC.readResultsWithTimestamps(result,
              stringKeyConverter);

      NavigableMap<Long, Number> metricMap = metricsResult.get(m1.getId());
      matchMetrics(metricValues, metricMap);

      // read the timeline entity using the reader this time. In metrics limit
      // specify Integer MAX_VALUE. A TIME_SERIES will be returned(if more than
      // one value exists for a metric).
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appId,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null,
          EnumSet.of(TimelineReader.Field.ALL), Integer.MAX_VALUE));
      assertNotNull(e1);

      // verify attributes
      assertEquals(appId, e1.getId());
      assertEquals(TimelineEntityType.YARN_APPLICATION.toString(),
          e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      Map<String, Object> infoMap2 = e1.getInfo();
      assertEquals(infoMap, infoMap2);

      Map<String, Set<String>> isRelatedTo2 = e1.getIsRelatedToEntities();
      assertEquals(isRelatedTo, isRelatedTo2);

      Map<String, Set<String>> relatesTo2 = e1.getRelatesToEntities();
      assertEquals(relatesTo, relatesTo2);

      Map<String, String> conf2 = e1.getConfigs();
      assertEquals(conf, conf2);

      Set<TimelineMetric> metrics2 = e1.getMetrics();
      assertEquals(2, metrics2.size());
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        assertTrue(metric2.getId().equals("MAP_SLOT_MILLIS") ||
            metric2.getId().equals("MEM_USAGE"));
        if (metric2.getId().equals("MAP_SLOT_MILLIS")) {
          assertEquals(6, metricValues2.size());
          matchMetrics(metricValues, metricValues2);
        }
        if (metric2.getId().equals("MEM_USAGE")) {
          assertEquals(1, metricValues2.size());
          matchMetrics(aggMetricValues, metricValues2);
        }
      }

      // In metrics limit specify a value of 3. No more than 3 values for a
      // metric will be returned.
      e1 = reader.getEntity(new TimelineReaderContext(cluster, user, flow,
          runid, appId, entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null,
          EnumSet.of(TimelineReader.Field.ALL), 3));
      assertNotNull(e1);
      assertEquals(appId, e1.getId());
      assertEquals(TimelineEntityType.YARN_APPLICATION.toString(),
          e1.getType());
      assertEquals(conf, e1.getConfigs());
      metrics2 = e1.getMetrics();
      assertEquals(2, metrics2.size());
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        assertTrue(metricValues2.size() <= 3);
        assertTrue(metric2.getId().equals("MAP_SLOT_MILLIS") ||
            metric2.getId().equals("MEM_USAGE"));
      }

      // Check if single value(latest value) instead of time series is returned
      // if metricslimit is not set(null), irrespective of number of metric
      // values.
      e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appId,
         entity.getType(), entity.getId()), new TimelineDataToRetrieve(
         null, null, EnumSet.of(TimelineReader.Field.ALL), null));
      assertNotNull(e1);
      assertEquals(appId, e1.getId());
      assertEquals(TimelineEntityType.YARN_APPLICATION.toString(),
          e1.getType());
      assertEquals(cTime, e1.getCreatedTime());
      assertEquals(infoMap, e1.getInfo());
      assertEquals(isRelatedTo, e1.getIsRelatedToEntities());
      assertEquals(relatesTo, e1.getRelatesToEntities());
      assertEquals(conf, e1.getConfigs());
      assertEquals(2, e1.getMetrics().size());
      for (TimelineMetric metric : e1.getMetrics()) {
        assertEquals(1, metric.getValues().size());
        assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric.getType());
        assertTrue(metric.getId().equals("MAP_SLOT_MILLIS") ||
            metric.getId().equals("MEM_USAGE"));
        assertEquals(1, metric.getValues().size());
        if (metric.getId().equals("MAP_SLOT_MILLIS")) {
          assertTrue(metric.getValues().containsKey(ts - 20000));
          assertEquals(metricValues.get(ts - 20000),
              metric.getValues().get(ts - 20000));
        }
        if (metric.getId().equals("MEM_USAGE")) {
          assertTrue(metric.getValues().containsKey(aggTs - 120000));
          assertEquals(aggMetricValues.get(aggTs - 120000),
              metric.getValues().get(aggTs - 120000));
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
  public void testEvents() throws IOException {
    TimelineEvent event = new TimelineEvent();
    String eventId = ApplicationMetricsConstants.CREATED_EVENT_TYPE;
    event.setId(eventId);
    Long expTs = 1436512802000L;
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
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_events";
      String user = "user2";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName = "application_123465899910_1001";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, entities);
      hbi.stop();

      // retrieve the row
      ApplicationRowKey applicationRowKey =
          new ApplicationRowKey(cluster, user, flow, runid, appName);
      byte[] rowKey = applicationRowKey.getRowKey();
      Get get = new Get(rowKey);
      get.setMaxVersions(Integer.MAX_VALUE);
      Connection conn = ConnectionFactory.createConnection(c1);
      Result result = new ApplicationTable().getResult(c1, conn, get);

      assertTrue(result != null);

      // check the row key
      byte[] row1 = result.getRow();
      assertTrue(isApplicationRowKeyCorrect(row1, cluster, user, flow, runid,
          appName));

      Map<EventColumnName, Object> eventsResult =
          ApplicationColumnPrefix.EVENT.readResults(result,
              new EventColumnNameConverter());
      // there should be only one event
      assertEquals(1, eventsResult.size());
      for (Map.Entry<EventColumnName, Object> e : eventsResult.entrySet()) {
        EventColumnName eventColumnName = e.getKey();
        // the qualifier is a compound key
        // hence match individual values
        assertEquals(eventId, eventColumnName.getId());
        assertEquals(expTs, eventColumnName.getTimestamp());
        assertEquals(expKey, eventColumnName.getInfoKey());
        Object value = e.getValue();
        // there should be only one timestamp and value
        assertEquals(expVal, value.toString());
      }

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
      TimelineEntity e2 = reader.getEntity(
          new TimelineReaderContext(cluster, user, null, null, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
      assertNotNull(e1);
      assertNotNull(e2);
      assertEquals(e1, e2);

      // check the events
      NavigableSet<TimelineEvent> events = e1.getEvents();
      // there should be only one event
      assertEquals(1, events.size());
      for (TimelineEvent e : events) {
        assertEquals(eventId, e.getId());
        assertEquals(expTs, Long.valueOf(e.getTimestamp()));
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
  public void testNonIntegralMetricValues() throws IOException {
    TimelineEntities teApp = new TimelineEntities();
    ApplicationEntity entityApp = new ApplicationEntity();
    String appId = "application_1000178881110_2002";
    entityApp.setId(appId);
    entityApp.setCreatedTime(1425016501000L);
    // add metrics with floating point values
    Set<TimelineMetric> metricsApp = new HashSet<>();
    TimelineMetric mApp = new TimelineMetric();
    mApp.setId("MAP_SLOT_MILLIS");
    Map<Long, Number> metricAppValues = new HashMap<Long, Number>();
    long ts = System.currentTimeMillis();
    metricAppValues.put(ts - 20, 10.5);
    metricAppValues.put(ts - 10, 20.5);
    mApp.setType(Type.TIME_SERIES);
    mApp.setValues(metricAppValues);
    metricsApp.add(mApp);
    entityApp.addMetrics(metricsApp);
    teApp.addEntity(entityApp);

    TimelineEntities teEntity = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    entity.setId("hello");
    entity.setType("world");
    entity.setCreatedTime(1425016501000L);
    // add metrics with floating point values
    Set<TimelineMetric> metricsEntity = new HashSet<>();
    TimelineMetric mEntity = new TimelineMetric();
    mEntity.setId("MAP_SLOT_MILLIS");
    mEntity.addValue(ts - 20, 10.5);
    metricsEntity.add(mEntity);
    entity.addMetrics(metricsEntity);
    teEntity.addEntity(entity);

    HBaseTimelineWriterImpl hbi = null;
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      // Writing application entity.
      try {
        hbi.write("c1", "u1", "f1", "v1", 1002345678919L, appId, teApp);
        Assert.fail("Expected an exception as metric values are non integral");
      } catch (IOException e) {}

      // Writing generic entity.
      try {
        hbi.write("c1", "u1", "f1", "v1", 1002345678919L, appId, teEntity);
        Assert.fail("Expected an exception as metric values are non integral");
      } catch (IOException e) {}
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  @Test
  public void testReadApps() throws Exception {
    TimelineEntity entity = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    assertNotNull(entity);
    assertEquals(3, entity.getConfigs().size());
    assertEquals(1, entity.getIsRelatedToEntities().size());
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
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
    assertEquals(5, infoCnt);
    assertEquals(4, eventCnt);
    assertEquals(4, relatesToCnt);
    assertEquals(4, isRelatedToCnt);
  }

  @Test
  public void testFilterAppsByCreatedTime() throws Exception {
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, 1425016502000L, 1425016502040L, null,
        null, null, null, null, null),
        new TimelineDataToRetrieve());
    assertEquals(3, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("application_1111111111_2222") &&
          !entity.getId().equals("application_1111111111_3333") &&
          !entity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entities with ids' application_1111111111_2222, " +
            "application_1111111111_3333 and application_1111111111_4444" +
            " should be present");
      }
    }
    entities =  reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, 1425016502015L, null, null, null, null,
        null, null, null),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("application_1111111111_3333") &&
          !entity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Apps with ids' application_1111111111_3333 and" +
            " application_1111111111_4444 should be present");
      }
    }
    entities =  reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, 1425016502015L, null, null, null,
        null, null, null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    for (TimelineEntity entity : entities) {
      if (!entity.getId().equals("application_1111111111_2222")) {
        Assert.fail("App with id application_1111111111_2222 should" +
            " be present");
      }
    }
  }

  @Test
  public void testReadAppsDefaultView() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve());
    assertNotNull(e1);
    assertTrue(e1.getInfo().isEmpty() && e1.getConfigs().isEmpty() &&
        e1.getMetrics().isEmpty() && e1.getIsRelatedToEntities().isEmpty() &&
        e1.getRelatesToEntities().isEmpty());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve());
    assertEquals(3, es1.size());
    for (TimelineEntity e : es1) {
      assertTrue(e.getInfo().isEmpty() && e.getConfigs().isEmpty() &&
          e.getMetrics().isEmpty() && e.getIsRelatedToEntities().isEmpty() &&
          e.getRelatesToEntities().isEmpty());
    }
  }

  @Test
  public void testReadAppsByFields() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.INFO, Field.CONFIGS), null));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(0, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.IS_RELATED_TO, Field.METRICS), null));
    assertEquals(3, es1.size());
    int metricsCnt = 0;
    int isRelatedToCnt = 0;
    int infoCnt = 0;
    for (TimelineEntity entity : es1) {
      metricsCnt += entity.getMetrics().size();
      isRelatedToCnt += entity.getIsRelatedToEntities().size();
      infoCnt += entity.getInfo().size();
    }
    assertEquals(0, infoCnt);
    assertEquals(4, isRelatedToCnt);
    assertEquals(3, metricsCnt);
  }

  @Test
  public void testReadAppsIsRelatedTo() throws Exception {
    TimelineFilterList irt = new TimelineFilterList(Operator.OR);
    irt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task",
        new HashSet<Object>(Arrays.asList("relatedto1"))));
    irt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task2",
        new HashSet<Object>(Arrays.asList("relatedto4"))));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt, null, null, null,
        null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    assertEquals(2, entities.size());
    int isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222") &&
          !timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity ids' should have been application_1111111111_2222"
            + " and application_1111111111_3333");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt1, null, null,
        null, null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity id should have been application_1111111111_4444");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt2, null, null,
        null, null),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222") &&
          !timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity ids' should have been application_1111111111_2222"
            + " and application_1111111111_3333");
      }
    }
    assertEquals(0, isRelatedToCnt);

    TimelineFilterList irt3 = new TimelineFilterList();
    irt3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3", "relatedto5"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt3, null, null,
        null, null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity id should have been application_1111111111_3333");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt4, null, null,
        null, null),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList irt5 = new TimelineFilterList();
    irt5.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "task1",
        new HashSet<Object>(Arrays.asList("relatedto3", "relatedto7"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt5, null, null,
        null, null),
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, irt6, null, null,
        null, null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    isRelatedToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity id should have been application_1111111111_3333");
      }
    }
    assertEquals(0, isRelatedToCnt);
  }


  @Test
  public void testReadAppsRelatesTo() throws Exception {
    TimelineFilterList rt = new TimelineFilterList(Operator.OR);
    rt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container2",
        new HashSet<Object>(Arrays.asList("relatesto7"))));
    rt.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container1",
        new HashSet<Object>(Arrays.asList("relatesto4"))));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt, null, null, null, null,
        null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    assertEquals(2, entities.size());
    int relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222") &&
          !timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity ids' should have been application_1111111111_2222"
            + " and application_1111111111_4444");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt1, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity id should have been application_1111111111_3333");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt2, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222") &&
          !timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity ids' should have been application_1111111111_2222"
            + " and application_1111111111_4444");
      }
    }
    assertEquals(0, relatesToCnt);

    TimelineFilterList rt3 = new TimelineFilterList();
    rt3.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatesto1", "relatesto3"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt3, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222")) {
        Assert.fail("Entity id should have been application_1111111111_2222");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt4, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(0, entities.size());

    TimelineFilterList rt5 = new TimelineFilterList();
    rt5.addFilter(new TimelineKeyValuesFilter(
        TimelineCompareOp.EQUAL, "container",
        new HashSet<Object>(Arrays.asList("relatedto1", "relatesto8"))));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt5, null, null, null, null,
        null),
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt6, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222")) {
        Assert.fail("Entity id should have been application_1111111111_2222");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, rt7, null, null, null, null,
        null),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_3333")) {
        Assert.fail("Entity id should have been application_1111111111_3333");
      }
    }
    assertEquals(0, relatesToCnt);
  }

  @Test
  public void testReadAppsRelationsAndEventFiltersDefaultView()
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, relatesTo, isRelatedTo,
        null, null, null, eventFilter),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    int eventCnt = 0;
    int isRelatedToCnt = 0;
    int relatesToCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      isRelatedToCnt += timelineEntity.getIsRelatedToEntities().size();
      relatesToCnt += timelineEntity.getRelatesToEntities().size();
      if (!timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity id should have been application_1111111111_4444");
      }
    }
    assertEquals(0, eventCnt);
    assertEquals(0, isRelatedToCnt);
    assertEquals(0, relatesToCnt);
  }

  @Test
  public void testReadAppsConfigFilters() throws Exception {
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
    assertEquals(2, entities.size());
    int cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(5, cfgCnt);

    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList1, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList2, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList3 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "dummy_config", "value1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList3, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList4 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_config", "value1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList4, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList confFilterList5 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_config", "value1", false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList5, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.CONFIGS),
        null));
    assertEquals(3, entities.size());
  }

  @Test
  public void testReadAppsEventFilters() throws Exception {
    TimelineFilterList ef = new TimelineFilterList();
    ef.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.EQUAL, "update_event"));
    ef.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
    assertEquals(1, entities.size());
    int eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity id should have been application_1111111111_4444");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef1), new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity id should have been application_1111111111_4444");
      }
    }
    assertEquals(0, eventCnt);

    TimelineFilterList ef2 = new TimelineFilterList();
    ef2.addFilter(new TimelineExistsFilter(
        TimelineCompareOp.NOT_EQUAL, "end_event"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef2),
        new TimelineDataToRetrieve());
    assertEquals(2, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222") &&
          !timelineEntity.getId().equals("application_1111111111_4444")) {
        Assert.fail("Entity ids' should have been application_1111111111_2222"
            + " and application_1111111111_4444");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef3),
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef4),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222")) {
        Assert.fail("Entity id should have been application_1111111111_2222");
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        null, ef5),
        new TimelineDataToRetrieve());
    assertEquals(1, entities.size());
    eventCnt = 0;
    for (TimelineEntity timelineEntity : entities) {
      eventCnt += timelineEntity.getEvents().size();
      if (!timelineEntity.getId().equals("application_1111111111_2222")) {
        Assert.fail("Entity id should have been application_1111111111_2222");
      }
    }
    assertEquals(0, eventCnt);
  }

  @Test
  public void testReadAppsConfigPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve(list, null, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getConfigs().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null) ,
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(list, null, null, null));
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
  public void testReadAppsConfigFilterPrefix() throws Exception {
    TimelineFilterList confFilterList = new TimelineFilterList();
    confFilterList.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "cfg_param1", "value1"));
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList, null, null),
        new TimelineDataToRetrieve(list, null, null, null));
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
    TimelineFilterList confsToRetrieve =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "config_"));
    TimelineFilterList confFilterList1 =
        new TimelineFilterList(Operator.OR, list1, list2);
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilterList1, null, null),
        new TimelineDataToRetrieve(confsToRetrieve, null, null, null));
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
  public void testReadAppsMetricFilters() throws Exception {
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
    assertEquals(2, entities.size());
    int metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(3, metricCnt);

    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL), null));
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList1, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList2, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList3 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.EQUAL, "dummy_metric", 5));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList3, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList4 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_metric", 5));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList4, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
    assertEquals(0, entities.size());

    TimelineFilterList metricFilterList5 = new TimelineFilterList(
        new TimelineCompareFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_metric", 5, false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList5, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS),
        null));
    assertEquals(3, entities.size());
  }

  @Test
  public void testReadAppsMetricPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve(null, list, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getMetrics().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, list, null, null));
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
  public void testReadAppsMetricFilterPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    TimelineFilterList metricFilterList = new TimelineFilterList();
    metricFilterList.addFilter(new TimelineCompareFilter(
        TimelineCompareOp.GREATER_OR_EQUAL, "MAP1_SLOT_MILLIS", 0L));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList, null),
        new TimelineDataToRetrieve(null, list, null, null));
    int metricCnt = 0;
    assertEquals(1, entities.size());
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
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
    TimelineFilterList metricsToRetrieve = new TimelineFilterList(Operator.OR,
        new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    TimelineFilterList metricFilterList1 =
        new TimelineFilterList(Operator.OR, list1, list2);
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList1, null),
        new TimelineDataToRetrieve(null, metricsToRetrieve, null, null));
    metricCnt = 0;
    assertEquals(2, entities.size());
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
      for (TimelineMetric metric : entity.getMetrics()) {
        assertTrue("Metric Id returned should start with MAP1_",
            metric.getId().startsWith("MAP1_"));
      }
    }
    assertEquals(2, metricCnt);

    entities = reader.getEntities(new TimelineReaderContext("cluster1", "user1",
        "some_flow_name", 1002345678919L, null,
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilterList1, null), new TimelineDataToRetrieve(null,
        metricsToRetrieve, EnumSet.of(Field.METRICS), Integer.MAX_VALUE));
    metricCnt = 0;
    int metricValCnt = 0;
    assertEquals(2, entities.size());
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
  public void testReadAppsInfoFilters() throws Exception {
    TimelineFilterList list1 = new TimelineFilterList();
    list1.addFilter(new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "infoMapKey3", 85.85));
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
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(2, entities.size());
    int infoCnt = 0;
    for (TimelineEntity entity : entities) {
      infoCnt += entity.getInfo().size();
    }
    assertEquals(5, infoCnt);

    TimelineFilterList infoFilterList1 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey1", "infoMapValue1"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList1,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(1, entities.size());
    infoCnt = 0;
    for (TimelineEntity entity : entities) {
      infoCnt += entity.getInfo().size();
    }
    assertEquals(3, infoCnt);

    TimelineFilterList infoFilterList2 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey1", "infoMapValue2"),
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "infoMapKey3", 85.85));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList2,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList3 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.EQUAL, "dummy_info", "some_value"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList3,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList4 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_info", "some_value"));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList4,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(0, entities.size());

    TimelineFilterList infoFilterList5 = new TimelineFilterList(
        new TimelineKeyValueFilter(
        TimelineCompareOp.NOT_EQUAL, "dummy_info", "some_value", false));
    entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, infoFilterList5,
        null, null, null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.INFO), null));
    assertEquals(3, entities.size());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
