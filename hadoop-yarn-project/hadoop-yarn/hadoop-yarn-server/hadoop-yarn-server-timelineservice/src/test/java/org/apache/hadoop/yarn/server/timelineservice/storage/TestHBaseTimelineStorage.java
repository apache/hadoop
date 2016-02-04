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
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList.Operator;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.application.ApplicationTable;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimelineStorageUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumn;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnFamily;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityColumnPrefix;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityRowKey;
import org.apache.hadoop.yarn.server.timelineservice.storage.entity.EntityTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
  private HBaseTimelineReaderImpl reader;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    createSchema();
    loadEntities();
    loadApps();
  }

  private static void createSchema() throws IOException {
    TimelineSchemaCreator.createAllTables(util.getConfiguration(), false);
  }

  private static void loadApps() throws IOException {
    TimelineEntities te = new TimelineEntities();
    TimelineEntity entity = new TimelineEntity();
    String id = "application_1111111111_2222";
    entity.setId(id);
    entity.setType(TimelineEntityType.YARN_APPLICATION.toString());
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
    conf.put("cfg_param1", "value3");
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

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    TimelineEvent event = new TimelineEvent();
    event.setId("event1");
    event.setTimestamp(ts - 2000);
    entity.addEvent(event);
    te.addEntity(entity);

    TimelineEntities te1 = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "application_1111111111_3333";
    entity1.setId(id1);
    entity1.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entity1.setCreatedTime(cTime);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap1 = new HashMap<String, Object>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    entity1.addInfo(infoMap1);

    // add the isRelatedToEntity info
    String key1 = "task";
    String value1 = "is_related_to_entity_id_here";
    Set<String> isRelatedToSet1 = new HashSet<String>();
    isRelatedToSet1.add(value1);
    Map<String, Set<String>> isRelatedTo1 = new HashMap<String, Set<String>>();
    isRelatedTo1.put(key, isRelatedToSet1);
    entity1.setIsRelatedToEntities(isRelatedTo1);

    // add the relatesTo info
    key1 = "container";
    value1 = "relates_to_entity_id_here";
    Set<String> relatesToSet1 = new HashSet<String>();
    relatesToSet1.add(value1);
    value1 = "relates_to_entity_id_here_Second";
    relatesToSet1.add(value1);
    Map<String, Set<String>> relatesTo1 = new HashMap<String, Set<String>>();
    relatesTo1.put(key1, relatesToSet1);
    entity1.setRelatesToEntities(relatesTo1);

    // add some config entries
    Map<String, String> conf1 = new HashMap<String, String>();
    conf1.put("cfg_param1", "value1");
    conf1.put("cfg_param2", "value2");
    entity1.addConfigs(conf1);

    // add metrics
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    Map<Long, Number> metricValues1 = new HashMap<Long, Number>();
    long ts1 = System.currentTimeMillis();
    metricValues1.put(ts1 - 120000, 100000000);
    metricValues1.put(ts1 - 100000, 200000000);
    metricValues1.put(ts1 - 80000, 300000000);
    metricValues1.put(ts1 - 60000, 400000000);
    metricValues1.put(ts1 - 40000, 50000000000L);
    metricValues1.put(ts1 - 20000, 60000000000L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues1);
    metrics1.add(m2);
    entity1.addMetrics(metrics1);
    te1.addEntity(entity1);

    TimelineEntities te2 = new TimelineEntities();
    TimelineEntity entity2 = new TimelineEntity();
    String id2 = "application_1111111111_4444";
    entity2.setId(id2);
    entity2.setType(TimelineEntityType.YARN_APPLICATION.toString());
    entity2.setCreatedTime(cTime);
    te2.addEntity(entity2);
    HBaseTimelineWriterImpl hbi = null;
    try {
      hbi = new HBaseTimelineWriterImpl(util.getConfiguration());
      hbi.init(util.getConfiguration());
      hbi.start();
      String cluster = "cluster1";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = "application_1111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      appName = "application_1111111111_3333";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te1);
      appName = "application_1111111111_4444";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te2);
      hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  private static void loadEntities() throws IOException {
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
    conf.put("cfg_param1", "value3");
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

    TimelineMetric m12 = new TimelineMetric();
    m12.setId("MAP1_BYTES");
    m12.addValue(ts, 50);
    metrics.add(m12);
    entity.addMetrics(metrics);
    te.addEntity(entity);

    TimelineEntity entity1 = new TimelineEntity();
    String id1 = "hello1";
    entity1.setId(id1);
    entity1.setType(type);
    entity1.setCreatedTime(cTime);

    // add the info map in Timeline Entity
    Map<String, Object> infoMap1 = new HashMap<String, Object>();
    infoMap1.put("infoMapKey1", "infoMapValue1");
    infoMap1.put("infoMapKey2", 10);
    entity1.addInfo(infoMap1);

    // add the isRelatedToEntity info
    String key1 = "task";
    String value1 = "is_related_to_entity_id_here";
    Set<String> isRelatedToSet1 = new HashSet<String>();
    isRelatedToSet1.add(value1);
    Map<String, Set<String>> isRelatedTo1 = new HashMap<String, Set<String>>();
    isRelatedTo1.put(key, isRelatedToSet1);
    entity1.setIsRelatedToEntities(isRelatedTo1);

    // add the relatesTo info
    key1 = "container";
    value1 = "relates_to_entity_id_here";
    Set<String> relatesToSet1 = new HashSet<String>();
    relatesToSet1.add(value1);
    value1 = "relates_to_entity_id_here_Second";
    relatesToSet1.add(value1);
    Map<String, Set<String>> relatesTo1 = new HashMap<String, Set<String>>();
    relatesTo1.put(key1, relatesToSet1);
    entity1.setRelatesToEntities(relatesTo1);

    // add some config entries
    Map<String, String> conf1 = new HashMap<String, String>();
    conf1.put("cfg_param1", "value1");
    conf1.put("cfg_param2", "value2");
    entity1.addConfigs(conf1);

    // add metrics
    Set<TimelineMetric> metrics1 = new HashSet<>();
    TimelineMetric m2 = new TimelineMetric();
    m2.setId("MAP1_SLOT_MILLIS");
    Map<Long, Number> metricValues1 = new HashMap<Long, Number>();
    long ts1 = System.currentTimeMillis();
    metricValues1.put(ts1 - 120000, 100000000);
    metricValues1.put(ts1 - 100000, 200000000);
    metricValues1.put(ts1 - 80000, 300000000);
    metricValues1.put(ts1 - 60000, 400000000);
    metricValues1.put(ts1 - 40000, 50000000000L);
    metricValues1.put(ts1 - 20000, 60000000000L);
    m2.setType(Type.TIME_SERIES);
    m2.setValues(metricValues1);
    metrics1.add(m2);
    entity1.addMetrics(metrics1);
    te.addEntity(entity1);

    TimelineEntity entity2 = new TimelineEntity();
    String id2 = "hello2";
    entity2.setId(id2);
    entity2.setType(type);
    entity2.setCreatedTime(cTime);
    te.addEntity(entity2);
    HBaseTimelineWriterImpl hbi = null;
    try {
        hbi = new HBaseTimelineWriterImpl(util.getConfiguration());
        hbi.init(util.getConfiguration());
        hbi.start();
        String cluster = "cluster1";
        String user = "user1";
        String flow = "some_flow_name";
        String flowVersion = "AB7822C10F1111";
        long runid = 1002345678919L;
        String appName = "application_1231111111_1111";
        hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
        hbi.stop();
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
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
  public void testWriteApplicationToHBase() throws Exception {
    TimelineEntities te = new TimelineEntities();
    ApplicationEntity entity = new ApplicationEntity();
    String appId = "application_1000178881110_2002";
    entity.setId(appId);
    long cTime = 1425016501000L;
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
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
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
      assertEquals(15, result.size());

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
      matchMetrics(metricValues, metricMap);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appId,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(
          null, null, EnumSet.of(TimelineReader.Field.ALL)));
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
      assertEquals(metrics, metrics2);
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        matchMetrics(metricValues, metricValues2);
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
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
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_write_entity";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName =
          ApplicationId.newInstance(System.currentTimeMillis(), 1).toString();
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
          matchMetrics(metricValues, metricMap);
        }
      }
      assertEquals(1, rowCount);
      assertEquals(16, colCount);

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
      Set<TimelineEntity> es1 = reader.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), null),
          new TimelineEntityFilters(),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
      assertNotNull(e1);
      assertEquals(1, es1.size());

      // verify attributes
      assertEquals(id, e1.getId());
      assertEquals(type, e1.getType());
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
      assertEquals(metrics, metrics2);
      for (TimelineMetric metric2 : metrics2) {
        Map<Long, Number> metricValues2 = metric2.getValues();
        matchMetrics(metricValues, metricValues2);
      }
    } finally {
      if (hbi != null) {
        hbi.stop();
        hbi.close();
      }
    }
  }

  private boolean isRowKeyCorrect(byte[] rowKey, String cluster, String user,
      String flow, long runid, String appName, TimelineEntity te) {

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

  private boolean isApplicationRowKeyCorrect(byte[] rowKey, String cluster,
      String user, String flow, long runid, String appName) {

    ApplicationRowKey key = ApplicationRowKey.parseRowKey(rowKey);

    assertEquals(cluster, key.getClusterId());
    assertEquals(user, key.getUserId());
    assertEquals(flow, key.getFlowName());
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
        assertEquals(
            TimelineStorageUtils.invertLong(expTs), Bytes.toLong(karr[1]));
        assertEquals(expKey, Bytes.toString(karr[2]));
        Object value = e.getValue();
        // there should be only one timestamp and value
        assertEquals(expVal, value.toString());
      }

      // read the timeline entity using the reader this time
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
      TimelineEntity e2 = reader.getEntity(
          new TimelineReaderContext(cluster, user, null, null, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
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
    try {
      Configuration c1 = util.getConfiguration();
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.start();
      String cluster = "cluster_test_empty_eventkey";
      String user = "user_emptyeventkey";
      String flow = "other_flow_name";
      String flowVersion = "1111F01C2287BA";
      long runid = 1009876543218L;
      String appName =
          ApplicationId.newInstance(System.currentTimeMillis(), 1).toString();
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
            assertEquals(TimelineStorageUtils.invertLong(expTs),
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
      TimelineEntity e1 = reader.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), entity.getId()),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
      Set<TimelineEntity> es1 = reader.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, appName,
          entity.getType(), null),
          new TimelineEntityFilters(),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
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
  public void testReadEntities() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", "hello"),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(1, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world",
        null), new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
    assertEquals(3, es1.size());
  }

  @Test
  public void testReadEntitiesDefaultView() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", "hello"),
        new TimelineDataToRetrieve());
    assertNotNull(e1);
    assertTrue(e1.getInfo().isEmpty() && e1.getConfigs().isEmpty() &&
        e1.getMetrics().isEmpty() && e1.getIsRelatedToEntities().isEmpty() &&
        e1.getRelatesToEntities().isEmpty());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
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
  public void testReadEntitiesByFields() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", "hello"),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.INFO, Field.CONFIGS)));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(0, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.IS_RELATED_TO, Field.METRICS)));
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
    assertEquals(2, isRelatedToCnt);
    assertEquals(3, metricsCnt);
  }

  @Test
  public void testReadEntitiesConfigPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", "hello"),
        new TimelineDataToRetrieve(list, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getConfigs().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(list, null, null));
    int cfgCnt = 0;
    for (TimelineEntity entity : es1) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(3, cfgCnt);
  }

  @Test
  public void testReadEntitiesConfigFilterPrefix() throws Exception {
    Map<String, String> confFilters = ImmutableMap.of("cfg_param1","value1");
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilters, null, null),
        new TimelineDataToRetrieve(list, null, null));
    assertEquals(1, entities.size());
    int cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
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
        1002345678919L, "application_1231111111_1111","world", "hello"),
        new TimelineDataToRetrieve(null, list, null));
    assertNotNull(e1);
    assertEquals(1, e1.getMetrics().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, list, null));
    int metricCnt = 0;
    for (TimelineEntity entity : es1) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(2, metricCnt);
  }

  @Test
  public void testReadEntitiesMetricFilterPrefix() throws Exception {
    Set<String> metricFilters = ImmutableSet.of("MAP1_SLOT_MILLIS");
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1","user1", "some_flow_name",
        1002345678919L, "application_1231111111_1111","world", null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilters, null),
        new TimelineDataToRetrieve(null, list, null));
    assertEquals(1, entities.size());
    int metricCnt = 0;
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(1, metricCnt);
  }

  @Test
  public void testReadApps() throws Exception {
    TimelineEntity e1 = reader.getEntity(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, "application_1111111111_2222",
        TimelineEntityType.YARN_APPLICATION.toString(), null),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(1, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, null, EnumSet.of(Field.ALL)));
    assertEquals(3, es1.size());
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
        null, null, EnumSet.of(Field.INFO, Field.CONFIGS)));
    assertNotNull(e1);
    assertEquals(3, e1.getConfigs().size());
    assertEquals(0, e1.getIsRelatedToEntities().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(
        null, null, EnumSet.of(Field.IS_RELATED_TO, Field.METRICS)));
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
    assertEquals(2, isRelatedToCnt);
    assertEquals(3, metricsCnt);
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
        new TimelineDataToRetrieve(list, null, null));
    assertNotNull(e1);
    assertEquals(1, e1.getConfigs().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null) ,
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(list, null, null));
    int cfgCnt = 0;
    for (TimelineEntity entity : es1) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(3, cfgCnt);
  }

  @Test
  public void testReadAppsConfigFilterPrefix() throws Exception {
    Map<String, String> confFilters = ImmutableMap.of("cfg_param1","value1");
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "cfg_"));
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null,
        confFilters, null, null),
        new TimelineDataToRetrieve(list, null, null));
    assertEquals(1, entities.size());
    int cfgCnt = 0;
    for (TimelineEntity entity : entities) {
      cfgCnt += entity.getConfigs().size();
    }
    assertEquals(2, cfgCnt);
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
        new TimelineDataToRetrieve(null, list, null));
    assertNotNull(e1);
    assertEquals(1, e1.getMetrics().size());
    Set<TimelineEntity> es1 = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(),
        new TimelineDataToRetrieve(null, list, null));
    int metricCnt = 0;
    for (TimelineEntity entity : es1) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(2, metricCnt);
  }

  @Test
  public void testReadAppsMetricFilterPrefix() throws Exception {
    TimelineFilterList list =
        new TimelineFilterList(Operator.OR,
            new TimelinePrefixFilter(TimelineCompareOp.EQUAL, "MAP1_"));
    Set<String> metricFilters = ImmutableSet.of("MAP1_SLOT_MILLIS");
    Set<TimelineEntity> entities = reader.getEntities(
        new TimelineReaderContext("cluster1", "user1", "some_flow_name",
        1002345678919L, null, TimelineEntityType.YARN_APPLICATION.toString(),
        null),
        new TimelineEntityFilters(null, null, null, null, null, null, null,
        metricFilters, null),
        new TimelineDataToRetrieve(null, list, null));
    int metricCnt = 0;
    assertEquals(1, entities.size());
    for (TimelineEntity entity : entities) {
      metricCnt += entity.getMetrics().size();
    }
    assertEquals(1, metricCnt);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
