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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric.Type;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Unit test HBaseTimelineWriterImpl
 * YARN 3411
 *
 * @throws Exception
 */
public class TestHBaseTimelineWriterImpl {

  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
    createSchema();
  }

  private static void createSchema() throws IOException {
    byte[][] families = new byte[3][];
    families[0] = EntityColumnFamily.INFO.getInBytes();
    families[1] = EntityColumnFamily.CONFIG.getInBytes();
    families[2] = EntityColumnFamily.METRICS.getInBytes();
    TimelineSchemaCreator.createTimelineEntityTable(util.getHBaseAdmin(),
        util.getConfiguration());
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
    Long mTime = 1425026901000L;
    entity.setCreatedTime(cTime);
    entity.setModifiedTime(mTime);

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
    metricValues.put(1429741609000L, 100000000);
    metricValues.put(1429742609000L, 200000000);
    metricValues.put(1429743609000L, 300000000);
    metricValues.put(1429744609000L, 400000000);
    metricValues.put(1429745609000L, 50000000000L);
    metricValues.put(1429746609000L, 60000000000L);
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
      String cluster = "cluster1";
      String user = "user1";
      String flow = "some_flow_name";
      String flowVersion = "AB7822C10F1111";
      long runid = 1002345678919L;
      String appName = "some app name";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.stop();

      // scan the table and see that entity exists
      Scan s = new Scan();
      byte[] startRow = TimelineWriterUtils.getRowKeyPrefix(cluster, user, flow,
          runid, appName);
      s.setStartRow(startRow);
      s.setMaxVersions(Integer.MAX_VALUE);
      ResultScanner scanner = null;
      TableName entityTableName = TableName
          .valueOf(TimelineEntitySchemaConstants.DEFAULT_ENTITY_TABLE_NAME);
      Connection conn = ConnectionFactory.createConnection(c1);
      Table entityTable = conn.getTable(entityTableName);
      int rowCount = 0;
      int colCount = 0;
      scanner = entityTable.getScanner(s);
      for (Result result : scanner) {
        if (result != null && !result.isEmpty()) {
          rowCount++;
          colCount += result.size();
          byte[] row1 = result.getRow();
          assertTrue(isRowKeyCorrect(row1, cluster, user, flow, runid, appName,
              entity));

          // check info column family
          NavigableMap<byte[], byte[]> infoValues = result
              .getFamilyMap(EntityColumnFamily.INFO.getInBytes());
          String id1 = TimelineWriterUtils.getValueAsString(
              EntityColumnDetails.ID.getInBytes(), infoValues);
          assertEquals(id, id1);
          String type1 = TimelineWriterUtils.getValueAsString(
              EntityColumnDetails.TYPE.getInBytes(), infoValues);
          assertEquals(type, type1);
          Long cTime1 = TimelineWriterUtils.getValueAsLong(
              EntityColumnDetails.CREATED_TIME.getInBytes(), infoValues);
          assertEquals(cTime1, cTime);
          Long mTime1 = TimelineWriterUtils.getValueAsLong(
              EntityColumnDetails.MODIFIED_TIME.getInBytes(), infoValues);
          assertEquals(mTime1, mTime);
          checkRelatedEntities(isRelatedTo, infoValues,
              EntityColumnDetails.PREFIX_IS_RELATED_TO.getInBytes());
          checkRelatedEntities(relatesTo, infoValues,
              EntityColumnDetails.PREFIX_RELATES_TO.getInBytes());

          // check config column family
          NavigableMap<byte[], byte[]> configValuesResult = result
              .getFamilyMap(EntityColumnFamily.CONFIG.getInBytes());
          checkConfigs(configValuesResult, conf);

          NavigableMap<byte[], byte[]> metricsResult = result
              .getFamilyMap(EntityColumnFamily.METRICS.getInBytes());
          checkMetricsSizeAndKey(metricsResult, metrics);
          List<Cell> metricCells = result.getColumnCells(
              EntityColumnFamily.METRICS.getInBytes(),
              Bytes.toBytes(m1.getId()));
          checkMetricsTimeseries(metricCells, m1);
        }
      }
      assertEquals(1, rowCount);
      assertEquals(15, colCount);

    } finally {
      hbi.stop();
      hbi.close();
    }
  }

  private void checkMetricsTimeseries(List<Cell> metricCells,
      TimelineMetric m1) throws IOException {
    Map<Long, Number> timeseries = m1.getValues();
    assertEquals(metricCells.size(), timeseries.size());
    for (Cell c1 : metricCells) {
      assertTrue(timeseries.containsKey(c1.getTimestamp()));
      assertEquals(GenericObjectMapper.read(CellUtil.cloneValue(c1)),
          timeseries.get(c1.getTimestamp()));
    }
  }

  private void checkMetricsSizeAndKey(
      NavigableMap<byte[], byte[]> metricsResult, Set<TimelineMetric> metrics) {
    assertEquals(metrics.size(), metricsResult.size());
    for (TimelineMetric m1 : metrics) {
      byte[] key = Bytes.toBytes(m1.getId());
      assertTrue(metricsResult.containsKey(key));
    }
  }

  private void checkConfigs(NavigableMap<byte[], byte[]> configValuesResult,
      Map<String, String> conf) throws IOException {

    assertEquals(conf.size(), configValuesResult.size());
    byte[] columnName;
    for (String key : conf.keySet()) {
      columnName = Bytes.toBytes(key);
      assertTrue(configValuesResult.containsKey(columnName));
      byte[] value = configValuesResult.get(columnName);
      assertNotNull(value);
      assertEquals(conf.get(key), GenericObjectMapper.read(value));
    }
  }

  private void checkRelatedEntities(Map<String, Set<String>> isRelatedTo,
      NavigableMap<byte[], byte[]> infoValues, byte[] columnPrefix)
      throws IOException {

    for (String key : isRelatedTo.keySet()) {
      byte[] columnName = TimelineWriterUtils.join(
          TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES, columnPrefix,
          Bytes.toBytes(key));

      byte[] value = infoValues.get(columnName);
      assertNotNull(value);
      String isRelatedToEntities = GenericObjectMapper.read(value).toString();
      assertNotNull(isRelatedToEntities);
      assertEquals(
          TimelineWriterUtils.getValueAsString(
              TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR,
              isRelatedTo.get(key)), isRelatedToEntities);
    }
  }

  private boolean isRowKeyCorrect(byte[] rowKey, String cluster, String user,
      String flow, Long runid, String appName, TimelineEntity te) {

    byte[][] rowKeyComponents = TimelineWriterUtils.split(rowKey,
        TimelineEntitySchemaConstants.ROW_KEY_SEPARATOR_BYTES);

    assertTrue(rowKeyComponents.length == 7);
    assertEquals(user, Bytes.toString(rowKeyComponents[0]));
    assertEquals(cluster, Bytes.toString(rowKeyComponents[1]));
    assertEquals(flow, Bytes.toString(rowKeyComponents[2]));
    assertEquals(TimelineWriterUtils.encodeRunId(runid),
        Bytes.toLong(rowKeyComponents[3]));
    assertEquals(TimelineWriterUtils.cleanse(appName), Bytes.toString(rowKeyComponents[4]));
    assertEquals(te.getType(), Bytes.toString(rowKeyComponents[5]));
    assertEquals(te.getId(), Bytes.toString(rowKeyComponents[6]));
    return true;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
