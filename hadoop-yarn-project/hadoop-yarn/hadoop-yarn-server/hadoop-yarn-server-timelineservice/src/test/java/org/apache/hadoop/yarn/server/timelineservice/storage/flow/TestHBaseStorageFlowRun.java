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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineCompareOp;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelineFilterList;
import org.apache.hadoop.yarn.server.timelineservice.reader.filter.TimelinePrefixFilter;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineSchemaCreator;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineReader.Field;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the FlowRun and FlowActivity Tables
 */
public class TestHBaseStorageFlowRun {

  private static HBaseTestingUtility util;

  private final String metric1 = "MAP_SLOT_MILLIS";
  private final String metric2 = "HDFS_BYTES_READ";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
    createSchema();
  }

  private static void createSchema() throws IOException {
    TimelineSchemaCreator.createAllTables(util.getConfiguration(), false);
  }

  /**
   * Writes 4 timeline entities belonging to one flow run through the
   * {@link HBaseTimelineWriterImpl}
   *
   * Checks the flow run table contents
   *
   * The first entity has a created event, metrics and a finish event.
   *
   * The second entity has a created event and this is the entity with smallest
   * start time. This should be the start time for the flow run.
   *
   * The third entity has a finish event and this is the entity with the max end
   * time. This should be the end time for the flow run.
   *
   * The fourth entity has a created event which has a start time that is
   * greater than min start time.
   *
   */
  @Test
  public void testWriteFlowRunMinMax() throws Exception {

    TimelineEntities te = new TimelineEntities();
    te.addEntity(TestFlowDataGenerator.getEntity1());

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    String cluster = "testWriteFlowRunMinMaxToHBase_cluster1";
    String user = "testWriteFlowRunMinMaxToHBase_user1";
    String flow = "testing_flowRun_flow_name";
    String flowVersion = "CF7022C10F1354";
    long runid = 1002345678919L;
    String appName = "application_100000000000_1111";
    long minStartTs = 1425026900000L;
    long greaterStartTs = 30000000000000L;
    long endTs = 1439750690000L;
    TimelineEntity entityMinStartTime = TestFlowDataGenerator
        .getEntityMinStartTime(minStartTs);

    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);

      // write another entity with the right min start time
      te = new TimelineEntities();
      te.addEntity(entityMinStartTime);
      appName = "application_100000000000_3333";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);

      // writer another entity for max end time
      TimelineEntity entityMaxEndTime = TestFlowDataGenerator
          .getEntityMaxEndTime(endTs);
      te = new TimelineEntities();
      te.addEntity(entityMaxEndTime);
      appName = "application_100000000000_4444";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);

      // writer another entity with greater start time
      TimelineEntity entityGreaterStartTime = TestFlowDataGenerator
          .getEntityGreaterStartTime(greaterStartTs);
      te = new TimelineEntities();
      te.addEntity(entityGreaterStartTime);
      appName = "application_1000000000000000_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);

      // flush everything to hbase
      hbi.flush();
    } finally {
      hbi.close();
    }

    Connection conn = ConnectionFactory.createConnection(c1);
    // check in flow run table
    Table table1 = conn.getTable(TableName
        .valueOf(FlowRunTable.DEFAULT_TABLE_NAME));
    // scan the table and see that we get back the right min and max
    // timestamps
    byte[] startRow = FlowRunRowKey.getRowKey(cluster, user, flow, runid);
    Get g = new Get(startRow);
    g.addColumn(FlowRunColumnFamily.INFO.getBytes(),
        FlowRunColumn.MIN_START_TIME.getColumnQualifierBytes());
    g.addColumn(FlowRunColumnFamily.INFO.getBytes(),
        FlowRunColumn.MAX_END_TIME.getColumnQualifierBytes());
    Result r1 = table1.get(g);
    assertNotNull(r1);
    assertTrue(!r1.isEmpty());
    Map<byte[], byte[]> values = r1.getFamilyMap(FlowRunColumnFamily.INFO
        .getBytes());

    assertEquals(2, r1.size());
    long starttime = Bytes.toLong(values.get(
        FlowRunColumn.MIN_START_TIME.getColumnQualifierBytes()));
    assertEquals(minStartTs, starttime);
    assertEquals(endTs, Bytes.toLong(values
        .get(FlowRunColumn.MAX_END_TIME.getColumnQualifierBytes())));

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      // get the flow run entity
      TimelineEntity entity = hbr.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineDataToRetrieve());
      assertTrue(TimelineEntityType.YARN_FLOW_RUN.matches(entity.getType()));
      FlowRunEntity flowRun = (FlowRunEntity)entity;
      assertEquals(minStartTs, flowRun.getStartTime());
      assertEquals(endTs, flowRun.getMaxEndTime());
    } finally {
      hbr.close();
    }
  }

  /**
   * Writes two application entities of the same flow run. Each application has
   * two metrics: slot millis and hdfs bytes read. Each metric has values at two
   * timestamps.
   *
   * Checks the metric values of the flow in the flow run table. Flow metric
   * values should be the sum of individual metric values that belong to the
   * latest timestamp for that metric
   */
  @Test
  public void testWriteFlowRunMetricsOneFlow() throws Exception {
    String cluster = "testWriteFlowRunMetricsOneFlow_cluster1";
    String user = "testWriteFlowRunMetricsOneFlow_user1";
    String flow = "testing_flowRun_metrics_flow_name";
    String flowVersion = "CF7022C10F1354";
    long runid = 1002345678919L;

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entityApp1 = TestFlowDataGenerator
        .getEntityMetricsApp1(System.currentTimeMillis());
    te.addEntity(entityApp1);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      String appName = "application_11111111111111_1111";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      // write another application with same metric to this flow
      te = new TimelineEntities();
      TimelineEntity entityApp2 = TestFlowDataGenerator
          .getEntityMetricsApp2(System.currentTimeMillis());
      te.addEntity(entityApp2);
      appName = "application_11111111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.flush();
    } finally {
      hbi.close();
    }

    // check flow run
    checkFlowRunTable(cluster, user, flow, runid, c1);

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      TimelineEntity entity = hbr.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineDataToRetrieve());
      assertTrue(TimelineEntityType.YARN_FLOW_RUN.matches(entity.getType()));
      Set<TimelineMetric> metrics = entity.getMetrics();
      assertEquals(2, metrics.size());
      for (TimelineMetric metric : metrics) {
        String id = metric.getId();
        Map<Long, Number> values = metric.getValues();
        assertEquals(1, values.size());
        Number value = null;
        for (Number n : values.values()) {
          value = n;
        }
        switch (id) {
        case metric1:
          assertEquals(141L, value);
          break;
        case metric2:
          assertEquals(57L, value);
          break;
        default:
          fail("unrecognized metric: " + id);
        }
      }
    } finally {
      hbr.close();
    }
  }

  private void checkFlowRunTable(String cluster, String user, String flow,
      long runid, Configuration c1) throws IOException {
    Scan s = new Scan();
    s.addFamily(FlowRunColumnFamily.INFO.getBytes());
    byte[] startRow = FlowRunRowKey.getRowKey(cluster, user, flow, runid);
    s.setStartRow(startRow);
    String clusterStop = cluster + "1";
    byte[] stopRow = FlowRunRowKey.getRowKey(clusterStop, user, flow, runid);
    s.setStopRow(stopRow);
    Connection conn = ConnectionFactory.createConnection(c1);
    Table table1 = conn.getTable(TableName
        .valueOf(FlowRunTable.DEFAULT_TABLE_NAME));
    ResultScanner scanner = table1.getScanner(s);

    int rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      Map<byte[], byte[]> values = result.getFamilyMap(FlowRunColumnFamily.INFO
          .getBytes());
      rowCount++;
      // check metric1
      byte[] q = ColumnHelper.getColumnQualifier(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(), metric1);
      assertTrue(values.containsKey(q));
      assertEquals(141L, Bytes.toLong(values.get(q)));

      // check metric2
      assertEquals(2, values.size());
      q = ColumnHelper.getColumnQualifier(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(), metric2);
      assertTrue(values.containsKey(q));
      assertEquals(57L, Bytes.toLong(values.get(q)));
    }
    assertEquals(1, rowCount);
  }

  @Test
  public void testWriteFlowRunMetricsPrefix() throws Exception {
    String cluster = "testWriteFlowRunMetricsOneFlow_cluster1";
    String user = "testWriteFlowRunMetricsOneFlow_user1";
    String flow = "testing_flowRun_metrics_flow_name";
    String flowVersion = "CF7022C10F1354";
    long runid = 1002345678919L;

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entityApp1 = TestFlowDataGenerator
        .getEntityMetricsApp1(System.currentTimeMillis());
    te.addEntity(entityApp1);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      String appName = "application_11111111111111_1111";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      // write another application with same metric to this flow
      te = new TimelineEntities();
      TimelineEntity entityApp2 = TestFlowDataGenerator
          .getEntityMetricsApp2(System.currentTimeMillis());
      te.addEntity(entityApp2);
      appName = "application_11111111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.flush();
    } finally {
      hbi.close();
    }

    // check flow run
    checkFlowRunTable(cluster, user, flow, runid, c1);

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      TimelineFilterList metricsToRetrieve =
          new TimelineFilterList(new TimelinePrefixFilter(TimelineCompareOp.EQUAL,
          metric1.substring(0, metric1.indexOf("_") + 1)));
      TimelineEntity entity = hbr.getEntity(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineDataToRetrieve(null, metricsToRetrieve, null));
      assertTrue(TimelineEntityType.YARN_FLOW_RUN.matches(entity.getType()));
      Set<TimelineMetric> metrics = entity.getMetrics();
      assertEquals(1, metrics.size());
      for (TimelineMetric metric : metrics) {
        String id = metric.getId();
        Map<Long, Number> values = metric.getValues();
        assertEquals(1, values.size());
        Number value = null;
        for (Number n : values.values()) {
          value = n;
        }
        switch (id) {
        case metric1:
          assertEquals(141L, value);
          break;
        default:
          fail("unrecognized metric: " + id);
        }
      }

      Set<TimelineEntity> entities = hbr.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineEntityFilters(),
          new TimelineDataToRetrieve(null, metricsToRetrieve, null));
      assertEquals(1, entities.size());
      for (TimelineEntity timelineEntity : entities) {
        Set<TimelineMetric> timelineMetrics = timelineEntity.getMetrics();
        assertEquals(1, timelineMetrics.size());
        for (TimelineMetric metric : timelineMetrics) {
          String id = metric.getId();
          Map<Long, Number> values = metric.getValues();
          assertEquals(1, values.size());
          Number value = null;
          for (Number n : values.values()) {
            value = n;
          }
          switch (id) {
          case metric1:
            assertEquals(141L, value);
            break;
          default:
            fail("unrecognized metric: " + id);
          }
        }
      }
    } finally {
      hbr.close();
    }
  }

  @Test
  public void testWriteFlowRunsMetricFields() throws Exception {
    String cluster = "testWriteFlowRunMetricsOneFlow_cluster1";
    String user = "testWriteFlowRunMetricsOneFlow_user1";
    String flow = "testing_flowRun_metrics_flow_name";
    String flowVersion = "CF7022C10F1354";
    long runid = 1002345678919L;

    TimelineEntities te = new TimelineEntities();
    TimelineEntity entityApp1 = TestFlowDataGenerator
        .getEntityMetricsApp1(System.currentTimeMillis());
    te.addEntity(entityApp1);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);
      String appName = "application_11111111111111_1111";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      // write another application with same metric to this flow
      te = new TimelineEntities();
      TimelineEntity entityApp2 = TestFlowDataGenerator
          .getEntityMetricsApp2(System.currentTimeMillis());
      te.addEntity(entityApp2);
      appName = "application_11111111111111_2222";
      hbi.write(cluster, user, flow, flowVersion, runid, appName, te);
      hbi.flush();
    } finally {
      hbi.close();
    }

    // check flow run
    checkFlowRunTable(cluster, user, flow, runid, c1);

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      Set<TimelineEntity> entities = hbr.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineEntityFilters(),
          new TimelineDataToRetrieve());
      assertEquals(1, entities.size());
      for (TimelineEntity timelineEntity : entities) {
        assertEquals(0, timelineEntity.getMetrics().size());
      }

      entities = hbr.getEntities(
          new TimelineReaderContext(cluster, user, flow, runid, null,
          TimelineEntityType.YARN_FLOW_RUN.toString(), null),
          new TimelineEntityFilters(),
          new TimelineDataToRetrieve(null, null, EnumSet.of(Field.METRICS)));
      assertEquals(1, entities.size());
      for (TimelineEntity timelineEntity : entities) {
        Set<TimelineMetric> timelineMetrics = timelineEntity.getMetrics();
        assertEquals(2, timelineMetrics.size());
        for (TimelineMetric metric : timelineMetrics) {
          String id = metric.getId();
          Map<Long, Number> values = metric.getValues();
          assertEquals(1, values.size());
          Number value = null;
          for (Number n : values.values()) {
            value = n;
          }
          switch (id) {
          case metric1:
            assertEquals(141L, value);
            break;
          case metric2:
            assertEquals(57L, value);
            break;
          default:
            fail("unrecognized metric: " + id);
          }
        }
      }
    } finally {
      hbr.close();
    }
  }

  @Test
  public void testWriteFlowRunFlush() throws Exception {
    String cluster = "atestFlushFlowRun_cluster1";
    String user = "atestFlushFlowRun__user1";
    String flow = "atestFlushFlowRun_flow_name";
    String flowVersion = "AF1021C19F1351";
    long runid = 1449526652000L;

    int start = 10;
    int count = 20000;
    int appIdSuffix = 1;
    HBaseTimelineWriterImpl hbi = null;
    long insertTs = 1449796654827L - count;
    long minTS = insertTs + 1;
    long startTs = insertTs;
    Configuration c1 = util.getConfiguration();
    TimelineEntities te1 = null;
    TimelineEntity entityApp1 = null;
    TimelineEntity entityApp2 = null;
    try {
      hbi = new HBaseTimelineWriterImpl(c1);
      hbi.init(c1);

      for (int i = start; i < count; i++) {
        String appName = "application_1060350000000_" + appIdSuffix;
        insertTs++;
        te1 = new TimelineEntities();
        entityApp1 = TestFlowDataGenerator.getMinFlushEntity(insertTs);
        te1.addEntity(entityApp1);
        entityApp2 = TestFlowDataGenerator.getMaxFlushEntity(insertTs);
        te1.addEntity(entityApp2);
        hbi.write(cluster, user, flow, flowVersion, runid, appName, te1);
        Thread.sleep(1);

        appName = "application_1001199480000_7" + appIdSuffix;
        insertTs++;
        appIdSuffix++;
        te1 = new TimelineEntities();
        entityApp1 = TestFlowDataGenerator.getMinFlushEntity(insertTs);
        te1.addEntity(entityApp1);
        entityApp2 = TestFlowDataGenerator.getMaxFlushEntity(insertTs);
        te1.addEntity(entityApp2);

        hbi.write(cluster, user, flow, flowVersion, runid, appName, te1);
        if (i % 1000 == 0) {
          hbi.flush();
          checkMinMaxFlush(c1, minTS, startTs, count, cluster, user, flow,
              runid, false);
        }
      }
    } finally {
      hbi.flush();
      hbi.close();
      checkMinMaxFlush(c1, minTS, startTs, count, cluster, user, flow, runid,
          true);
    }
  }

  private void checkMinMaxFlush(Configuration c1, long minTS, long startTs,
      int count, String cluster, String user, String flow, long runid,
      boolean checkMax) throws IOException {
    Connection conn = ConnectionFactory.createConnection(c1);
    // check in flow run table
    Table table1 = conn.getTable(TableName
        .valueOf(FlowRunTable.DEFAULT_TABLE_NAME));
    // scan the table and see that we get back the right min and max
    // timestamps
    byte[] startRow = FlowRunRowKey.getRowKey(cluster, user, flow, runid);
    Get g = new Get(startRow);
    g.addColumn(FlowRunColumnFamily.INFO.getBytes(),
        FlowRunColumn.MIN_START_TIME.getColumnQualifierBytes());
    g.addColumn(FlowRunColumnFamily.INFO.getBytes(),
        FlowRunColumn.MAX_END_TIME.getColumnQualifierBytes());

    Result r1 = table1.get(g);
    assertNotNull(r1);
    assertTrue(!r1.isEmpty());
    Map<byte[], byte[]> values = r1.getFamilyMap(FlowRunColumnFamily.INFO
        .getBytes());
    int start = 10;
    assertEquals(2, r1.size());
    long starttime = Bytes.toLong(values
        .get(FlowRunColumn.MIN_START_TIME.getColumnQualifierBytes()));
    assertEquals(minTS, starttime);
    if (checkMax) {
      assertEquals(startTs + 2 * (count - start)
          + TestFlowDataGenerator.END_TS_INCR,
          Bytes.toLong(values
          .get(FlowRunColumn.MAX_END_TIME.getColumnQualifierBytes())));
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
