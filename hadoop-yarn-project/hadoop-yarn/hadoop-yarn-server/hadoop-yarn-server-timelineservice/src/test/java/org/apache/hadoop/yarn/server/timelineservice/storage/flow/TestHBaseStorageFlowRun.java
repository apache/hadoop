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
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineSchemaCreator;
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
    long minStartTs = 10000000000000L;
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
    long starttime = (Long) GenericObjectMapper.read(values
        .get(FlowRunColumn.MIN_START_TIME.getColumnQualifierBytes()));
    assertEquals(minStartTs, starttime);
    assertEquals(endTs, GenericObjectMapper.read(values
        .get(FlowRunColumn.MAX_END_TIME.getColumnQualifierBytes())));

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      // get the flow run entity
      TimelineEntity entity =
          hbr.getEntity(user, cluster, flow, runid, null,
              TimelineEntityType.YARN_FLOW_RUN.toString(), null, null);
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
    TimelineEntity entityApp1 = TestFlowDataGenerator.getEntityMetricsApp1();
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
      TimelineEntity entityApp2 = TestFlowDataGenerator.getEntityMetricsApp2();
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
      TimelineEntity entity =
          hbr.getEntity(user, cluster, flow, runid, null,
            TimelineEntityType.YARN_FLOW_RUN.toString(), null, null);
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
          assertEquals(141, value);
          break;
        case metric2:
          assertEquals(57, value);
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
      assertEquals(141, GenericObjectMapper.read(values.get(q)));

      // check metric2
      assertEquals(2, values.size());
      q = ColumnHelper.getColumnQualifier(
          FlowRunColumnPrefix.METRIC.getColumnPrefixBytes(), metric2);
      assertTrue(values.containsKey(q));
      assertEquals(57, GenericObjectMapper.read(values.get(q)));
    }
    assertEquals(1, rowCount);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
