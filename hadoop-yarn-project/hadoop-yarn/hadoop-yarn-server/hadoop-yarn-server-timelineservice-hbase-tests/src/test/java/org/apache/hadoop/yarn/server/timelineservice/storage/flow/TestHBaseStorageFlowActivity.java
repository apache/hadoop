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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntityType;
import org.apache.hadoop.yarn.server.timeline.GenericObjectMapper;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineDataToRetrieve;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineEntityFilters;
import org.apache.hadoop.yarn.server.timelineservice.reader.TimelineReaderContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.DataGeneratorForTest;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineReaderImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.HBaseTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.BaseTableRW;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ColumnHelper;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineSchemaUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the FlowRun and FlowActivity Tables.
 */
public class TestHBaseStorageFlowActivity {

  private static HBaseTestingUtility util;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    util.startMiniCluster();
    DataGeneratorForTest.createSchema(util.getConfiguration());
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
   * The test also checks in the flow activity table that one entry has been
   * made for all of these 4 application entities since they belong to the same
   * flow run.
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
    long minStartTs = 1424995200300L;
    long greaterStartTs = 1424995200300L + 864000L;
    long endTs = 1424995200300L + 86000000L;
    TimelineEntity entityMinStartTime = TestFlowDataGenerator
        .getEntityMinStartTime(minStartTs);

    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);

      UserGroupInformation remoteUser =
          UserGroupInformation.createRemoteUser(user);
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te, remoteUser);

      // write another entity with the right min start time
      te = new TimelineEntities();
      te.addEntity(entityMinStartTime);
      appName = "application_100000000000_3333";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te, remoteUser);

      // writer another entity for max end time
      TimelineEntity entityMaxEndTime = TestFlowDataGenerator
          .getEntityMaxEndTime(endTs);
      te = new TimelineEntities();
      te.addEntity(entityMaxEndTime);
      appName = "application_100000000000_4444";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te, remoteUser);

      // writer another entity with greater start time
      TimelineEntity entityGreaterStartTime = TestFlowDataGenerator
          .getEntityGreaterStartTime(greaterStartTs);
      te = new TimelineEntities();
      te.addEntity(entityGreaterStartTime);
      appName = "application_1000000000000000_2222";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te, remoteUser);

      // flush everything to hbase
      hbi.flush();
    } finally {
      if (hbi != null) {
        hbi.close();
      }
    }

    Connection conn = ConnectionFactory.createConnection(c1);
    // check in flow activity table
    Table table1 = conn.getTable(
        BaseTableRW.getTableName(c1, FlowActivityTableRW.TABLE_NAME_CONF_NAME,
            FlowActivityTableRW.DEFAULT_TABLE_NAME));
    byte[] startRow =
        new FlowActivityRowKey(cluster, minStartTs, user, flow).getRowKey();
    Get g = new Get(startRow);
    Result r1 = table1.get(g);
    assertNotNull(r1);
    assertTrue(!r1.isEmpty());
    Map<byte[], byte[]> values = r1.getFamilyMap(FlowActivityColumnFamily.INFO
        .getBytes());
    assertEquals(1, values.size());
    byte[] row = r1.getRow();
    FlowActivityRowKey flowActivityRowKey = FlowActivityRowKey.parseRowKey(row);
    assertNotNull(flowActivityRowKey);
    assertEquals(cluster, flowActivityRowKey.getClusterId());
    assertEquals(user, flowActivityRowKey.getUserId());
    assertEquals(flow, flowActivityRowKey.getFlowName());
    Long dayTs = HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(minStartTs);
    assertEquals(dayTs, flowActivityRowKey.getDayTimestamp());
    assertEquals(1, values.size());
    checkFlowActivityRunId(runid, flowVersion, values);

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();
      // get the flow activity entity
      Set<TimelineEntity> entities = hbr.getEntities(
          new TimelineReaderContext(cluster, null, null, null, null,
          TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null),
          new TimelineEntityFilters.Builder().entityLimit(10L).build(),
          new TimelineDataToRetrieve());
      assertEquals(1, entities.size());
      for (TimelineEntity e : entities) {
        FlowActivityEntity flowActivity = (FlowActivityEntity)e;
        assertEquals(cluster, flowActivity.getCluster());
        assertEquals(user, flowActivity.getUser());
        assertEquals(flow, flowActivity.getFlowName());
        assertEquals(dayTs, Long.valueOf(flowActivity.getDate().getTime()));
        Set<FlowRunEntity> flowRuns = flowActivity.getFlowRuns();
        assertEquals(1, flowRuns.size());
      }
    } finally {
      if (hbr != null) {
        hbr.close();
      }
    }
  }

  /**
   * Write 1 application entity and checks the record for today in the flow
   * activity table.
   */
  @Test
  public void testWriteFlowActivityOneFlow() throws Exception {
    String cluster = "testWriteFlowActivityOneFlow_cluster1";
    String user = "testWriteFlowActivityOneFlow_user1";
    String flow = "flow_activity_test_flow_name";
    String flowVersion = "A122110F135BC4";
    long runid = 1001111178919L;

    TimelineEntities te = new TimelineEntities();
    long appCreatedTime = 1425016501000L;
    TimelineEntity entityApp1 =
        TestFlowDataGenerator.getFlowApp1(appCreatedTime);
    te.addEntity(entityApp1);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);
      String appName = "application_1111999999_1234";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion,
          runid, appName), te, UserGroupInformation.createRemoteUser(user));
      hbi.flush();
    } finally {
      if (hbi != null) {
        hbi.close();
      }
    }
    // check flow activity
    checkFlowActivityTable(cluster, user, flow, flowVersion, runid, c1,
        appCreatedTime);

    // use the reader to verify the data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();

      Set<TimelineEntity> entities = hbr.getEntities(
          new TimelineReaderContext(cluster, user, flow, null, null,
          TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null),
          new TimelineEntityFilters.Builder().entityLimit(10L).build(),
          new TimelineDataToRetrieve());
      assertEquals(1, entities.size());
      for (TimelineEntity e : entities) {
        FlowActivityEntity entity = (FlowActivityEntity)e;
        NavigableSet<FlowRunEntity> flowRuns = entity.getFlowRuns();
        assertEquals(1, flowRuns.size());
        for (FlowRunEntity flowRun : flowRuns) {
          assertEquals(runid, flowRun.getRunId());
          assertEquals(flowVersion, flowRun.getVersion());
        }
      }
    } finally {
      if (hbr != null) {
        hbr.close();
      }
    }
  }

  private void checkFlowActivityTable(String cluster, String user, String flow,
      String flowVersion, long runid, Configuration c1, long appCreatedTime)
          throws IOException {
    Scan s = new Scan();
    s.addFamily(FlowActivityColumnFamily.INFO.getBytes());
    byte[] startRow =
        new FlowActivityRowKey(cluster, appCreatedTime, user, flow).getRowKey();
    s.setStartRow(startRow);
    String clusterStop = cluster + "1";
    byte[] stopRow =
        new FlowActivityRowKey(clusterStop, appCreatedTime, user, flow)
            .getRowKey();
    s.setStopRow(stopRow);
    Connection conn = ConnectionFactory.createConnection(c1);
    Table table1 = conn.getTable(
        BaseTableRW.getTableName(c1, FlowActivityTableRW.TABLE_NAME_CONF_NAME,
            FlowActivityTableRW.DEFAULT_TABLE_NAME));
    ResultScanner scanner = table1.getScanner(s);
    int rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowActivityColumnFamily.INFO.getBytes());
      rowCount++;
      byte[] row = result.getRow();
      FlowActivityRowKey flowActivityRowKey = FlowActivityRowKey
          .parseRowKey(row);
      assertNotNull(flowActivityRowKey);
      assertEquals(cluster, flowActivityRowKey.getClusterId());
      assertEquals(user, flowActivityRowKey.getUserId());
      assertEquals(flow, flowActivityRowKey.getFlowName());
      Long dayTs = HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(
          appCreatedTime);
      assertEquals(dayTs, flowActivityRowKey.getDayTimestamp());
      assertEquals(1, values.size());
      checkFlowActivityRunId(runid, flowVersion, values);
    }
    assertEquals(1, rowCount);
  }

  /**
   * Writes 3 applications each with a different run id and version for the same
   * {cluster, user, flow}.
   *
   * They should be getting inserted into one record in the flow activity table
   * with 3 columns, one per run id.
   */
  @Test
  public void testFlowActivityTableOneFlowMultipleRunIds() throws IOException {
    String cluster = "testManyRunsFlowActivity_cluster1";
    String user = "testManyRunsFlowActivity_c_user1";
    String flow = "flow_activity_test_flow_name";
    String flowVersion1 = "A122110F135BC4";
    long runid1 = 11111111111L;

    String flowVersion2 = "A12222222222C4";
    long runid2 = 2222222222222L;

    String flowVersion3 = "A1333333333C4";
    long runid3 = 3333333333333L;

    TimelineEntities te = new TimelineEntities();
    long appCreatedTime = 1425016501000L;
    TimelineEntity entityApp1 =
        TestFlowDataGenerator.getFlowApp1(appCreatedTime);
    te.addEntity(entityApp1);

    HBaseTimelineWriterImpl hbi = null;
    Configuration c1 = util.getConfiguration();
    try {
      hbi = new HBaseTimelineWriterImpl();
      hbi.init(c1);

      UserGroupInformation remoteUser =
          UserGroupInformation.createRemoteUser(user);

      String appName = "application_11888888888_1111";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion1,
          runid1, appName), te, remoteUser);

      // write an application with to this flow but a different runid/ version
      te = new TimelineEntities();
      te.addEntity(entityApp1);
      appName = "application_11888888888_2222";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion2,
          runid2, appName), te, remoteUser);

      // write an application with to this flow but a different runid/ version
      te = new TimelineEntities();
      te.addEntity(entityApp1);
      appName = "application_11888888888_3333";
      hbi.write(new TimelineCollectorContext(cluster, user, flow, flowVersion3,
          runid3, appName), te, remoteUser);

      hbi.flush();
    } finally {
      if (hbi != null) {
        hbi.close();
      }
    }
    // check flow activity
    checkFlowActivityTableSeveralRuns(cluster, user, flow, c1, flowVersion1,
        runid1, flowVersion2, runid2, flowVersion3, runid3, appCreatedTime);

    // use the timeline reader to verify data
    HBaseTimelineReaderImpl hbr = null;
    try {
      hbr = new HBaseTimelineReaderImpl();
      hbr.init(c1);
      hbr.start();

      Set<TimelineEntity> entities = hbr.getEntities(
          new TimelineReaderContext(cluster, null, null, null, null,
          TimelineEntityType.YARN_FLOW_ACTIVITY.toString(), null),
          new TimelineEntityFilters.Builder().entityLimit(10L).build(),
          new TimelineDataToRetrieve());
      assertEquals(1, entities.size());
      for (TimelineEntity e : entities) {
        FlowActivityEntity flowActivity = (FlowActivityEntity)e;
        assertEquals(cluster, flowActivity.getCluster());
        assertEquals(user, flowActivity.getUser());
        assertEquals(flow, flowActivity.getFlowName());
        long dayTs =
            HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(appCreatedTime);
        assertEquals(dayTs, flowActivity.getDate().getTime());
        Set<FlowRunEntity> flowRuns = flowActivity.getFlowRuns();
        assertEquals(3, flowRuns.size());
        for (FlowRunEntity flowRun : flowRuns) {
          long runId = flowRun.getRunId();
          String version = flowRun.getVersion();
          if (runId == runid1) {
            assertEquals(flowVersion1, version);
          } else if (runId == runid2) {
            assertEquals(flowVersion2, version);
          } else if (runId == runid3) {
            assertEquals(flowVersion3, version);
          } else {
            fail("unknown run id: " + runId);
          }
        }
      }
    } finally {
      if (hbr != null) {
        hbr.close();
      }
    }
  }

  private void checkFlowActivityTableSeveralRuns(String cluster, String user,
      String flow, Configuration c1, String flowVersion1, long runid1,
      String flowVersion2, long runid2, String flowVersion3, long runid3,
      long appCreatedTime)
      throws IOException {
    Scan s = new Scan();
    s.addFamily(FlowActivityColumnFamily.INFO.getBytes());
    byte[] startRow =
        new FlowActivityRowKey(cluster, appCreatedTime, user, flow).getRowKey();
    s.setStartRow(startRow);
    String clusterStop = cluster + "1";
    byte[] stopRow = new FlowActivityRowKey(clusterStop, appCreatedTime, user,
        flow).getRowKey();
    s.setStopRow(stopRow);
    Connection conn = ConnectionFactory.createConnection(c1);
    Table table1 = conn.getTable(
        BaseTableRW.getTableName(c1, FlowActivityTableRW.TABLE_NAME_CONF_NAME,
            FlowActivityTableRW.DEFAULT_TABLE_NAME));
    ResultScanner scanner = table1.getScanner(s);
    int rowCount = 0;
    for (Result result : scanner) {
      assertNotNull(result);
      assertTrue(!result.isEmpty());
      byte[] row = result.getRow();
      FlowActivityRowKey flowActivityRowKey = FlowActivityRowKey
          .parseRowKey(row);
      assertNotNull(flowActivityRowKey);
      assertEquals(cluster, flowActivityRowKey.getClusterId());
      assertEquals(user, flowActivityRowKey.getUserId());
      assertEquals(flow, flowActivityRowKey.getFlowName());
      Long dayTs = HBaseTimelineSchemaUtils.getTopOfTheDayTimestamp(
          appCreatedTime);
      assertEquals(dayTs, flowActivityRowKey.getDayTimestamp());

      Map<byte[], byte[]> values = result
          .getFamilyMap(FlowActivityColumnFamily.INFO.getBytes());
      rowCount++;
      assertEquals(3, values.size());
      checkFlowActivityRunId(runid1, flowVersion1, values);
      checkFlowActivityRunId(runid2, flowVersion2, values);
      checkFlowActivityRunId(runid3, flowVersion3, values);
    }
    // the flow activity table is such that it will insert
    // into current day's record
    // hence, if this test runs across the midnight boundary,
    // it may fail since it would insert into two records
    // one for each day
    assertEquals(1, rowCount);
  }

  private void checkFlowActivityRunId(long runid, String flowVersion,
      Map<byte[], byte[]> values) throws IOException {
    byte[] rq = ColumnHelper.getColumnQualifier(
        FlowActivityColumnPrefix.RUN_ID.getColumnPrefixBytes(),
        GenericObjectMapper.write(runid));
    for (Map.Entry<byte[], byte[]> k : values.entrySet()) {
      String actualQ = Bytes.toString(k.getKey());
      if (Bytes.toString(rq).equals(actualQ)) {
        String actualV = (String) GenericObjectMapper.read(k.getValue());
        assertEquals(flowVersion, actualV);
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
}
