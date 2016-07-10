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
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.collector.TimelineCollectorContext;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.OfflineAggregationInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class TestPhoenixOfflineAggregationWriterImpl extends BaseTest {
  private static PhoenixOfflineAggregationWriterImpl storage;
  private static final int BATCH_SIZE = 3;

  @BeforeClass
  public static void setup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    storage = setupPhoenixClusterAndWriterForTest(conf);
  }

  @Test(timeout = 90000)
  public void testFlowLevelAggregationStorage() throws Exception {
    testAggregator(OfflineAggregationInfo.FLOW_AGGREGATION);
  }

  @Test(timeout = 90000)
  public void testUserLevelAggregationStorage() throws Exception {
    testAggregator(OfflineAggregationInfo.USER_AGGREGATION);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    storage.dropTable(OfflineAggregationInfo.FLOW_AGGREGATION_TABLE_NAME);
    storage.dropTable(OfflineAggregationInfo.USER_AGGREGATION_TABLE_NAME);
    tearDownMiniCluster();
  }

  private static PhoenixOfflineAggregationWriterImpl
      setupPhoenixClusterAndWriterForTest(YarnConfiguration conf)
      throws Exception {
    Map<String, String> props = new HashMap<>();
    // Must update config before starting server
    props.put(QueryServices.STATS_USE_CURRENT_TIME_ATTRIB,
        Boolean.FALSE.toString());
    props.put("java.security.krb5.realm", "");
    props.put("java.security.krb5.kdc", "");
    props.put(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER,
        Boolean.FALSE.toString());
    props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(5000));
    props.put(IndexWriterUtils.HTABLE_THREAD_KEY, Integer.toString(100));
    // Make a small batch size to test multiple calls to reserve sequences
    props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB,
        Long.toString(BATCH_SIZE));
    // Must update config before starting server
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));

    // Change connection settings for test
    conf.set(
        YarnConfiguration.PHOENIX_OFFLINE_STORAGE_CONN_STR,
        getUrl());
    PhoenixOfflineAggregationWriterImpl
        myWriter = new PhoenixOfflineAggregationWriterImpl(TEST_PROPERTIES);
    myWriter.init(conf);
    myWriter.start();
    myWriter.createPhoenixTables();
    return myWriter;
  }

  private static TimelineEntity getTestAggregationTimelineEntity() {
    TimelineEntity entity = new TimelineEntity();
    String id = "hello1";
    String type = "testAggregationType";
    entity.setId(id);
    entity.setType(type);
    entity.setCreatedTime(1425016501000L);

    TimelineMetric metric = new TimelineMetric();
    metric.setId("HDFS_BYTES_READ");
    metric.addValue(1425016501100L, 8000);
    entity.addMetric(metric);

    return entity;
  }

  private void testAggregator(OfflineAggregationInfo aggregationInfo)
      throws Exception {
    // Set up a list of timeline entities and write them back to Phoenix
    int numEntity = 1;
    TimelineEntities te = new TimelineEntities();
    te.addEntity(getTestAggregationTimelineEntity());
    TimelineCollectorContext context = new TimelineCollectorContext("cluster_1",
        "user1", "testFlow", null, 0L, null);
    storage.writeAggregatedEntity(context, te,
        aggregationInfo);

    // Verify if we're storing all entities
    String[] primaryKeyList = aggregationInfo.getPrimaryKeyList();
    String sql = "SELECT COUNT(" + primaryKeyList[primaryKeyList.length - 1]
        +") FROM " + aggregationInfo.getTableName();
    verifySQLWithCount(sql, numEntity, "Number of entities should be ");
    // Check metric
    sql = "SELECT COUNT(m.HDFS_BYTES_READ) FROM "
        + aggregationInfo.getTableName() + "(m.HDFS_BYTES_READ VARBINARY) ";
    verifySQLWithCount(sql, numEntity,
        "Number of entities with info should be ");
  }


  private void verifySQLWithCount(String sql, int targetCount, String message)
      throws Exception {
    try (
        Statement stmt =
          storage.getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue("Result set empty on statement " + sql, rs.next());
      assertNotNull("Fail to execute query " + sql, rs);
      assertEquals(message + " " + targetCount, targetCount, rs.getInt(1));
    } catch (SQLException se) {
      fail("SQL exception on query: " + sql
          + " With exception message: " + se.getLocalizedMessage());
    }
  }
}
