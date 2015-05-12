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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class TestPhoenixTimelineWriterImpl extends BaseTest {
  private static PhoenixTimelineWriterImpl writer;
  private static final int BATCH_SIZE = 3;

  @BeforeClass
  public static void setup() throws Exception {
    YarnConfiguration conf = new YarnConfiguration();
    writer = setupPhoenixClusterAndWriterForTest(conf);
  }

  @Test(timeout = 90000)
  public void testPhoenixWriterBasic() throws Exception {
    // Set up a list of timeline entities and write them back to Phoenix
    int numEntity = 12;
    TimelineEntities te =
        TestTimelineWriterImpl.getStandardTestTimelineEntities(numEntity);
    writer.write("cluster_1", "user1", "testFlow", "version1", 1l, "app_test_1", te);
    // Verify if we're storing all entities
    String sql = "SELECT COUNT(entity_id) FROM "
        + PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME;
    verifySQLWithCount(sql, numEntity, "Number of entities should be ");
    // Check config (half of all entities)
    sql = "SELECT COUNT(c.config) FROM "
        + PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME + "(c.config VARCHAR) ";
    verifySQLWithCount(sql, (numEntity / 2),
        "Number of entities with config should be ");
    // Check info (half of all entities)
    sql = "SELECT COUNT(i.info1) FROM "
        + PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME + "(i.info1 VARBINARY) ";
    verifySQLWithCount(sql, (numEntity / 2),
        "Number of entities with info should be ");
    // Check config and info (a quarter of all entities)
    sql = "SELECT COUNT(entity_id) FROM "
        + PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME
        + "(c.config VARCHAR, i.info1 VARBINARY) "
        + "WHERE c.config IS NOT NULL AND i.info1 IS NOT NULL";
    verifySQLWithCount(sql, (numEntity / 4),
        "Number of entities with both config and info should be ");
    // Check relatesToEntities and isRelatedToEntities
    sql = "SELECT COUNT(entity_id) FROM "
        + PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME
        + "(rt.testType VARCHAR, ir.testType VARCHAR) "
        + "WHERE rt.testType IS NOT NULL AND ir.testType IS NOT NULL";
    verifySQLWithCount(sql, numEntity - 2,
        "Number of entities with both relatesTo and isRelatedTo should be ");
    // Check event
    sql = "SELECT COUNT(entity_id) FROM "
        + PhoenixTimelineWriterImpl.EVENT_TABLE_NAME;
    verifySQLWithCount(sql, (numEntity / 4), "Number of events should be ");
    // Check metrics
    sql = "SELECT COUNT(entity_id) FROM "
        + PhoenixTimelineWriterImpl.METRIC_TABLE_NAME;
    verifySQLWithCount(sql, (numEntity / 4), "Number of events should be ");
  }

  @AfterClass
  public static void cleanup() throws Exception {
    writer.dropTable(PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME);
    writer.dropTable(PhoenixTimelineWriterImpl.EVENT_TABLE_NAME);
    writer.dropTable(PhoenixTimelineWriterImpl.METRIC_TABLE_NAME);
    writer.serviceStop();
    tearDownMiniCluster();
  }

  private static PhoenixTimelineWriterImpl setupPhoenixClusterAndWriterForTest(
      YarnConfiguration conf) throws Exception{
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

    PhoenixTimelineWriterImpl myWriter = new PhoenixTimelineWriterImpl();
    // Change connection settings for test
    conf.set(
        PhoenixTimelineWriterImpl.TIMELINE_SERVICE_PHOENIX_STORAGE_CONN_STR,
        getUrl());
    myWriter.connProperties = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    myWriter.serviceInit(conf);
    return myWriter;
  }

  private void verifySQLWithCount(String sql, int targetCount, String message)
      throws Exception {
    try (
        Statement stmt =
          writer.getConnection().createStatement();
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
