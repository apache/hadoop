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

import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestPhoenixTimelineWriterImpl {
  private PhoenixTimelineWriterImpl writer;

  @Before
  public void setup() throws Exception {
    // TODO: launch a miniphoenix cluster, or else we're directly operating on
    // the active Phoenix cluster
    YarnConfiguration conf = new YarnConfiguration();
    writer = createPhoenixWriter(conf);
  }

  @Ignore
  @Test
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

  @After
  public void cleanup() throws Exception {
    // Note: it is assumed that we're working on a test only cluster, or else
    // this cleanup process will drop the entity table.
    writer.dropTable(PhoenixTimelineWriterImpl.ENTITY_TABLE_NAME);
    writer.dropTable(PhoenixTimelineWriterImpl.EVENT_TABLE_NAME);
    writer.dropTable(PhoenixTimelineWriterImpl.METRIC_TABLE_NAME);
    writer.serviceStop();
  }

  private static PhoenixTimelineWriterImpl createPhoenixWriter(
      YarnConfiguration conf) throws Exception{
    PhoenixTimelineWriterImpl myWriter = new PhoenixTimelineWriterImpl();
    myWriter.serviceInit(conf);
    return myWriter;
  }

  private void verifySQLWithCount(String sql, int targetCount, String message)
      throws Exception{
    try (
        Statement stmt =
          PhoenixTimelineWriterImpl.getConnection().createStatement();
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
