/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon.persistence;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hadoop.ozone.recon.schema.StatsSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.hadoop.ozone.recon.schema.tables.pojos.GlobalStats;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.hadoop.ozone.recon.schema.StatsSchemaDefinition.GLOBAL_STATS_TABLE_NAME;

/**
 * Class used to test StatsSchemaDefinition.
 */
public class TestStatsSchemaDefinition extends AbstractSqlDatabaseTest {

  @Test
  public void testIfStatsSchemaCreated() throws Exception {
    StatsSchemaDefinition schemaDefinition = getInjector().getInstance(
        StatsSchemaDefinition.class);

    schemaDefinition.initializeSchema();

    Connection connection =
        getInjector().getInstance(DataSource.class).getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        GLOBAL_STATS_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("key", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("value", Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("last_updated_timestamp",
        Types.VARCHAR));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    Assert.assertEquals(3, actualPairs.size());
    Assert.assertEquals(expectedPairs, actualPairs);
  }

  @Test
  public void testGlobalStatsCRUDOperations() throws Exception {
    // Verify table exists
    StatsSchemaDefinition schemaDefinition = getInjector().getInstance(
        StatsSchemaDefinition.class);

    schemaDefinition.initializeSchema();

    DataSource ds = getInjector().getInstance(DataSource.class);
    Connection connection = ds.getConnection();

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        GLOBAL_STATS_TABLE_NAME, null);

    while (resultSet.next()) {
      Assert.assertEquals(GLOBAL_STATS_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    GlobalStatsDao dao = new GlobalStatsDao(
        getInjector().getInstance(Configuration.class));

    long now = System.currentTimeMillis();
    GlobalStats newRecord = new GlobalStats();
    newRecord.setLastUpdatedTimestamp(new Timestamp(now));
    newRecord.setKey("key1");
    newRecord.setValue(500L);

    // Create
    dao.insert(newRecord);
    GlobalStats newRecord2 = new GlobalStats();
    newRecord2.setLastUpdatedTimestamp(new Timestamp(now + 1000L));
    newRecord2.setKey("key2");
    newRecord2.setValue(10L);
    dao.insert(newRecord2);

    // Read
    GlobalStats dbRecord = dao.findById("key1");

    Assert.assertEquals("key1", dbRecord.getKey());
    Assert.assertEquals(Long.valueOf(500), dbRecord.getValue());
    Assert.assertEquals(new Timestamp(now), dbRecord.getLastUpdatedTimestamp());

    dbRecord = dao.findById("key2");
    Assert.assertEquals("key2", dbRecord.getKey());
    Assert.assertEquals(Long.valueOf(10), dbRecord.getValue());
    Assert.assertEquals(new Timestamp(now + 1000L),
        dbRecord.getLastUpdatedTimestamp());

    // Update
    dbRecord.setValue(100L);
    dbRecord.setLastUpdatedTimestamp(new Timestamp(now + 2000L));
    dao.update(dbRecord);

    // Read updated
    dbRecord = dao.findById("key2");

    Assert.assertEquals(new Timestamp(now + 2000L),
        dbRecord.getLastUpdatedTimestamp());
    Assert.assertEquals(Long.valueOf(100L), dbRecord.getValue());

    // Delete
    dao.deleteById("key1");

    // Verify
    dbRecord = dao.findById("key1");

    Assert.assertNull(dbRecord);
  }
}
