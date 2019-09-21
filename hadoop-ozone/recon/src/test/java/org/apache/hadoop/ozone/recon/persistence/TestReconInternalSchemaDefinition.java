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

import static org.hadoop.ozone.recon.schema.ReconInternalSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hadoop.ozone.recon.schema.ReconInternalSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class used to test ReconInternalSchemaDefinition.
 */
public class TestReconInternalSchemaDefinition extends AbstractSqlDatabaseTest {

  @Test
  public void testSchemaCreated() throws Exception {
    ReconInternalSchemaDefinition schemaDefinition = getInjector().getInstance(
        ReconInternalSchemaDefinition.class);

    schemaDefinition.initializeSchema();

    Connection connection =
        getInjector().getInstance(DataSource.class).getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        RECON_TASK_STATUS_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("task_name", Types.VARCHAR));
    expectedPairs.add(new ImmutablePair<>("last_updated_timestamp",
        Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("last_updated_seq_number",
        Types.INTEGER));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(
          resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    Assert.assertEquals(3, actualPairs.size());
    Assert.assertEquals(expectedPairs, actualPairs);
  }

  @Test
  public void testReconTaskStatusCRUDOperations() throws Exception {
    // Verify table exists
    ReconInternalSchemaDefinition schemaDefinition = getInjector().getInstance(
        ReconInternalSchemaDefinition.class);

    schemaDefinition.initializeSchema();

    DataSource ds = getInjector().getInstance(DataSource.class);
    Connection connection = ds.getConnection();

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        RECON_TASK_STATUS_TABLE_NAME, null);

    while (resultSet.next()) {
      Assert.assertEquals(RECON_TASK_STATUS_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    ReconTaskStatusDao dao = new ReconTaskStatusDao(getInjector().getInstance(
        Configuration.class));

    long now = System.currentTimeMillis();
    ReconTaskStatus newRecord = new ReconTaskStatus();
    newRecord.setTaskName("HelloWorldTask");
    newRecord.setLastUpdatedTimestamp(now);
    newRecord.setLastUpdatedSeqNumber(100L);

    // Create
    dao.insert(newRecord);

    ReconTaskStatus newRecord2 = new ReconTaskStatus();
    newRecord2.setTaskName("GoodbyeWorldTask");
    newRecord2.setLastUpdatedTimestamp(now);
    newRecord2.setLastUpdatedSeqNumber(200L);
    // Create
    dao.insert(newRecord2);

    // Read
    ReconTaskStatus dbRecord = dao.findById("HelloWorldTask");

    Assert.assertEquals("HelloWorldTask", dbRecord.getTaskName());
    Assert.assertEquals(Long.valueOf(now), dbRecord.getLastUpdatedTimestamp());
    Assert.assertEquals(Long.valueOf(100), dbRecord.getLastUpdatedSeqNumber());

    // Update
    dbRecord.setLastUpdatedSeqNumber(150L);
    dao.update(dbRecord);

    // Read updated
    dbRecord = dao.findById("HelloWorldTask");
    Assert.assertEquals(Long.valueOf(150), dbRecord.getLastUpdatedSeqNumber());

    // Delete
    dao.deleteById("GoodbyeWorldTask");

    // Verify
    dbRecord = dao.findById("GoodbyeWorldTask");

    Assert.assertNull(dbRecord);
  }

}
