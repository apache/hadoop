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

package org.hadoop.ozone.recon.schema;

import com.google.inject.Inject;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class used to create tables that are required for storing Ozone statistics.
 */
public class StatsSchemaDefinition implements ReconSchemaDefinition {

  public static final String GLOBAL_STATS_TABLE_NAME = "global_stats";
  private final DataSource dataSource;

  @Inject
  StatsSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    createGlobalStatsTable(conn);
  }

  /**
   * Create the Ozone Global Stats table.
   * @param conn connection
   */
  private void createGlobalStatsTable(Connection conn) {
    DSL.using(conn).createTableIfNotExists(GLOBAL_STATS_TABLE_NAME)
        .column("key", SQLDataType.VARCHAR(255))
        .column("value", SQLDataType.BIGINT)
        .column("last_updated_timestamp", SQLDataType.TIMESTAMP)
        .constraint(DSL.constraint("pk_key")
            .primaryKey("key"))
        .execute();
  }
}
