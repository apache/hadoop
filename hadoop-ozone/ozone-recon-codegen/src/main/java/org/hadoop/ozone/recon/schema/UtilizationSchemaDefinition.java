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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.transaction.annotation.Transactional;

import com.google.inject.Inject;

/**
 * Programmatic definition of Recon DDL.
 */
public class UtilizationSchemaDefinition implements ReconSchemaDefinition {

  private final DataSource dataSource;

  public static final String CLUSTER_GROWTH_DAILY_TABLE_NAME =
      "cluster_growth_daily";

  @Inject
  UtilizationSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  @Transactional
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    createClusterGrowthTable(conn);
  }

  void createClusterGrowthTable(Connection conn) {
    DSL.using(conn).createTableIfNotExists(CLUSTER_GROWTH_DAILY_TABLE_NAME)
        .column("timestamp", SQLDataType.TIMESTAMP)
        .column("datanode_id", SQLDataType.INTEGER)
        .column("datanode_host", SQLDataType.VARCHAR(1024))
        .column("rack_id", SQLDataType.VARCHAR(1024))
        .column("available_size", SQLDataType.BIGINT)
        .column("used_size", SQLDataType.BIGINT)
        .column("container_count", SQLDataType.INTEGER)
        .column("block_count", SQLDataType.INTEGER)
        .constraint(DSL.constraint("pk_timestamp_datanode_id")
            .primaryKey("timestamp", "datanode_id"))
        .execute();
  }


}
