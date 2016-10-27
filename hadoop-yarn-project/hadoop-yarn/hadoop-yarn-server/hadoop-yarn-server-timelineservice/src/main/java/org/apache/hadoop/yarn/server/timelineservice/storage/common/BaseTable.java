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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Implements behavior common to tables used in the timeline service storage. It
 * is thread-safe, and can be used by multiple threads concurrently.
 *
 * @param <T> reference to the table instance class itself for type safety.
 */
public abstract class BaseTable<T> {

  /**
   * Name of config variable that is used to point to this table.
   */
  private final String tableNameConfName;

  /**
   * Unless the configuration overrides, this will be the default name for the
   * table when it is created.
   */
  private final String defaultTableName;

  /**
   * @param tableNameConfName name of config variable that is used to point to
   *          this table.
   * @param defaultTableName Default table name if table from config is not
   *          found.
   */
  protected BaseTable(String tableNameConfName, String defaultTableName) {
    this.tableNameConfName = tableNameConfName;
    this.defaultTableName = defaultTableName;
  }

  /**
   * Used to create a type-safe mutator for this table.
   *
   * @param hbaseConf used to read table name.
   * @param conn used to create a table from.
   * @return a type safe {@link BufferedMutator} for the entity table.
   * @throws IOException if any exception occurs while creating mutator for the
   *     table.
   */
  public TypedBufferedMutator<T> getTableMutator(Configuration hbaseConf,
      Connection conn) throws IOException {

    TableName tableName = this.getTableName(hbaseConf);

    // Plain buffered mutator
    BufferedMutator bufferedMutator = conn.getBufferedMutator(tableName);

    // Now make this thing type safe.
    // This is how service initialization should hang on to this variable, with
    // the proper type
    TypedBufferedMutator<T> table =
        new BufferedMutatorDelegator<T>(bufferedMutator);

    return table;
  }

  /**
   * @param hbaseConf used to read settings that override defaults
   * @param conn used to create table from
   * @param scan that specifies what you want to read from this table.
   * @return scanner for the table.
   * @throws IOException if any exception occurs while getting the scanner.
   */
  public ResultScanner getResultScanner(Configuration hbaseConf,
      Connection conn, Scan scan) throws IOException {
    Table table = conn.getTable(getTableName(hbaseConf));
    return table.getScanner(scan);
  }

  /**
   *
   * @param hbaseConf used to read settings that override defaults
   * @param conn used to create table from
   * @param get that specifies what single row you want to get from this table
   * @return result of get operation
   * @throws IOException if any exception occurs while getting the result.
   */
  public Result getResult(Configuration hbaseConf, Connection conn, Get get)
      throws IOException {
    Table table = conn.getTable(getTableName(hbaseConf));
    return table.get(get);
  }

  /**
   * Get the table name for the input table.
   *
   * @param conf HBase configuration from which table name will be fetched.
   * @param tableName name of the table to be fetched
   * @return A {@link TableName} object.
   */
  public static TableName getTableName(Configuration conf, String tableName) {
    String tableSchemaPrefix =  conf.get(
        YarnConfiguration.TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX_NAME,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_HBASE_SCHEMA_PREFIX);
    return TableName.valueOf(tableSchemaPrefix + tableName);
  }

  /**
   * Get the table name for this table.
   *
   * @param conf HBase configuration from which table name will be fetched.
   * @return A {@link TableName} object.
   */
  public TableName getTableName(Configuration conf) {
    String tableName = conf.get(tableNameConfName, defaultTableName);
    return getTableName(conf, tableName);
  }

  /**
   * Get the table name based on the input config parameters.
   *
   * @param conf HBase configuration from which table name will be fetched.
   * @param tableNameInConf the table name parameter in conf.
   * @param defaultTableName the default table name.
   * @return A {@link TableName} object.
   */
  public static TableName getTableName(Configuration conf,
      String tableNameInConf, String defaultTableName) {
    String tableName = conf.get(tableNameInConf, defaultTableName);
    return getTableName(conf, tableName);
  }

  /**
   * Used to create the table in HBase. Should be called only once (per HBase
   * instance).
   *
   * @param admin Used for doing HBase table operations.
   * @param hbaseConf Hbase configuration.
   * @throws IOException if any exception occurs while creating the table.
   */
  public abstract void createTable(Admin admin, Configuration hbaseConf)
      throws IOException;

}
