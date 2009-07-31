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

package org.apache.hadoop.sqoop.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.sqoop.util.ImportError;

/**
 * Abstract interface that manages connections to a database.
 * The implementations of this class drive the actual discussion with
 * the database about table formats, etc.
 */
public interface ConnManager {

  /**
   * Return a list of all databases on a server
   */
  String [] listDatabases();

  /**
   * Return a list of all tables in a database
   */
  String [] listTables();

  /**
   * Return a list of column names in a table in the order returned by the db.
   */
  String [] getColumnNames(String tableName);

  /**
   * Return the name of the primary key for a table, or null if there is none.
   */
  String getPrimaryKey(String tableName);

  /**
   * Return an unordered mapping from colname to sqltype for
   * all columns in a table.
   *
   * The Integer type id is a constant from java.sql.Types
   */
  Map<String, Integer> getColumnTypes(String tableName);

  /**
   * Execute a SQL statement to read the named set of columns from a table.
   * If columns is null, all columns from the table are read. This is a direct
   * (non-parallelized) read of the table back to the current client.
   * The client is responsible for calling ResultSet.close() when done with the
   * returned ResultSet object.
   */
  ResultSet readTable(String tableName, String [] columns) throws SQLException;

  /**
   * @return the actual database connection
   */
  Connection getConnection() throws SQLException;

  /**
   * @return a string identifying the driver class to load for this JDBC connection type.
   */
  String getDriverClass();

  /**
   * Execute a SQL statement 's' and print its results to stdout
   */
  void execAndPrint(String s);

  /**
   * Perform an import of a table from the database into HDFS
   */
  void importTable(String tableName, String jarFile, Configuration conf)
      throws IOException, ImportError;

  /**
   * Perform any shutdown operations on the connection.
   */
  void close() throws SQLException;
}

