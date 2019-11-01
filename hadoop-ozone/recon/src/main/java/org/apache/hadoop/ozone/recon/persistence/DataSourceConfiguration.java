package org.apache.hadoop.ozone.recon.persistence;

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

/**
 * Common configuration needed to instantiate {@link javax.sql.DataSource}.
 */
public interface DataSourceConfiguration {
  /**
   * Get database driver class name available on the classpath.
   */
  String getDriverClass();

  /**
   * Get Jdbc Url for the database server.
   */
  String getJdbcUrl();

  /**
   * Get username for the db.
   */
  String getUserName();

  /**
   * Get password for the db.
   */
  String getPassword();

  /**
   * Should autocommit be turned on for the datasource.
   */
  boolean setAutoCommit();

  /**
   * Sets the maximum time (in milliseconds) to wait before a call to
   * getConnection is timed out.
   */
  long getConnectionTimeout();

  /**
   * Get a string representation of {@link org.jooq.SQLDialect}.
   */
  String getSqlDialect();

  /**
   * In a production database this should be set to something like 10.
   * SQLite does not allow multiple connections, hence this defaults to 1.
   */
  Integer getMaxActiveConnections();

  /**
   * Sets the maximum connection age (in seconds).
   */
  Integer getMaxConnectionAge();

  /**
   * Sets the maximum idle connection age (in seconds).
   */
  Integer getMaxIdleConnectionAge();

  /**
   * Statement specific to database, usually SELECT 1.
   */
  String getConnectionTestStatement();

  /**
   * How often to test idle connections for being active (in seconds).
   */
  Integer getIdleConnectionTestPeriod();
}
