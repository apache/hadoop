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

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.sqoop.ImportOptions;

/**
 * Database manager that is connects to a generic JDBC-compliant
 * database; its constructor is parameterized on the JDBC Driver
 * class to load.
 *
 * 
 *
 */
public class GenericJdbcManager extends SqlManager {

  public static final Log LOG = LogFactory.getLog(GenericJdbcManager.class.getName());

  private String jdbcDriverClass;
  private Connection connection;

  public GenericJdbcManager(final String driverClass, final ImportOptions opts) {
    super(opts);

    this.jdbcDriverClass = driverClass;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (null == this.connection) {
      this.connection = makeConnection();
    }

    return this.connection;
  }

  protected boolean hasOpenConnection() {
    return this.connection != null;
  }

  public void close() throws SQLException {
    super.close();
    if (null != this.connection) {
      this.connection.close();
      this.connection = null;
    }
  }

  public String getDriverClass() {
    return jdbcDriverClass;
  }
}

