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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.mapred.ImportJob;
import org.apache.hadoop.sqoop.util.ImportError;

/**
 * Manages connections to Oracle databases.
 * Requires the Oracle JDBC driver.
 */
public class OracleManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(OracleManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS = "oracle.jdbc.OracleDriver";

  public OracleManager(final ImportOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  protected String getColNamesQuery(String tableName) {
    // SqlManager uses "tableName AS t" which doesn't work in Oracle.
    return "SELECT t.* FROM " + tableName + " t";
  }

  /**
   * Create a connection to the database; usually used only from within
   * getConnection(), which enforces a singleton guarantee around the
   * Connection object.
   *
   * Oracle-specific driver uses READ_COMMITTED which is the weakest
   * semantics Oracle supports.
   */
  protected Connection makeConnection() throws SQLException {

    Connection connection;
    String driverClass = getDriverClass();

    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Could not load db driver class: " + driverClass);
    }

    String username = options.getUsername();
    String password = options.getPassword();
    if (null == username) {
      connection = DriverManager.getConnection(options.getConnectString());
    } else {
      connection = DriverManager.getConnection(options.getConnectString(), username, password);
    }

    // We only use this for metadata queries. Loosest semantics are okay.
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    return connection;
  }

  /**
   * This importTable() implementation continues to use the older DBInputFormat
   * because DataDrivenDBInputFormat does not currently work with Oracle.
   */
  public void importTable(String tableName, String jarFile, Configuration conf)
      throws IOException, ImportError {
    ImportJob importer = new ImportJob(options);
    String splitCol = options.getSplitByCol();
    if (null == splitCol) {
      // If the user didn't specify a splitting column, try to infer one.
      splitCol = getPrimaryKey(tableName);
    }

    if (null == splitCol) {
      // Can't infer a primary key.
      throw new ImportError("No primary key could be found for table " + tableName
          + ". Please specify one with --split-by.");
    }

    importer.runImport(tableName, jarFile, splitCol, conf);
  }
}

