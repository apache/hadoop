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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.util.ImportError;

/**
 * Manages connections to MySQL databases
 */
public class MySQLManager extends GenericJdbcManager {

  public static final Log LOG = LogFactory.getLog(MySQLManager.class.getName());

  // driver class to ensure is loaded when making db connection.
  private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

  // set to true after we warn the user that we can use direct fastpath.
  private static boolean warningPrinted = false;

  public MySQLManager(final ImportOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  protected MySQLManager(final ImportOptions opts, boolean ignored) {
    // constructor used by subclasses to avoid the --direct warning.
    super(DRIVER_CLASS, opts);
  }

  @Override
  protected String getColNamesQuery(String tableName) {
    // Use mysql-specific hints and LIMIT to return fast
    return "SELECT t.* FROM " + tableName + " AS t LIMIT 1";
  }

  @Override
  public String[] listDatabases() {
    // TODO(aaron): Add an automated unit test for this.

    ResultSet results;
    try {
      results = execute("SHOW DATABASES");
    } catch (SQLException sqlE) {
      LOG.error("Error executing statement: " + sqlE.toString());
      return null;
    }

    try {
      ArrayList<String> databases = new ArrayList<String>();
      while (results.next()) {
        String dbName = results.getString(1);
        databases.add(dbName);
      }

      return databases.toArray(new String[0]);
    } catch (SQLException sqlException) {
      LOG.error("Error reading from database: " + sqlException.toString());
      return null;
    } finally {
      try {
        results.close();
      } catch (SQLException sqlE) {
        LOG.warn("Exception closing ResultSet: " + sqlE.toString());
      }
    }
  }

  @Override
  public void importTable(String tableName, String jarFile, Configuration conf)
        throws IOException, ImportError {

    // Check that we're not doing a MapReduce from localhost. If we are, point
    // out that we could use mysqldump.
    if (!MySQLManager.warningPrinted) {
      String connectString = options.getConnectString();

      if (null != connectString && connectString.indexOf("//localhost") != -1) {
        // if we're not doing a remote connection, they should have a LocalMySQLManager.
        // TODO(aaron): After LocalMySQLManager supports --host/--port, this should
        // always be issued.
        LOG.warn("It looks like you are importing from mysql on");
        LOG.warn("localhost. This transfer can be faster! Use the");
        LOG.warn("--direct option to exercise a MySQL-specific fast");
        LOG.warn("path.");

        MySQLManager.warningPrinted = true; // don't display this twice.
      }
    }

    // Then run the normal importTable() method.
    super.importTable(tableName, jarFile, conf);
  }

  /**
   * Executes an arbitrary SQL statement. Sets mysql-specific parameter
   * to ensure the entire table is not buffered in RAM before reading
   * any rows. A consequence of this is that every ResultSet returned
   * by this method *MUST* be close()'d, or read to exhaustion before
   * another query can be executed from this ConnManager instance.
   *
   * @param stmt The SQL statement to execute
   * @return A ResultSet encapsulating the results or null on error
   */
  protected ResultSet execute(String stmt, Object... args) throws SQLException {
    PreparedStatement statement = null;
    statement = this.getConnection().prepareStatement(stmt,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
    if (null != args) {
      for (int i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
    }

    LOG.info("Executing SQL statement: " + stmt);
    return statement.executeQuery();
  }
}

