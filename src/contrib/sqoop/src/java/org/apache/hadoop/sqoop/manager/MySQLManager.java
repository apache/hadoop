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

  // set to true after we warn the user that we can use local fastpath.
  private static boolean warningPrinted = false;

  public MySQLManager(final ImportOptions opts) {
    super(DRIVER_CLASS, opts);
  }

  protected MySQLManager(final ImportOptions opts, boolean ignored) {
    // constructor used by subclasses to avoid the --local warning.
    super(DRIVER_CLASS, opts);
  }

  @Override
  public String[] listDatabases() {
    // TODO(aaron): Add an automated unit test for this.

    ResultSet results = execute("SHOW DATABASES");
    if (null == results) {
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
        LOG.warn("It looks like you are importing from mysql on");
        LOG.warn("localhost. This transfer can be faster! Use the");
        LOG.warn("--local option to exercise a MySQL-specific fast");
        LOG.warn("path.");

        MySQLManager.warningPrinted = true; // don't display this twice.
      }
    }

    // Then run the normal importTable() method.
    super.importTable(tableName, jarFile, conf);
  }
}

