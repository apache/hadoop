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

package org.apache.hadoop.sqoop;

import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.manager.GenericJdbcManager;
import org.apache.hadoop.sqoop.manager.HsqldbManager;
import org.apache.hadoop.sqoop.manager.LocalMySQLManager;
import org.apache.hadoop.sqoop.manager.MySQLManager;
import org.apache.hadoop.sqoop.manager.OracleManager;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Static factory class to create the ConnManager type required
 * for the current import job.
 */
public final class ConnFactory {

  public static final Log LOG = LogFactory.getLog(ConnFactory.class.getName());

  private ConnFactory() { }

  /**
   * Factory method to get a ConnManager for the given JDBC connect string
   * @param opts The parsed command-line options
   * @return a ConnManager instance for the appropriate database
   * @throws IOException if it cannot find a ConnManager for this schema
   */
  public static ConnManager getManager(ImportOptions opts) throws IOException {

    String manualDriver = opts.getDriverClassName();
    if (manualDriver != null) {
      // User has manually specified JDBC implementation with --driver.
      // Just use GenericJdbcManager.
      return new GenericJdbcManager(manualDriver, opts);
    }

    String connectStr = opts.getConnectString();

    int schemeStopIdx = connectStr.indexOf("//");
    if (-1 == schemeStopIdx) {
      // no scheme component?
      throw new IOException("Malformed connect string: " + connectStr);
    }

    String scheme = connectStr.substring(0, schemeStopIdx);

    if (null == scheme) {
      // We don't know if this is a mysql://, hsql://, etc.
      // Can't do anything with this.
      throw new IOException("Null scheme associated with connect string.");
    }

    if (scheme.equals("jdbc:mysql:")) {
      if (opts.isDirect()) {
        return new LocalMySQLManager(opts);
      } else {
        return new MySQLManager(opts);
      }
    } else if (scheme.equals("jdbc:hsqldb:hsql:")) {
      return new HsqldbManager(opts);
    } else if (scheme.startsWith("jdbc:oracle:")) {
      return new OracleManager(opts);
    } else {
      throw new IOException("Unknown connection scheme: " + scheme);
    }
  }
}

