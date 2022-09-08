/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * HSQLDB implementation of {@link FederationStateStore}.
 */
public class MySQLFederationStateStore extends SQLFederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLFederationStateStore.class);

  private Connection conn;

  private static final String TABLE_APPLICATIONSHOMESUBCLUSTER =
      " CREATE TABLE applicationsHomeSubCluster ("
          + " applicationId varchar(64) NOT NULL,"
          + " homeSubCluster varchar(256) NOT NULL,"
          + " CONSTRAINT pk_applicationId PRIMARY KEY (applicationId))";

  @Override
  public void init(Configuration conf) {
    try {
      super.init(conf);
    } catch (YarnException e1) {
      LOG.error("ERROR: failed to init HSQLDB " + e1.getMessage());
    }
    try {
      conn = super.conn;

      LOG.info("Database Init: Start");

      conn.prepareStatement(TABLE_APPLICATIONSHOMESUBCLUSTER).execute();


      LOG.info("Database Init: Complete");
    } catch (SQLException e) {
      LOG.error("ERROR: failed to inizialize HSQLDB " + e.getMessage());
    }
  }

  public void closeConnection() {
    try {
      conn.close();
    } catch (SQLException e) {
      LOG.error(
          "ERROR: failed to close connection to HSQLDB DB " + e.getMessage());
    }
  }
}
