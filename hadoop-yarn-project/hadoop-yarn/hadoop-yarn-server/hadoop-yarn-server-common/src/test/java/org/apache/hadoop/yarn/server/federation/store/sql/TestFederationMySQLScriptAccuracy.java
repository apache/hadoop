/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.sql;

import org.apache.hadoop.yarn.server.federation.store.impl.MySQLFederationStateStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public class TestFederationMySQLScriptAccuracy extends FederationSQLAccuracyTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFederationMySQLScriptAccuracy.class);

  private static final String MYSQL_COMPATIBILITY = ";sql.syntax_mys=true";

  @Override
  protected MySQLFederationStateStore createStateStore() {
    return new MySQLFederationStateStore();
  }

  @Override
  protected String getSQLURL() {
    return DATABASE_URL + System.currentTimeMillis() + MYSQL_COMPATIBILITY;
  }

  @Test
  public void checkMysqlScriptAccuracy() throws SQLException {
    MySQLFederationStateStore federationStateStore = this.createStateStore();
    federationStateStore.initConnection(this.getConf());

    // get a list of tables
    List<String> tables = federationStateStore.getTables();
    for (String table : tables) {
      federationStateStore.getConn().prepareStatement(table).execute();
    }

    LOG.info("FederationStateStore create {} tables.", tables.size());
  }
}
