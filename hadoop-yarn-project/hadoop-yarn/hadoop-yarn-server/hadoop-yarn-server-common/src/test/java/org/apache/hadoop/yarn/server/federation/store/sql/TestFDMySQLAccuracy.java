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

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.impl.MySQLFederationStateStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFDMySQLAccuracy extends FederationSQLAccuracyTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFDMySQLAccuracy.class);

  private static final String HSQLDB_DRIVER = "org.hsqldb.jdbc.JDBCDataSource";
  private static final String DATABASE_URL = "jdbc:hsqldb:mem:state";
  private static final String DATABASE_USERNAME = "SA";
  private static final String DATABASE_PASSWORD = "";
  private static final String MYSQL_COMPATIBILITY = ";sql.syntax_mys=true";

  @Override
  protected FederationStateStore createStateStore() {
     YarnConfiguration conf = new YarnConfiguration();
     conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_JDBC_CLASS, HSQLDB_DRIVER);
     conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_USERNAME, DATABASE_USERNAME);
     conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_PASSWORD, DATABASE_PASSWORD);
     conf.set(YarnConfiguration.FEDERATION_STATESTORE_SQL_URL,
         DATABASE_URL + System.currentTimeMillis() + MYSQL_COMPATIBILITY);
     super.setConf(conf);
     return new MySQLFederationStateStore();
  }

  @Test
  public void xxx(){

  }
}
