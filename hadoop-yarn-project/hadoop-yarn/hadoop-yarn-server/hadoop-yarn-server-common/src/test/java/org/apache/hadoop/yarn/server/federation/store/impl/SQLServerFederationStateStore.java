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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQLServerFederationStateStore implementation of {@link FederationStateStore}.
 */
public class SQLServerFederationStateStore extends SQLFederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerFederationStateStore.class);

  private Connection conn;

  private String createTableScriptPath;

  private static String createProcedureScriptPath;

  private List<String> tables = new ArrayList<>();

  private List<String> procedures = new ArrayList<>();

  @Override
  public void init(Configuration conf) {
    try {
      super.init(conf);

      // get the sql that creates the table
      createTableScriptPath = "." + File.separator + "target" + File.separator +
          "test-classes" + File.separator + "SQLServer/FederationStateStoreTables.sql";
      LOG.info("createTableScriptPath >> {}", createTableScriptPath);
      String createTableSQL = FileUtils.readFileToString(new File(createTableScriptPath),
          StandardCharsets.UTF_8);
      Pattern p = Pattern.compile("IF NOT EXISTS.*\\n(.*\\n){0,50}.*GO");
      Matcher m = p.matcher(createTableSQL);
      while(m!=null && m.find()) {
        tables.add(m.group());
      }

      // get the sql that creates the stored procedure
      createProcedureScriptPath = "." + File.separator + "target" + File.separator +
          "test-classes" + File.separator + "SQLServer/FederationStateStoreStoreProcs.sql";
      String createProcedureSQL = FileUtils.readFileToString(new File(createProcedureScriptPath),
          StandardCharsets.UTF_8);
      String[] results = createProcedureSQL.split("GO");
      for (String result : results) {
        if (StringUtils.contains(result, "CREATE PROCEDURE")) {
          procedures.add(result);
        }
      }

      LOG.info("SqlServer - tables = {}, procedures = {}", tables.size(), procedures.size());

      conn = super.conn;
    } catch (YarnException | IOException e1) {
      LOG.error("ERROR: failed to init HSQLDB " + e1.getMessage());
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

  public Connection getConn() {
    return conn;
  }

  public List<String> getTables() {
    return tables;
  }

  public List<String> getProcedures() {
    return procedures;
  }
}
