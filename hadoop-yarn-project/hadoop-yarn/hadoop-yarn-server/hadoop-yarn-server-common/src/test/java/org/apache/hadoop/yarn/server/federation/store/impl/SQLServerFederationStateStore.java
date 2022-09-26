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
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SQLServerFederationStateStore implementation of {@link FederationStateStore}.
 */
public class SQLServerFederationStateStore extends HSQLDBFederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerFederationStateStore.class);

  @Override
  public void init(Configuration conf) {
    try {
      super.initConnection(conf);
      // get the sql that creates the table
      extractCreateTableSQL("SQLServer", "CREATE TABLE .*\\n(.*,\\n){1,5}.*(\\n.*){1,15}\\)");

      List<String> tables = getTables();

      // replacing some incompatible syntaxes
      if (tables != null && !tables.isEmpty()) {
        tables = tables.stream().map(table -> {
          String newTable = table.replace("COLLATE Latin1_General_100_BIN2", "").
                  replace("DEFAULT GETUTCDATE()", "").
                  replace("[dbo].", "").
                  replace("[", "").
                  replace("]", "");
          return newTable;
        }).collect(Collectors.toList());
        setTables(tables);
      }

      // print log
      LOG.info("SqlServer - tables = {}.", tables.size());
    } catch (IOException e) {
      LOG.error("ERROR: failed to init HSQLDB {}.", e.getMessage());
    }
  }
}
