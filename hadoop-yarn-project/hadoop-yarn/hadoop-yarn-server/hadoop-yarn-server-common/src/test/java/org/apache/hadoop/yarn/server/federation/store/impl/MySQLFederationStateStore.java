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

/**
 * MySQLFederationStateStore implementation of {@link FederationStateStore}.
 */
public class MySQLFederationStateStore extends HSQLDBFederationStateStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLFederationStateStore.class);

  @Override
  public void init(Configuration conf) {
    try {
      super.initConnection(conf);
      // get the sql that creates the table
      extractCreateTableSQL("MySQL", "CREATE TABLE.*\\n(.*,\\n){1,10}.*\\n.*");
      // print log
      LOG.info("Mysql - tables = {}.", getTables().size());
    } catch (IOException e) {
      LOG.error("ERROR: failed to init HSQLDB {}", e.getMessage());
    }
  }
}
