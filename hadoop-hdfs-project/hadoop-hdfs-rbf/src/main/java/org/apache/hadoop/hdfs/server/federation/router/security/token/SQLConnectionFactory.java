/*
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

package org.apache.hadoop.hdfs.server.federation.router.security.token;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.SQLDelegationTokenSecretManager;


/**
 * Interface to provide SQL connections to the {@link SQLDelegationTokenSecretManagerImpl}.
 */
public interface SQLConnectionFactory {
  String CONNECTION_URL = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.url";
  String CONNECTION_USERNAME = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.username";
  String CONNECTION_PASSWORD = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.password";
  String CONNECTION_DRIVER = SQLDelegationTokenSecretManager.SQL_DTSM_CONF_PREFIX
      + "connection.driver";

  Connection getConnection() throws SQLException;
  void shutdown();

  default Connection getConnection(boolean autocommit) throws SQLException {
    Connection connection = getConnection();
    connection.setAutoCommit(autocommit);
    return connection;
  }
}

