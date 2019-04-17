/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_STORE_IMPL_ROCKSDB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class contains constants for Recon configuration keys.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class ReconServerConfigKeys {

  public static final String OZONE_RECON_HTTP_ENABLED_KEY =
      "ozone.recon.http.enabled";
  public static final String OZONE_RECON_HTTP_BIND_HOST_KEY =
      "ozone.recon.http-bind-host";
  public static final String OZONE_RECON_HTTPS_BIND_HOST_KEY =
      "ozone.recon.https-bind-host";
  public static final String OZONE_RECON_HTTP_ADDRESS_KEY =
      "ozone.recon.http-address";
  public static final String OZONE_RECON_HTTPS_ADDRESS_KEY =
      "ozone.recon.https-address";
  public static final String OZONE_RECON_KEYTAB_FILE =
      "ozone.recon.keytab.file";
  public static final String OZONE_RECON_HTTP_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_RECON_HTTP_BIND_PORT_DEFAULT = 9888;
  public static final int OZONE_RECON_HTTPS_BIND_PORT_DEFAULT = 9889;
  public static final String OZONE_RECON_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      "ozone.recon.authentication.kerberos.principal";

  public static final String OZONE_RECON_CONTAINER_DB_CACHE_SIZE_MB =
      "ozone.recon.container.db.cache.size.mb";
  public static final int OZONE_RECON_CONTAINER_DB_CACHE_SIZE_DEFAULT = 128;

  public static final String OZONE_RECON_DB_DIR = "ozone.recon.db.dir";

  public static final String OZONE_RECON_OM_SNAPSHOT_DB_DIR =
      "ozone.recon.om.db.dir";

  public static final String RECON_OM_SOCKET_TIMEOUT =
      "recon.om.socket.timeout";
  public static final String RECON_OM_SOCKET_TIMEOUT_DEFAULT = "5s";

  public static final String RECON_OM_CONNECTION_TIMEOUT =
      "recon.om.connection.timeout";
  public static final String RECON_OM_CONNECTION_TIMEOUT_DEFAULT = "5s";

  public static final String RECON_OM_CONNECTION_REQUEST_TIMEOUT =
      "recon.om.connection.request.timeout";

  public static final String RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT = "5s";

  public static final String RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY =
      "recon.om.snapshot.task.initial.delay";
  public static final String
      RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT = "1m";

  public static final String OZONE_RECON_CONTAINER_DB_STORE_IMPL =
      "ozone.recon.container.db.impl";
  public static final String OZONE_RECON_CONTAINER_DB_STORE_IMPL_DEFAULT =
      OZONE_METADATA_STORE_IMPL_ROCKSDB;

  public static final String RECON_OM_SNAPSHOT_TASK_INTERVAL =
      "recon.om.snapshot.task.interval.delay";
  public static final String RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT
      = "10m";

  public static final String RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM =
      "recon.om.snapshot.task.flush.param";

  // Persistence properties
  public static final String OZONE_RECON_SQL_DB_DRIVER =
      "ozone.recon.sql.db.driver";
  public static final String OZONE_RECON_SQL_DB_JDBC_URL =
      "ozone.recon.sql.db.jdbc.url";
  public static final String OZONE_RECON_SQL_DB_USER =
      "ozone.recon.sql.db.username";
  public static final String OZONE_RECON_SQL_DB_PASSWORD =
      "ozone.recon.sql.db.password";
  public static final String OZONE_RECON_SQL_AUTO_COMMIT =
      "ozone.recon.sql.db.auto.commit";
  public static final String OZONE_RECON_SQL_CONNECTION_TIMEOUT =
      "ozone.recon.sql.db.conn.timeout";
  public static final String OZONE_RECON_SQL_MAX_ACTIVE_CONNECTIONS =
      "ozone.recon.sql.db.conn.max.active";
  public static final String OZONE_RECON_SQL_MAX_CONNECTION_AGE =
      "ozone.recon.sql.db.conn.max.age";
  public static final String OZONE_RECON_SQL_MAX_IDLE_CONNECTION_AGE =
      "ozone.recon.sql.db.conn.idle.max.age";
  public static final String OZONE_RECON_SQL_IDLE_CONNECTION_TEST_PERIOD =
      "ozone.recon.sql.db.conn.idle.test.period";
  public static final String OZONE_RECON_SQL_MAX_IDLE_CONNECTION_TEST_STMT =
      "ozone.recon.sql.db.conn.idle.test";

  /**
   * Private constructor for utility class.
   */
  private ReconServerConfigKeys() {
  }
}
