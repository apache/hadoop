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

  public static final String OZONE_RECON_DB_DIRS = "ozone.recon.db.dirs";

  /**
   * Private constructor for utility class.
   */
  private ReconServerConfigKeys() {
  }
}
