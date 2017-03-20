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

package org.apache.hadoop.fs.azure.security;

/**
 * Constants for used with WASB security implementation.
 */
public final class Constants {

  private Constants() {
  }

  /**
   * Configuration parameter name expected in the Configuration
   * object to provide the url of the remote service {@value}
   */
  public static final String KEY_CRED_SERVICE_URL = "fs.azure.cred.service.url";
  /**
   * Default port of the remote service used as delegation token manager and Azure storage SAS key generator.
   */
  public static final int DEFAULT_CRED_SERVICE_PORT = 50911;

  /**
   * Default remote delegation token manager endpoint.
   */
  public static final String DEFAULT_DELEGATION_TOKEN_MANAGER_ENDPOINT = "/tokenmanager/v1";

  /**
   * The configuration property to enable Kerberos support.
   */

  public static final String AZURE_KERBEROS_SUPPORT_PROPERTY_NAME = "fs.azure.enable.kerberos.support";

  /**
   * Parameter to be used for impersonation.
   */
  public static final String DOAS_PARAM = "doas";
}
