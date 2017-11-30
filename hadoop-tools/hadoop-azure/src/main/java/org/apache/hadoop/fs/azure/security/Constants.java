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

  /**
   * The configuration property to enable Kerberos support.
   */

  public static final String AZURE_KERBEROS_SUPPORT_PROPERTY_NAME =
      "fs.azure.enable.kerberos.support";
  /**
   * The configuration property to enable SPNEGO token cache.
   */
  public static final String AZURE_ENABLE_SPNEGO_TOKEN_CACHE =
      "fs.azure.enable.spnego.token.cache";

  /**
   * Parameter to be used for impersonation.
   */
  public static final String DOAS_PARAM = "doas";
  /**
   * Error message for Authentication failures.
   */
  public static final String AUTHENTICATION_FAILED_ERROR_MESSAGE =
      "Authentication Failed ";

  private Constants() {
  }
}
