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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;


/**
 * This interface provides an extensibility model for customizing the acquisition
 * of Azure Active Directory Access Tokens.   When "fs.azure.account.auth.type" is
 * set to "Custom", implementors may use the
 * "fs.azure.account.oauth.provider.type.{accountName}" configuration property
 * to specify a class with a custom implementation of CustomTokenProviderAdaptee.
 * This class will be dynamically loaded, initialized, and invoked to provide
 * AAD Access Tokens and their Expiry.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CustomTokenProviderAdaptee {

  /**
   * Initialize with supported configuration. This method is invoked when the
   * (URI, Configuration)} method is invoked.
   *
   * @param configuration Configuration object
   * @param accountName Account Name
   * @throws IOException if instance can not be configured.
   */
  void initialize(Configuration configuration, final String accountName)
          throws IOException;

  /**
   * Obtain the access token that should be added to https connection's header.
   * Will be called depending upon {@link #getExpiryTime()} expiry time is set,
   * so implementations should be performant. Implementations are responsible
   * for any refreshing of the token.
   *
   * @return String containing the access token
   * @throws IOException if there is an error fetching the token
   */
  String getAccessToken() throws IOException;

  /**
   * Obtain expiry time of the token. If implementation is performant enough to
   * maintain expiry and expect {@link #getAccessToken()} call for every
   * connection then safe to return current or past time.
   *
   * However recommended to use the token expiry time received from Azure Active
   * Directory.
   *
   * @return Date to expire access token retrieved from AAD.
   */
  Date getExpiryTime();
}
