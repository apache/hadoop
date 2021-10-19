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
package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides tokens based on username and password.
 */
public class UserPasswordTokenProvider extends AccessTokenProvider {

  private final String authEndpoint;

  private final String username;

  private final String password;

  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  public UserPasswordTokenProvider(final String authEndpoint,
                                   final String username, final String password) {
    Preconditions.checkNotNull(authEndpoint, "authEndpoint");
    Preconditions.checkNotNull(username, "username");
    Preconditions.checkNotNull(password, "password");

    this.authEndpoint = authEndpoint;
    this.username = username;
    this.password = password;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing user-password based token");
    return AzureADAuthenticator.getTokenUsingClientCreds(authEndpoint, username, password);
  }
}
