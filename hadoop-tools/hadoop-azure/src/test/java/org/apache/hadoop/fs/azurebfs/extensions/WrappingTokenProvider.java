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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME;

/**
 * Implements a wrapper around ClientCredsTokenProvider.
 */
@SuppressWarnings("UseOfObsoleteDateTimeApi")
public class WrappingTokenProvider implements CustomTokenProviderAdaptee,
    BoundDTExtension {

  public static final String NAME
      = "org.apache.hadoop.fs.azurebfs.extensions.WrappingTokenProvider";

  public static final String UA_STRING = "provider=";

  public static final String CREATED = UA_STRING + "created";
  public static final String INITED = UA_STRING + "inited";
  public static final String BOUND = UA_STRING + "bound";
  public static final String CLOSED = UA_STRING + "closed";

  public static final String ACCESS_TOKEN = "accessToken";

  /** URI; only set once bound. */
  private URI uri;

  private String accountName;

  private String state = CREATED;

  @Override
  public void initialize(
      final Configuration configuration,
      final String account)
      throws IOException {
    state = INITED;
    accountName = account;
  }

  @Override
  public String getAccessToken() throws IOException {
    return ACCESS_TOKEN;
  }

  @Override
  public Date getExpiryTime() {
    return new Date(System.currentTimeMillis());
  }

  @Override
  public void bind(final URI fsURI, final Configuration conf)
      throws IOException {
    state = BOUND;
    uri = fsURI;
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public void close() throws IOException {
    state = CLOSED;
  }

  @Override
  public String getUserAgentSuffix() {
    return state;
  }

  /**
   * Enable the custom token provider.
   * This doesn't set any account-specific options.
   * @param conf configuration to patch.
   */
  public static void enable(Configuration conf) {
    conf.setEnum(FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME,
        AuthType.Custom);
    conf.set(FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME, NAME);
  }
}
