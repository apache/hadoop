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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides tokens based on Azure VM's Managed Service Identity.
 */
public class MsiTokenProvider extends AccessTokenProvider {

  private final String tenantGuid;

  private final String clientId;

  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  public MsiTokenProvider(final String tenantGuid, final String clientId) {
    this.tenantGuid = tenantGuid;
    this.clientId = clientId;
  }

  @Override
  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing token from MSI");
    AzureADToken token = AzureADAuthenticator.getTokenFromMsi(tenantGuid, clientId, false);
    return token;
  }
}