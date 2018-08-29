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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

/**
 * Provides tokens based on custom implementation, following the Adapter Design
 * Pattern.
 */
public final class CustomTokenProviderAdapter extends AccessTokenProvider {

  private CustomTokenProviderAdaptee adaptee;
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  /**
   * Constructs a token provider based on the custom token provider.
   *
   * @param adaptee the custom token provider
   */
  public CustomTokenProviderAdapter(CustomTokenProviderAdaptee adaptee) {
    Preconditions.checkNotNull(adaptee, "adaptee");
    this.adaptee = adaptee;
  }

  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing custom based token");

    AzureADToken azureADToken = new AzureADToken();
    azureADToken.setAccessToken(adaptee.getAccessToken());
    azureADToken.setExpiry(adaptee.getExpiryTime());

    return azureADToken;
  }
}