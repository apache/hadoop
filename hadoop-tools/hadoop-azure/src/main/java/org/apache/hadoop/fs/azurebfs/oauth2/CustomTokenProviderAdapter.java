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
import java.net.URI;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.BoundDTExtension;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AzureADAuthenticator.HttpException;

/**
 * Provides tokens based on custom implementation, following the Adapter Design
 * Pattern.
 */
public final class CustomTokenProviderAdapter extends AccessTokenProvider
  implements BoundDTExtension {

  private final int fetchTokenRetryCount;
  private CustomTokenProviderAdaptee adaptee;
  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenProvider.class);

  /**
   * Constructs a token provider based on the custom token provider.
   *
   * @param adaptee the custom token provider
   * @param customTokenFetchRetryCount max retry count for customTokenFetch
   */
  public CustomTokenProviderAdapter(CustomTokenProviderAdaptee adaptee, int customTokenFetchRetryCount) {
    Preconditions.checkNotNull(adaptee, "adaptee");
    this.adaptee = adaptee;
    fetchTokenRetryCount = customTokenFetchRetryCount;
  }

  protected AzureADToken refreshToken() throws IOException {
    LOG.debug("AADToken: refreshing custom based token");

    AzureADToken azureADToken = new AzureADToken();

    String accessToken = null;

    Exception ex;
    boolean succeeded = false;
    // Custom token providers should have their own retry policies,
    // Providing a linear retry option for the the retry count
    // mentioned in config "fs.azure.custom.token.fetch.retry.count"
    int retryCount = fetchTokenRetryCount;
    do {
      ex = null;
      try {
        accessToken = adaptee.getAccessToken();
        LOG.trace("CustomTokenProvider Access token fetch was successful with retry count {}",
            (fetchTokenRetryCount - retryCount));
      } catch (Exception e) {
        LOG.debug("CustomTokenProvider Access token fetch failed with retry count {}",
            (fetchTokenRetryCount - retryCount));
        ex = e;
      }

      succeeded = (ex == null);
      retryCount--;
    } while (!succeeded && (retryCount) >= 0);

    if (!succeeded) {
      HttpException httpEx = new HttpException(
          -1,
          "",
          String.format("CustomTokenProvider getAccessToken threw %s : %s",
              ex.getClass().getTypeName(), ex.getMessage()),
          "",
          "",
          ""
      );
      throw httpEx;
    }

    azureADToken.setAccessToken(accessToken);
    azureADToken.setExpiry(adaptee.getExpiryTime());

    return azureADToken;
  }

  /**
   * Bind to the filesystem by passing the binding call on
   * to any custom token provider adaptee which implements
   * {@link BoundDTExtension}.
   * No-op if they don't.
   * @param fsURI URI of the filesystem.
   * @param conf configuration of this extension.
   * @throws IOException failure.
   */
  @Override
  public void bind(final URI fsURI,
      final Configuration conf)
      throws IOException {
    ExtensionHelper.bind(adaptee, fsURI, conf);
  }

  @Override
  public void close() {
    ExtensionHelper.close(adaptee);
  }

  /**
   * Get a suffix for the UserAgent suffix of HTTP requests, which
   * can be used to identify the principal making ABFS requests.
   *
   * If the adaptee is a BoundDTExtension, it is queried for a UA Suffix;
   * otherwise "" is returned.
   *
   * @return an empty string, or a key=value string to be added to the UA
   * header.
   */
  public String getUserAgentSuffix() {
    String suffix = ExtensionHelper.getUserAgentSuffix(adaptee, "");
    return suffix != null ? suffix : "";
  }
}
