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

package org.apache.hadoop.fs.azurebfs.authentication;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.extensions.AbfsAuthorizer;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

/**
 * Adapter class that holds handle to configured authentication instance.
 * Any new authentication support added will have to be added here
 * and ensured that AbfsRestOperation will include the auth in the Htpp store
 * request.
 */
public class AbfsStoreAuthenticator {

  public static final Logger LOG = LoggerFactory
      .getLogger(AbfsStoreAuthenticator.class);
  private AuthType authType;
  private String sasToken = null;
  private SharedKeyCredentials creds;
  private AccessTokenProvider tokenProvider;
  private boolean isAuthorizerProvidingSAS = false;
  private AbfsAuthorizer authorizer;

  /**
   * AbfsStoreAuthenticator instance holds authentication type
   * and authentication provider instance
   *
   * @param abfsConfiguration
   * @param uri
   * @throws IOException
   */
  // TODO: get accountname from baseUrl
  public AbfsStoreAuthenticator(AbfsConfiguration abfsConfiguration,
      URI uri, AbfsAuthorizer authorizer) throws IOException {

    String fileSystemname = UriUtils.authorityParts(uri)[0];
    String accountName = UriUtils.authorityParts(uri)[1];
    this.authType = abfsConfiguration.getAuthType(accountName);

    if (authorizer != null) {
      if (abfsConfiguration.getAuthorizerAuthType(fileSystemname, accountName) == AuthType.SAS) {
        isAuthorizerProvidingSAS = true;
        this.authorizer = authorizer;
        this.authType = AuthType.SAS;
      } else {
        if (abfsConfiguration.getAuthorizerAuthType(fileSystemname, accountName) != AuthType.None) {
          throw new UnsupportedOperationException("Authorizer can only be "
              + "configured to provide SAS AuthType.");
        }
      }
    }

    if (!isAuthorizerProvidingSAS) {
      switch (this.authType) {
      case OAuth:
      case Custom:
        setTokenProvider(abfsConfiguration, uri);
        break;
      case SharedKey:
        setSharedCredentialCreds(accountName, uri, abfsConfiguration);
        int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
        if (dotIndex <= 0) {
          throw new InvalidUriException(uri.toString() + " - account name is not fully qualified.");
        }
        creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
            abfsConfiguration.getStorageAccountKey());
        break;
      case SAS: // new type - "SAS"
        if (abfsConfiguration.getAbfsAuthorizer().getClass() == null) {
          throw new UnsupportedOperationException("There is no " + "Authorizer configured");
        }
        break;
      }
    }
  }

  public void setSASToken(String sasToken) {
    this.sasToken = sasToken;
  }

  private void setSharedCredentialCreds(String accountName, URI uri,
      AbfsConfiguration abfsConfiguration) throws AzureBlobFileSystemException {
    int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
    if (dotIndex <= 0) {
      throw new InvalidUriException(
          uri.toString() + " - account name is not fully qualified.");
    }
    creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
        abfsConfiguration.getStorageAccountKey());
  }

  private void setTokenProvider(AbfsConfiguration abfsConfiguration, URI uri)
      throws IOException {
    tokenProvider = abfsConfiguration.getTokenProvider();
    ExtensionHelper
        .bind(tokenProvider, uri, abfsConfiguration.getRawConfiguration());
  }

  public AuthType getAuthType() {
    return authType;
  }

  public SharedKeyCredentials getSharedKeyCredentials() {
    return creds;
  }

  public AccessTokenProvider getTokenProvider() {
    return tokenProvider;
  }

  public synchronized String getAccessToken() throws IOException {
  if (tokenProvider != null) {
      return "Bearer " + tokenProvider.getToken().getAccessToken();
    } else {
      return null;
    }
  }

  public String getSasToken() {
    return this.sasToken;
  }

  public void close() {
    if (tokenProvider instanceof Closeable) {
      IOUtils.cleanupWithLogger(LOG, (Closeable) tokenProvider);
    }
  }
}
