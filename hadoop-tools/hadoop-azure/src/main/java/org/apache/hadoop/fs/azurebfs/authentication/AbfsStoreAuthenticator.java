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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidUriException;
import org.apache.hadoop.fs.azurebfs.extensions.ExtensionHelper;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.services.SharedKeyCredentials;
import org.apache.hadoop.io.IOUtils;

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
  private String sasToken;
  private SharedKeyCredentials creds;
  private AccessTokenProvider tokenProvider;

  /**
   * AbfsStoreAuthenticator instance holds authentication type
   * and authentication provider instance
   *
   * @param abfsConfiguration
   * @param accountName
   * @param uri
   * @throws IOException
   */
  // TODO: get accountname from baseUrl
  public AbfsStoreAuthenticator(AbfsConfiguration abfsConfiguration,
      String accountName, URI uri) throws IOException {
    this.authType = abfsConfiguration.getAuthType(accountName);
    sasToken = null;

    switch (this.authType) {
    case OAuth:
    case Custom:
      SetTokenProvider(abfsConfiguration, uri);
      break;
    case SharedKey:
      SetSharedCredentialCreds(accountName, uri, abfsConfiguration);
      int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
      if (dotIndex <= 0) {
        throw new InvalidUriException(
            uri.toString() + " - account name is not fully qualified.");
      }
      creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
          abfsConfiguration.getStorageAccountKey());
      break;
    case SAS: // new type - "SAS"
      if (abfsConfiguration.getAbfsAuthorizer().getClass() == null) {
        throw new UnsupportedOperationException(
            "There is no " + "Authorizer configured");
      }
      break;
    }
  }

  /**
   * Private copy constructor to be used when creating a new instance for
   * DSAS auth flow
   *
   * @param storeAuthenticator
   */
  private AbfsStoreAuthenticator() {
  }

  /**
   * Gets an instance of AbfsStoreAuthenticator for DSAS
   *
   * @param sasToken
   * @return
   */
  public static AbfsStoreAuthenticator getAbfsStoreAuthenticatorForSAS(
      final String sasToken) {
    AbfsStoreAuthenticator instance = new AbfsStoreAuthenticator();
    instance.authType = AuthType.SAS;
    instance.sasToken = sasToken;
    return instance;
  }

  private void SetSharedCredentialCreds(String accountName, URI uri,
      AbfsConfiguration abfsConfiguration) throws AzureBlobFileSystemException {
    int dotIndex = accountName.indexOf(AbfsHttpConstants.DOT);
    if (dotIndex <= 0) {
      throw new InvalidUriException(
          uri.toString() + " - account name is not fully qualified.");
    }
    creds = new SharedKeyCredentials(accountName.substring(0, dotIndex),
        abfsConfiguration.getStorageAccountKey());
  }

  private void SetTokenProvider(AbfsConfiguration abfsConfiguration, URI uri)
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
