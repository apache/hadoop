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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;

import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromBlobToDfs;
import static org.apache.hadoop.fs.azurebfs.utils.UriUtils.changeUrlFromDfsToBlob;

/**
 * AbfsClientHandler is a class that provides a way to get the AbfsClient
 * based on the service type.
 */
public class AbfsClientHandler {

  private AbfsServiceType defaultServiceType;
  private AbfsServiceType ingressServiceType;
  private final AbfsDfsClient dfsAbfsClient;

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = createDfsClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, tokenProvider, null, encryptionContextProvider,
        abfsClientContext);
    initServiceType(abfsConfiguration);
  }

  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    this.dfsAbfsClient = createDfsClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, null, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
    initServiceType(abfsConfiguration);
  }

  private void initServiceType(final AbfsConfiguration abfsConfiguration) {
    this.defaultServiceType = abfsConfiguration.getFsConfiguredServiceType();
    this.ingressServiceType = abfsConfiguration.getIngressServiceType();
  }

  public AbfsClient getClient() {
    return getClient(defaultServiceType);
  }

  public AbfsClient getClient(AbfsServiceType serviceType) {
    return serviceType == AbfsServiceType.DFS ? dfsAbfsClient : null;
  }

  private AbfsDfsClient createDfsClient(final URL baseUrl,
      final SharedKeyCredentials creds,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    URL dfsUrl = changeUrlFromBlobToDfs(baseUrl);
    if (tokenProvider != null) {
      return new AbfsDfsClient(dfsUrl, creds, abfsConfiguration,
          tokenProvider, encryptionContextProvider,
          abfsClientContext);
    } else {
      return new AbfsDfsClient(dfsUrl, creds, abfsConfiguration,
          sasTokenProvider, encryptionContextProvider,
          abfsClientContext);
    }
  }
}
