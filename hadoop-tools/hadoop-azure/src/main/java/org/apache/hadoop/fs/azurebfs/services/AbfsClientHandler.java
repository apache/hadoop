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

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class AbfsClientHandler implements Closeable {
  public static final Logger LOG = LoggerFactory.getLogger(AbfsClientHandler.class);

  private AbfsServiceType defaultServiceType;
  private AbfsServiceType ingressServiceType;
  private final AbfsDfsClient dfsAbfsClient;
  private final AbfsBlobClient blobAbfsClient;


  /**
   * Constructs an AbfsClientHandler instance.
   *
   * Initializes the default and ingress service types from the provided configuration,
   * then creates both DFS and Blob clients using the given params
   *
   * @param baseUrl the base URL for the file system.
   * @param sharedKeyCredentials credentials for shared key authentication.
   * @param abfsConfiguration the ABFS configuration.
   * @param tokenProvider the access token provider, may be null.
   * @param sasTokenProvider the SAS token provider, may be null.
   * @param encryptionContextProvider the encryption context provider
   * @param abfsClientContext the ABFS client context.
   * @throws IOException if client creation or URL conversion fails.
   */
  public AbfsClientHandler(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    // This will initialize the default and ingress service types.
    // This is needed before creating the clients so that we can do cache warmup
    // only for default client.
    initServiceType(abfsConfiguration);
    this.dfsAbfsClient = createDfsClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, tokenProvider, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
    this.blobAbfsClient = createBlobClient(baseUrl, sharedKeyCredentials,
        abfsConfiguration, tokenProvider, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
  }

  /**
   * Initialize the default service type based on the user configuration.
   * @param abfsConfiguration set by user.
   */
  private void initServiceType(final AbfsConfiguration abfsConfiguration) {
    this.defaultServiceType = abfsConfiguration.getFsConfiguredServiceType();
    this.ingressServiceType = abfsConfiguration.getIngressServiceType();
  }

  /**
   * Sets the default service type.
   *
   * @param defaultServiceType the service type to set as default
   */
  public void setDefaultServiceType(AbfsServiceType defaultServiceType) {
    this.defaultServiceType = defaultServiceType;
  }

  /**
   * Sets the ingress service type.
   *
   * @param ingressServiceType the ingress service type
   */
  public void setIngressServiceType(AbfsServiceType ingressServiceType) {
    this.ingressServiceType = ingressServiceType;
  }

  /**
   * Gets the default ingress service type.
   *
   * @return the default ingress service type
   */
  public AbfsServiceType getIngressServiceType() {
    return ingressServiceType;
  }

  /**
   * Get the AbfsClient based on the default service type.
   * @return AbfsClient
   */
  public AbfsClient getClient() {
    return getClient(defaultServiceType);
  }

  /**
   * Get the AbfsClient based on the ingress service type.
   *
   * @return AbfsClient for the ingress service type.
   */
  public AbfsClient getIngressClient() {
    return getClient(ingressServiceType);
  }

  /**
   * Get the AbfsClient based on the service type.
   * @param serviceType AbfsServiceType.
   * @return AbfsClient
   */
  public AbfsClient getClient(AbfsServiceType serviceType) {
    return serviceType == AbfsServiceType.DFS ? dfsAbfsClient : blobAbfsClient;
  }

  /**
   * Gets the AbfsDfsClient instance.
   *
   * @return the AbfsDfsClient instance.
   */
  public AbfsDfsClient getDfsClient() {
    return dfsAbfsClient;
  }

  /**
   * Gets the AbfsBlobClient instance.
   *
   * @return the AbfsBlobClient instance.
   */
  public AbfsBlobClient getBlobClient() {
    return blobAbfsClient;
  }

  /**
   * Create the AbfsDfsClient using the url used to configure file system.
   * If URL is for Blob endpoint, it will be converted to DFS endpoint.
   * @param baseUrl URL.
   * @param creds SharedKeyCredentials.
   * @param abfsConfiguration AbfsConfiguration.
   * @param tokenProvider AccessTokenProvider.
   * @param sasTokenProvider SASTokenProvider.
   * @param encryptionContextProvider EncryptionContextProvider.
   * @param abfsClientContext AbfsClientContext.
   * @return AbfsDfsClient with DFS endpoint URL.
   * @throws IOException if URL conversion fails.
   */
  private AbfsDfsClient createDfsClient(final URL baseUrl,
      final SharedKeyCredentials creds,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    URL dfsUrl = changeUrlFromBlobToDfs(baseUrl);
    LOG.debug(
        "Creating AbfsDfsClient with access token provider: %s and "
            + "SAS token provider: %s using the URL: %s",
        tokenProvider, sasTokenProvider, dfsUrl);
    return new AbfsDfsClient(dfsUrl, creds, abfsConfiguration,
        tokenProvider, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
  }

  /**
   * Create the AbfsBlobClient using the url used to configure file system.
   * If URL is for DFS endpoint, it will be converted to Blob endpoint.
   * @param baseUrl URL.
   * @param creds SharedKeyCredentials.
   * @param abfsConfiguration AbfsConfiguration.
   * @param tokenProvider AccessTokenProvider.
   * @param sasTokenProvider SASTokenProvider.
   * @param encryptionContextProvider EncryptionContextProvider.
   * @param abfsClientContext AbfsClientContext.
   * @return AbfsBlobClient with Blob endpoint URL.
   * @throws IOException if URL conversion fails.
   */
  private AbfsBlobClient createBlobClient(final URL baseUrl,
      final SharedKeyCredentials creds,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final SASTokenProvider sasTokenProvider,
      final EncryptionContextProvider encryptionContextProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    URL blobUrl = changeUrlFromDfsToBlob(baseUrl);
    LOG.debug(
        "Creating AbfsBlobClient with access token provider: %s and "
            + "SAS token provider: %s using the URL: %s",
        tokenProvider, sasTokenProvider, blobUrl);
    return new AbfsBlobClient(blobUrl, creds, abfsConfiguration,
        tokenProvider, sasTokenProvider, encryptionContextProvider,
        abfsClientContext);
  }

  @Override
  public void close() throws IOException {
    IOUtils.cleanupWithLogger(LOG, getDfsClient(), getBlobClient());
  }
}
