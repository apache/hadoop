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

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTP_SCHEME;
import static org.apache.http.conn.ssl.SSLConnectionSocketFactory.getDefaultHostnameVerifier;

/**
 * Client for AzureBlobFileSystem to execute HTTP requests over ApacheHttpClient.
 */
final class AbfsApacheHttpClient implements Closeable {

  /**
   * ApacheHttpClient instance that executes HTTP request.
   */
  private final CloseableHttpClient httpClient;

  /**
   * Flag to indicate if the client is usable. This is a JVM level flag, state of
   * this flag is shared across all instances of fileSystems. Once switched off,
   * the ApacheHttpClient would not be used for whole JVM lifecycle.
   */
  private static boolean usable = true;

  /**
   * Registers the switch off of ApacheHttpClient for all future use in the JVM.
   */
  static void registerFallback() {
    usable = false;
  }

  /**
   * @return if ApacheHttpClient is usable.
   */
  static boolean usable() {
    return usable;
  }

  AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final int readTimeout, final KeepAliveCache keepAliveCache) {
    final AbfsConnectionManager connMgr = new AbfsConnectionManager(
        createSocketFactoryRegistry(
            new SSLConnectionSocketFactory(delegatingSSLSocketFactory,
                getDefaultHostnameVerifier())),
        new AbfsHttpClientConnectionFactory(), keepAliveCache);
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsManagedHttpRequestExecutor(readTimeout))
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        /*
         * To prevent the read of system property http.agent. The agent is set
         * in request headers by AbfsClient. System property read is an
         * overhead.
         */
        .setUserAgent(EMPTY_STRING);
    httpClient = builder.build();
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  /**
   * Executes the HTTP request.
   *
   * @param httpRequest HTTP request to execute.
   * @param abfsHttpClientContext HttpClient context.
   * @param connectTimeout Connection timeout.
   * @param readTimeout Read timeout.
   *
   * @return HTTP response.
   * @throws IOException network error.
   */
  public HttpResponse execute(HttpRequestBase httpRequest,
      final AbfsManagedHttpClientContext abfsHttpClientContext,
      final int connectTimeout,
      final int readTimeout) throws IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(connectTimeout)
        .setSocketTimeout(readTimeout);
    httpRequest.setConfig(requestConfigBuilder.build());
    return httpClient.execute(httpRequest, abfsHttpClientContext);
  }

  /**
   * Creates the socket factory registry for HTTP and HTTPS.
   *
   * @param sslSocketFactory SSL socket factory.
   * @return Socket factory registry.
   */
  private Registry<ConnectionSocketFactory> createSocketFactoryRegistry(
      ConnectionSocketFactory sslSocketFactory) {
    if (sslSocketFactory == null) {
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register(HTTP_SCHEME,
              PlainConnectionSocketFactory.getSocketFactory())
          .build();
    }
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register(HTTP_SCHEME, PlainConnectionSocketFactory.getSocketFactory())
        .register(HTTPS_SCHEME, sslSocketFactory)
        .build();
  }
}
