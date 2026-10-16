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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.TailLatencyRequestTimeoutException;
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
   * In case, getting success response from apache client, sets the usable flag to true.
   */
  static void setUsable() {
    usable = true;
  }

  /**
   * @return if ApacheHttpClient is usable.
   */
  static boolean usable() {
    return usable;
  }

  AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final AbfsConfiguration abfsConfiguration,
      final KeepAliveCache keepAliveCache,
      URL baseUrl,
      final boolean isCacheWarmupNeeded) {
    final AbfsConnectionManager connMgr = new AbfsConnectionManager(
        createSocketFactoryRegistry(
            new SSLConnectionSocketFactory(delegatingSSLSocketFactory,
                getDefaultHostnameVerifier())),
        new AbfsHttpClientConnectionFactory(), keepAliveCache,
        abfsConfiguration, baseUrl, isCacheWarmupNeeded);
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(
            // In case of Expect:100-continue, the timeout for waiting for
            // the 100-continue response from the server is set using
            // ExpectWaitContinueTimeout. For other requests, the read timeout
            // is set using SocketTimeout.
            new AbfsManagedHttpRequestExecutor(
                abfsConfiguration.isExpectHeaderEnabled()
                    ? abfsConfiguration.getExpect100ContinueWaitTimeout()
                    : abfsConfiguration.getHttpReadTimeout()))
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
      final int readTimeout,
      final long tailLatencyTimeout) throws IOException {
    if (tailLatencyTimeout <= 0) {
      return executeWithoutDeadline(httpRequest, abfsHttpClientContext,
          connectTimeout, readTimeout);
    }
    return executeWithDeadline(httpRequest, abfsHttpClientContext,
        connectTimeout, readTimeout, tailLatencyTimeout);
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
  private HttpResponse executeWithoutDeadline(HttpRequestBase httpRequest,
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
   * Executes the HTTP request with a deadline. If the request does not complete
   * within the deadline, it is aborted and an IOException is thrown.
   *
   * @param httpRequest HTTP request to execute.
   * @param abfsHttpClientContext HttpClient context.
   * @param connectTimeout Connection timeout.
   * @param readTimeout Read timeout.
   * @param deadlineMillis Deadline in milliseconds.
   *
   * @return HTTP response.
   * @throws IOException network error or deadline exceeded.
   */
  private HttpResponse executeWithDeadline(HttpRequestBase httpRequest,
      final AbfsManagedHttpClientContext abfsHttpClientContext,
      final int connectTimeout,
      final int readTimeout,
      final long deadlineMillis) throws IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(connectTimeout)
        .setSocketTimeout(readTimeout);
    httpRequest.setConfig(requestConfigBuilder.build());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<HttpResponse> future = executor.submit(() ->
        httpClient.execute(httpRequest, abfsHttpClientContext)
    );

    try {
      return future.get(deadlineMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      /* Deadline exceeded, abort the request.
       * This will also kill the underlying socket exception in the HttpClient.
       * Connection will be marked stale and won't be returned back to KAC for reuse.
       */
      httpRequest.abort();
      throw new TailLatencyRequestTimeoutException(e);
    } catch (Exception e) {
      // Any other exception from execution should be thrown as IOException.
      throw new IOException("Request execution with deadline failed", e);
    } finally {
      executor.shutdownNow();
    }
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
