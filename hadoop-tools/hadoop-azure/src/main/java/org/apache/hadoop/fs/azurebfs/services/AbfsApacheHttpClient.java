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

public class AbfsApacheHttpClient {
  private final CloseableHttpClient httpClient;

  static AbfsApacheHttpClient ABFS_APACHE_HTTP_CLIENT = null;

  static synchronized void setClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final int readTimeout) {
    if (ABFS_APACHE_HTTP_CLIENT == null) {
      ABFS_APACHE_HTTP_CLIENT = new AbfsApacheHttpClient(
          delegatingSSLSocketFactory, readTimeout);
    }
  }

  private AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final int readTimeout) {
    final AbfsConnectionManager connMgr = new AbfsConnectionManager(
        createSocketFactoryRegistry(
            new SSLConnectionSocketFactory(delegatingSSLSocketFactory,
                getDefaultHostnameVerifier())),
        new AbfsConnFactory());
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

  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

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


  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(
      ConnectionSocketFactory sslSocketFactory) {
    if (sslSocketFactory == null) {
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register(HTTP_SCHEME, PlainConnectionSocketFactory.getSocketFactory())
          .build();
    }
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register(HTTP_SCHEME, PlainConnectionSocketFactory.getSocketFactory())
        .register(HTTPS_SCHEME, sslSocketFactory)
        .build();
  }
}
