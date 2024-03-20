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
import java.util.Stack;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EXPECT_100_JDK_ERROR;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;
import static org.apache.http.conn.ssl.SSLConnectionSocketFactory.getDefaultHostnameVerifier;


public class AbfsApacheHttpClient {
  public static final Stack<Integer> connectionReuseCount = new Stack<>();
  public static final Stack<Integer> kacSizeStack = new Stack<>();

  public void close() throws IOException {
    if(httpClient != null) {
      httpClient.close();
    }
  }

  public static class AbfsHttpClientContext extends HttpClientContext {
    Long connectTime;
    long readTime;
    long sendTime;
    HttpClientConnection httpClientConnection;
    long expect100HeaderSendTime = 0L;
    long expect100ResponseTime;

    long keepAliveTime;

    Boolean isBeingRead = false;
    final Boolean isReadable;
    final AbfsRestOperationType abfsRestOperationType;

    public AbfsHttpClientContext(Boolean isReadable, AbfsRestOperationType operationType) {
      this.isReadable = isReadable;
      this.abfsRestOperationType = operationType;
    }

    /**
     * This to be used only in tests to get connection level activity.
     */
    protected HttpClientConnection interceptConnectionActivity(HttpClientConnection httpClientConnection) {
      return httpClientConnection;
    }
  }

  private static class AbfsHttpRequestExecutor extends HttpRequestExecutor {

    public AbfsHttpRequestExecutor(final int expect100WaitTimeout) {
      super(expect100WaitTimeout);
    }

    @Override
    protected HttpResponse doSendRequest(final HttpRequest request,
        final HttpClientConnection conn,
        final HttpContext context) throws IOException, HttpException {
      final HttpClientConnection inteceptedConnection;
      if(context instanceof AbfsHttpClientContext) {
        inteceptedConnection = ((AbfsHttpClientContext) context).interceptConnectionActivity(conn);
      } else {
        inteceptedConnection = conn;
      }
//      long start = System.currentTimeMillis();
      final HttpResponse res = super.doSendRequest(request, inteceptedConnection, context);
//      long elapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).httpClientConnection = conn;
//        ((AbfsHttpClientContext) context).sendTime = elapsed;
      }
      if(request != null && request.containsHeader(EXPECT) && res != null && res.getStatusLine().getStatusCode() != 200) {
        throw new AbfsApacheHttpExpect100Exception(EXPECT_100_JDK_ERROR, res);
      }
      return res;
    }

    @Override
    protected HttpResponse doReceiveResponse(final HttpRequest request,
        final HttpClientConnection conn,
        final HttpContext context) throws HttpException, IOException {
      final HttpClientConnection interceptedConnection;
      if(context instanceof AbfsHttpClientContext) {
        interceptedConnection = ((AbfsHttpClientContext) context).interceptConnectionActivity(conn);
      } else {
        interceptedConnection = conn;
      }
      long start = System.currentTimeMillis();
      final HttpResponse res = super.doReceiveResponse(request, interceptedConnection, context);
      long elapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).readTime = elapsed;
      }
      return res;
    }
  }

  final CloseableHttpClient httpClient;

  private final AbfsConnectionManager connMgr;

  private final AbfsConfiguration abfsConfiguration;

  public AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final AbfsConfiguration abfsConfiguration) {
    this.abfsConfiguration = abfsConfiguration;
    connMgr = new AbfsConnectionManager(createSocketFactoryRegistry(
        new SSLConnectionSocketFactory(delegatingSSLSocketFactory, getDefaultHostnameVerifier())),
        new org.apache.hadoop.fs.azurebfs.services.AbfsConnFactory());
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsHttpRequestExecutor(abfsConfiguration.getHttpReadTimeout()))
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        .setUserAgent(
            ""); // SDK will set the user agent header in the pipeline. Don't let Apache waste time
    httpClient = builder.build();
  }

  public HttpResponse execute(HttpRequestBase httpRequest, final AbfsHttpClientContext abfsHttpClientContext) throws IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(abfsConfiguration.getHttpConnectionTimeout())
        .setSocketTimeout(abfsConfiguration.getHttpReadTimeout());
    httpRequest.setConfig(requestConfigBuilder.build());
    return httpClient.execute(httpRequest, abfsHttpClientContext);
  }


  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(ConnectionSocketFactory sslSocketFactory) {
    if(sslSocketFactory == null) {
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory())
          .build();
    }
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https", sslSocketFactory)
        .build();
  }
}
