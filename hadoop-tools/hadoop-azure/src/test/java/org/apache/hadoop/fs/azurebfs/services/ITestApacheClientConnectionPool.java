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
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ClosedIOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.functional.Tuples;
import org.apache.http.HttpHost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KEEP_ALIVE_CACHE_CLOSED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_METRIC_FORMAT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_NETWORKING_LIBRARY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.HTTPS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.verifyCause;
import static org.apache.http.conn.ssl.SSLConnectionSocketFactory.getDefaultHostnameVerifier;

/**
 * This test class tests the exception handling in ABFS thrown by the
 * {@link KeepAliveCache}.
 */
public class ITestApacheClientConnectionPool extends
    AbstractAbfsIntegrationTest {

  public ITestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testKacIsClosed() throws Throwable {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_NETWORKING_LIBRARY, APACHE_HTTP_CLIENT.name());
    configuration.unset(FS_AZURE_METRIC_FORMAT);
    try (AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration)) {
      KeepAliveCache kac = fs.getAbfsStore().getClientHandler().getIngressClient()
          .getKeepAliveCache();
      kac.close();
      AbfsDriverException ex = intercept(AbfsDriverException.class,
          KEEP_ALIVE_CACHE_CLOSED, () -> {
            fs.create(new Path("/test"));
          });
      verifyCause(ClosedIOException.class, ex);
    }
  }

  @Test
  public void testNonConnectedConnectionLogging() throws Exception {
    Map.Entry<HttpRoute, AbfsManagedApacheHttpConnection> testConnPair
        = getTestConnection();
    AbfsManagedApacheHttpConnection conn = testConnPair.getValue();
    String log = conn.toString();
    Assertions.assertThat(log.split(COLON).length)
        .describedAs("Log to have three fields: https://host:port:hashCode")
        .isEqualTo(4);
  }

  @Test
  public void testConnectedConnectionLogging() throws Exception {
    Map.Entry<HttpRoute, AbfsManagedApacheHttpConnection> testConnPair
        = getTestConnection();
    AbfsManagedApacheHttpConnection conn = testConnPair.getValue();
    HttpRoute httpRoute = testConnPair.getKey();

    Registry<ConnectionSocketFactory> socketFactoryRegistry
        = RegistryBuilder.<ConnectionSocketFactory>create()
        .register(HTTPS_SCHEME, new SSLConnectionSocketFactory(
            DelegatingSSLSocketFactory.getDefaultFactory(),
            getDefaultHostnameVerifier()))
        .build();
    new DefaultHttpClientConnectionOperator(
        socketFactoryRegistry, null, null).connect(conn,
        httpRoute.getTargetHost(), httpRoute.getLocalSocketAddress(),
        getConfiguration().getHttpConnectionTimeout(), SocketConfig.DEFAULT,
        new HttpClientContext());

    String log = conn.toString();
    Assertions.assertThat(log.split(COLON).length)
        .describedAs("Log to have three fields: https://host:port:hashCode")
        .isEqualTo(4);
  }

  private Map.Entry<HttpRoute, AbfsManagedApacheHttpConnection> getTestConnection()
      throws IOException {
    HttpHost host = new HttpHost(getFileSystem().getUri().getHost(),
        getFileSystem().getUri().getPort(),
        HTTPS_SCHEME);
    HttpRoute httpRoute = new HttpRoute(host);

    AbfsManagedApacheHttpConnection conn
        = (AbfsManagedApacheHttpConnection) new AbfsHttpClientConnectionFactory().create(
        httpRoute, null);

    return Tuples.pair(httpRoute, conn);
  }
}
