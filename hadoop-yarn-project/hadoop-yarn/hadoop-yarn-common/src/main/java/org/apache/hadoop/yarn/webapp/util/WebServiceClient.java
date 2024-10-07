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
package org.apache.hadoop.yarn.webapp.util;

import java.io.IOException;
import java.net.HttpURLConnection;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * Utility for handling Web client.
 *
 */
public class WebServiceClient {
  private static SSLFactory sslFactory = null;
  private static volatile WebServiceClient instance = null;
  private static boolean isHttps = false;

  /**
   * Construct a new WebServiceClient based on the configuration. It will try to
   * load SSL certificates when it is specified.
   *
   * @param conf configuration.
   * @throws Exception exception occur.
   */
  public static void initialize(Configuration conf) throws Exception {
    if (instance == null) {
      synchronized (WebServiceClient.class) {
        if (instance == null) {
          isHttps = YarnConfiguration.useHttps(conf);
          if (isHttps) {
            createSSLFactory(conf);
          }
          instance = new WebServiceClient();
        }
      }
    }
  }

  public static WebServiceClient getWebServiceClient() {
    return instance;
  }

  @VisibleForTesting
  SSLFactory getSSLFactory() {
    return sslFactory;
  }

  /**
   * Start SSL factory.
   *
   * @param conf configuration.
   * @return SSL factory.
   * @throws Exception exception occur.
   */
  private static SSLFactory createSSLFactory(Configuration conf)
      throws Exception {
    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
    return sslFactory;
  }

  /**
   * Create a client based on http conf.
   *
   * @return Client
   */
  public Client createClient() {
    final ClientConfig cc = new ClientConfig();
    cc.connectorProvider(getHttpURLConnectionFactory());
    return ClientBuilder.newClient(cc);
  }

  @VisibleForTesting
  protected HttpUrlConnectorProvider getHttpURLConnectionFactory() {
    return new HttpUrlConnectorProvider().connectionFactory(
        url -> {
          AuthenticatedURL.Token token = new AuthenticatedURL.Token();
          HttpURLConnection conn;
          try {
            HttpURLConnection.setFollowRedirects(false);
            // If https is chosen, configures SSL client.
            if (isHttps) {
              conn = new AuthenticatedURL(new KerberosAuthenticator(),
                  sslFactory).openConnection(url, token);
            } else {
              conn = new AuthenticatedURL().openConnection(url, token);
            }
          } catch (AuthenticationException e) {
            throw new IOException(e);
          }
          return conn;
        });
  }

  public synchronized static void destroy() {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    instance = null;
  }

}
