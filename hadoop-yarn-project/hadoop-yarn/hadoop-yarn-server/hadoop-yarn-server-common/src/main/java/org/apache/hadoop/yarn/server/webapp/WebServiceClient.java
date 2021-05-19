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
package org.apache.hadoop.yarn.server.webapp;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

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
   * @param conf Configuration object
   * @throws Exception if creation of SSLFactory fails
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
   * @param conf
   * @return
   * @throws Exception
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
    return new Client(
        new URLConnectionClientHandler(getHttpURLConnectionFactory()));
  }

  @VisibleForTesting
  protected HttpURLConnectionFactory getHttpURLConnectionFactory() {
    return new HttpURLConnectionFactory() {
      @Override
      public HttpURLConnection getHttpURLConnection(URL url)
          throws IOException {
        AuthenticatedURL.Token token = new AuthenticatedURL.Token();
        HttpURLConnection conn = null;
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
      }
    };
  }

  public synchronized static void destroy() {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
    instance = null;
  }

}