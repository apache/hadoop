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

package org.apache.slider.core.restclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.KerberosUgiAuthenticator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;

/**
 * Factory for URL connections; used behind the scenes in the Jersey integration.
 * <p>
 * Derived from the WebHDFS implementation.
 */
public class SliderURLConnectionFactory {
  private static final Logger log =
      LoggerFactory.getLogger(SliderURLConnectionFactory.class);

  /**
   * Timeout for socket connects and reads
   */
  public final static int DEFAULT_SOCKET_TIMEOUT = 60 * 1000; // 1 minute
  private final ConnectionConfigurator connConfigurator;

  private static final ConnectionConfigurator DEFAULT_CONFIGURATOR = new BasicConfigurator();

  /**
   * Construct a new URLConnectionFactory based on the configuration. It will
   * try to load SSL certificates when it is specified.
   */
  public static SliderURLConnectionFactory newInstance(Configuration conf) {
    ConnectionConfigurator conn;
    try {
      conn = newSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf);
    } catch (Exception e) {
      log.debug("Cannot load customized SSL configuration.", e);
      conn = DEFAULT_CONFIGURATOR;
    }
    return new SliderURLConnectionFactory(conn);
  }

  private SliderURLConnectionFactory(ConnectionConfigurator connConfigurator) {
    this.connConfigurator = connConfigurator;
  }

  /**
   * Create a new ConnectionConfigurator for SSL connections
   */
  private static ConnectionConfigurator newSslConnConfigurator(final int timeout,
      Configuration conf) throws IOException, GeneralSecurityException {
    final SSLFactory factory;
    final SSLSocketFactory sf;
    final HostnameVerifier hv;

    factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    factory.init();
    sf = factory.createSSLSocketFactory();
    hv = factory.getHostnameVerifier();

    return new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        if (conn instanceof HttpsURLConnection) {
          HttpsURLConnection c = (HttpsURLConnection) conn;
          c.setSSLSocketFactory(sf);
          c.setHostnameVerifier(hv);
        }
        SliderURLConnectionFactory.setupConnection(conn, timeout);
        return conn;
      }
    };
  }

  /**
   * Opens a url with read and connect timeouts
   *
   * @param url
   *          to open
   * @return URLConnection
   * @throws IOException
   */
  public URLConnection openConnection(URL url) throws IOException {
    try {
      return openConnection(url, false);
    } catch (AuthenticationException e) {
      // Unreachable
      return null;
    }
  }

  /**
   * Opens a url with read and connect timeouts
   *
   * @param url
   *          URL to open
   * @param isSpnego
   *          whether the url should be authenticated via SPNEGO
   * @return URLConnection
   * @throws IOException
   * @throws AuthenticationException
   */
  public URLConnection openConnection(URL url, boolean isSpnego)
      throws IOException, AuthenticationException {
    if (isSpnego) {
        log.debug("open AuthenticatedURL connection {}", url);
      UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
      final AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
      return new AuthenticatedURL(new KerberosUgiAuthenticator(),
          connConfigurator).openConnection(url, authToken);
    } else {
      log.debug("open URL connection {}", url);
      URLConnection connection = url.openConnection();
      if (connection instanceof HttpURLConnection) {
        connConfigurator.configure((HttpURLConnection) connection);
      }
      return connection;
    }
  }

  /**
   * Sets connection parameters on the given URLConnection
   * 
   * @param connection
   *          URLConnection to set
   * @param socketTimeout
   *          the connection and read timeout of the connection.
   */
  private static void setupConnection(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
    connection.setUseCaches(false);
    if (connection instanceof HttpURLConnection) {
      ((HttpURLConnection) connection).setInstanceFollowRedirects(true);
    }
  }

  private static class BasicConfigurator implements ConnectionConfigurator {
    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
        throws IOException {
      SliderURLConnectionFactory.setupConnection(conn, DEFAULT_SOCKET_TIMEOUT);
      return conn;
    }
  }
}
