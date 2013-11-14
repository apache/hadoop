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

package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

/**
 * Utilities for handling URLs
 */
@InterfaceAudience.LimitedPrivate({ "HDFS" })
@InterfaceStability.Unstable
public class URLConnectionFactory {
  private static final Log LOG = LogFactory.getLog(URLConnectionFactory.class);

  /** SPNEGO authenticator */
  private static final KerberosUgiAuthenticator AUTH = new KerberosUgiAuthenticator();

  /**
   * Timeout for socket connects and reads
   */
  public final static int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000; // 1 minute

  public static final URLConnectionFactory DEFAULT_CONNECTION_FACTORY = new URLConnectionFactory(
      DEFAULT_SOCKET_TIMEOUT);

  private int socketTimeout;

  /** Configure connections for AuthenticatedURL */
  private ConnectionConfigurator connConfigurator = new ConnectionConfigurator() {
    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
        throws IOException {
      URLConnectionFactory.setTimeouts(conn, socketTimeout);
      return conn;
    }
  };

  public URLConnectionFactory(int socketTimeout) {
    this.socketTimeout = socketTimeout;
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("open AuthenticatedURL connection" + url);
      }
      UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
      final AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
      return new AuthenticatedURL(AUTH, connConfigurator).openConnection(url,
          authToken);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("open URL connection");
      }
      URLConnection connection = url.openConnection();
      if (connection instanceof HttpURLConnection) {
        connConfigurator.configure((HttpURLConnection) connection);
      }
      return connection;
    }
  }

  public ConnectionConfigurator getConnConfigurator() {
    return connConfigurator;
  }

  public void setConnConfigurator(ConnectionConfigurator connConfigurator) {
    this.connConfigurator = connConfigurator;
  }

  /**
   * Sets timeout parameters on the given URLConnection.
   * 
   * @param connection
   *          URLConnection to set
   * @param socketTimeout
   *          the connection and read timeout of the connection.
   */
  static void setTimeouts(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
  }
}
