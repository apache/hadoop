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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticator;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;

/**
 * Utility Connector class which is used by timeline clients to securely get
 * connected to the timeline server.
 *
 */
public class TimelineConnector extends AbstractService {

  private static final Joiner JOINER = Joiner.on("");
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineConnector.class);
  public final static int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000; // 1 minute

  private SSLFactory sslFactory;
  private Client client;
  private ConnectionConfigurator connConfigurator;
  private DelegationTokenAuthenticator authenticator;
  private DelegationTokenAuthenticatedURL.Token token;
  private UserGroupInformation authUgi;
  private String doAsUser;
  @VisibleForTesting
  TimelineClientConnectionRetry connectionRetry;
  private boolean requireConnectionRetry;

  public TimelineConnector(boolean requireConnectionRetry,
      UserGroupInformation authUgi, String doAsUser,
      DelegationTokenAuthenticatedURL.Token token) {
    super("TimelineConnector");
    this.requireConnectionRetry = requireConnectionRetry;
    this.authUgi = authUgi;
    this.doAsUser = doAsUser;
    this.token = token;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(YarnJacksonJaxbJsonProvider.class);

    if (YarnConfiguration.useHttps(conf)) {
      // If https is chosen, configures SSL client.
      sslFactory = getSSLFactory(conf);
      connConfigurator = getConnConfigurator(sslFactory);
    } else {
      connConfigurator = DEFAULT_TIMEOUT_CONN_CONFIGURATOR;
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      authenticator = new KerberosDelegationTokenAuthenticator();
    } else {
      authenticator = new PseudoDelegationTokenAuthenticator();
    }
    authenticator.setConnectionConfigurator(connConfigurator);

    connectionRetry = new TimelineClientConnectionRetry(conf);
    client =
        new Client(
            new URLConnectionClientHandler(new TimelineURLConnectionFactory(
                authUgi, authenticator, connConfigurator, token, doAsUser)),
            cc);
    if (requireConnectionRetry) {
      TimelineJerseyRetryFilter retryFilter =
          new TimelineJerseyRetryFilter(connectionRetry);
      client.addFilter(retryFilter);
    }
  }

  private static final ConnectionConfigurator DEFAULT_TIMEOUT_CONN_CONFIGURATOR
    = new ConnectionConfigurator() {
        @Override
        public HttpURLConnection configure(HttpURLConnection conn)
            throws IOException {
          setTimeouts(conn, DEFAULT_SOCKET_TIMEOUT);
          return conn;
        }
      };

  private ConnectionConfigurator getConnConfigurator(SSLFactory sslFactoryObj) {
    try {
      return initSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, sslFactoryObj);
    } catch (Exception e) {
      LOG.debug("Cannot load customized ssl related configuration. "
          + "Fallback to system-generic settings.", e);
      return DEFAULT_TIMEOUT_CONN_CONFIGURATOR;
    }
  }

  private static ConnectionConfigurator initSslConnConfigurator(
      final int timeout, SSLFactory sslFactory)
      throws IOException, GeneralSecurityException {
    final SSLSocketFactory sf;
    final HostnameVerifier hv;

    sf = sslFactory.createSSLSocketFactory();
    hv = sslFactory.getHostnameVerifier();

    return new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        if (conn instanceof HttpsURLConnection) {
          HttpsURLConnection c = (HttpsURLConnection) conn;
          c.setSSLSocketFactory(sf);
          c.setHostnameVerifier(hv);
        }
        setTimeouts(conn, timeout);
        return conn;
      }
    };
  }

  protected SSLFactory getSSLFactory(Configuration conf)
      throws GeneralSecurityException, IOException {
    SSLFactory newSSLFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    newSSLFactory.init();
    return newSSLFactory;
  }

  private static void setTimeouts(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
  }

  public static URI constructResURI(Configuration conf, String address,
      String uri) {
    return URI.create(
        JOINER.join(YarnConfiguration.useHttps(conf) ? "https://" : "http://",
            address, uri));
  }

  DelegationTokenAuthenticatedURL getDelegationTokenAuthenticatedURL() {
    return new DelegationTokenAuthenticatedURL(authenticator, connConfigurator);
  }

  protected void serviceStop() {
    if (this.sslFactory != null) {
      this.sslFactory.destroy();
    }
  }

  public Client getClient() {
    return client;
  }

  public Object operateDelegationToken(
      final PrivilegedExceptionAction<?> action)
      throws IOException, YarnException {
    // Set up the retry operation
    TimelineClientRetryOp tokenRetryOp =
        createRetryOpForOperateDelegationToken(action);

    return connectionRetry.retryOn(tokenRetryOp);
  }

  @Private
  @VisibleForTesting
  TimelineClientRetryOp createRetryOpForOperateDelegationToken(
      final PrivilegedExceptionAction<?> action) throws IOException {
    return new TimelineClientRetryOpForOperateDelegationToken(this.authUgi,
        action);
  }

  /**
   * Abstract class for an operation that should be retried by timeline client.
   */
  @Private
  @VisibleForTesting
  public static abstract class TimelineClientRetryOp {
    // The operation that should be retried
    public abstract Object run() throws IOException;

    // The method to indicate if we should retry given the incoming exception
    public abstract boolean shouldRetryOn(Exception e);
  }

  private static class TimelineURLConnectionFactory
      implements HttpURLConnectionFactory {
    private DelegationTokenAuthenticator authenticator;
    private UserGroupInformation authUgi;
    private ConnectionConfigurator connConfigurator;
    private Token token;
    private String doAsUser;

    public TimelineURLConnectionFactory(UserGroupInformation authUgi,
        DelegationTokenAuthenticator authenticator,
        ConnectionConfigurator connConfigurator,
        DelegationTokenAuthenticatedURL.Token token, String doAsUser) {
      this.authUgi = authUgi;
      this.authenticator = authenticator;
      this.connConfigurator = connConfigurator;
      this.token = token;
      this.doAsUser = doAsUser;
    }

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
        throws IOException {
      authUgi.checkTGTAndReloginFromKeytab();
      try {
        return new DelegationTokenAuthenticatedURL(authenticator,
            connConfigurator).openConnection(url, token, doAsUser);
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      } catch (AuthenticationException ae) {
        throw new IOException(ae);
      }
    }

  }

  // Class to handle retry
  // Outside this class, only visible to tests
  @Private
  @VisibleForTesting
  static class TimelineClientConnectionRetry {

    // maxRetries < 0 means keep trying
    @Private
    @VisibleForTesting
    public int maxRetries;

    @Private
    @VisibleForTesting
    public long retryInterval;

    // Indicates if retries happened last time. Only tests should read it.
    // In unit tests, retryOn() calls should _not_ be concurrent.
    private boolean retried = false;

    @Private
    @VisibleForTesting
    boolean getRetired() {
      return retried;
    }

    // Constructor with default retry settings
    public TimelineClientConnectionRetry(Configuration conf) {
      Preconditions.checkArgument(
          conf.getInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES)
              >= -1,
          "%s property value should be greater than or equal to -1",
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
      Preconditions.checkArgument(
          conf.getLong(
              YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
              YarnConfiguration.
                DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS) > 0,
          "%s property value should be greater than zero",
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
      maxRetries =
          conf.getInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
      retryInterval = conf.getLong(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
    }

    public Object retryOn(TimelineClientRetryOp op)
        throws RuntimeException, IOException {
      int leftRetries = maxRetries;
      retried = false;

      // keep trying
      while (true) {
        try {
          // try perform the op, if fail, keep retrying
          return op.run();
        } catch (IOException | RuntimeException e) {
          // break if there's no retries left
          if (leftRetries == 0) {
            break;
          }
          if (op.shouldRetryOn(e)) {
            logException(e, leftRetries);
          } else {
            throw e;
          }
        }
        if (leftRetries > 0) {
          leftRetries--;
        }
        retried = true;
        try {
          // sleep for the given time interval
          Thread.sleep(retryInterval);
        } catch (InterruptedException ie) {
          LOG.warn("Client retry sleep interrupted! ");
        }
      }
      throw new RuntimeException("Failed to connect to timeline server. "
          + "Connection retries limit exceeded. "
          + "The posted timeline event may be missing");
    };

    private void logException(Exception e, int leftRetries) {
      if (leftRetries > 0) {
        LOG.info(
            "Exception caught by TimelineClientConnectionRetry," + " will try "
                + leftRetries + " more time(s).\nMessage: " + e.getMessage());
      } else {
        // note that maxRetries may be -1 at the very beginning
        LOG.info("ConnectionException caught by TimelineClientConnectionRetry,"
            + " will keep retrying.\nMessage: " + e.getMessage());
      }
    }
  }

  private static class TimelineJerseyRetryFilter extends ClientFilter {
    private TimelineClientConnectionRetry connectionRetry;

    public TimelineJerseyRetryFilter(
        TimelineClientConnectionRetry connectionRetry) {
      this.connectionRetry = connectionRetry;
    }

    @Override
    public ClientResponse handle(final ClientRequest cr)
        throws ClientHandlerException {
      // Set up the retry operation
      TimelineClientRetryOp jerseyRetryOp = new TimelineClientRetryOp() {
        @Override
        public Object run() {
          // Try pass the request, if fail, keep retrying
          return getNext().handle(cr);
        }

        @Override
        public boolean shouldRetryOn(Exception e) {
          // Only retry on connection exceptions
          return (e instanceof ClientHandlerException)
              && (e.getCause() instanceof ConnectException
                  || e.getCause() instanceof SocketTimeoutException
                  || e.getCause() instanceof SocketException);
        }
      };
      try {
        return (ClientResponse) connectionRetry.retryOn(jerseyRetryOp);
      } catch (IOException e) {
        throw new ClientHandlerException(
            "Jersey retry failed!\nMessage: " + e.getMessage());
      }
    }
  }

  @Private
  @VisibleForTesting
  public static class TimelineClientRetryOpForOperateDelegationToken
      extends TimelineClientRetryOp {

    private final UserGroupInformation authUgi;
    private final PrivilegedExceptionAction<?> action;

    public TimelineClientRetryOpForOperateDelegationToken(
        UserGroupInformation authUgi, PrivilegedExceptionAction<?> action) {
      this.authUgi = authUgi;
      this.action = action;
    }

    @Override
    public Object run() throws IOException {
      // Try pass the request, if fail, keep retrying
      authUgi.checkTGTAndReloginFromKeytab();
      try {
        return authUgi.doAs(action);
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean shouldRetryOn(Exception e) {
      // retry on connection exceptions
      // and SocketTimeoutException
      return (e instanceof ConnectException
          || e instanceof SocketTimeoutException);
    }
  }
}
