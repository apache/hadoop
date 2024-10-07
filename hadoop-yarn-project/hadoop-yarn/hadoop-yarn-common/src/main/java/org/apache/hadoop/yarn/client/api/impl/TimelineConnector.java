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
import java.time.Duration;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.glassfish.jersey.client.ClientConfig;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;

import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

/**
 * Utility Connector class which is used by timeline clients to securely get
 * connected to the timeline server.
 *
 */
public class TimelineConnector extends AbstractService {

  private static final Joiner JOINER = Joiner.on("");
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineConnector.class);

  private int socketTimeOut = 60_000;

  private SSLFactory sslFactory;
  Client client;
  private ConnectionConfigurator connConfigurator;
  private DelegationTokenAuthenticator authenticator;
  private DelegationTokenAuthenticatedURL.Token token;
  private UserGroupInformation authUgi;
  private String doAsUser;
  private boolean requireConnectionRetry;
  private RetryPolicy<Object> retryPolicy;

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
    ClientConfig cc = new ClientConfig();
    cc.register(YarnJacksonJaxbJsonProvider.class);

    if (YarnConfiguration.useHttps(conf)) {
      // If https is chosen, configures SSL client.
      sslFactory = getSSLFactory(conf);
      connConfigurator = getConnConfigurator(sslFactory);
    } else {
      connConfigurator = defaultTimeoutConnConfigurator;
    }
    String defaultAuth = UserGroupInformation.isSecurityEnabled() ?
            KerberosAuthenticationHandler.TYPE :
            PseudoAuthenticationHandler.TYPE;
    String authType = conf.get(YarnConfiguration.TIMELINE_HTTP_AUTH_TYPE,
            defaultAuth);
    if (authType.equals(KerberosAuthenticationHandler.TYPE)) {
      authenticator = new KerberosDelegationTokenAuthenticator();
    } else {
      authenticator = new PseudoDelegationTokenAuthenticator();
    }
    authenticator.setConnectionConfigurator(connConfigurator);

    retryPolicy = createRetryPolicy(conf);

    cc.connectorProvider(new HttpUrlConnectorProvider().connectionFactory(
        new TimelineURLConnectionFactory(
            authUgi, authenticator, connConfigurator, token, doAsUser)));

    client = ClientBuilder.newClient(cc);
  }

  private ConnectionConfigurator defaultTimeoutConnConfigurator = conn -> {
    setTimeouts(conn, socketTimeOut);
    return conn;
  };

  private ConnectionConfigurator getConnConfigurator(SSLFactory sslFactoryObj) {
    try {
      return initSslConnConfigurator(socketTimeOut, sslFactoryObj);
    } catch (Exception e) {
      LOG.debug("Cannot load customized ssl related configuration. "
          + "Fallback to system-generic settings.", e);
      return defaultTimeoutConnConfigurator;
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
    if (this.client != null) {
      this.client.close();
    }
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
    return Failsafe.with(retryPolicy).get(tokenRetryOp::run);
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
  }

  private static class TimelineURLConnectionFactory
      implements HttpUrlConnectorProvider.ConnectionFactory {
    private DelegationTokenAuthenticator authenticator;
    private UserGroupInformation authUgi;
    private ConnectionConfigurator connConfigurator;
    private Token token;
    private String doAsUser;

    TimelineURLConnectionFactory(UserGroupInformation authUgi,
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
    public HttpURLConnection getConnection(URL url) throws IOException {
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

  private RetryPolicy<Object> createRetryPolicy(Configuration conf) {
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
    int maxRetries = 0;
    if (requireConnectionRetry) {
      // maxRetries < 0 means keep trying
      maxRetries = conf.getInt(YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
    }
    long retryInterval = conf.getLong(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
    return new RetryPolicy<>()
        .handle(IOException.class, RuntimeException.class)
        .handleIf(e -> e instanceof ProcessingException
          && (e.getCause() instanceof ConnectException
          || e.getCause() instanceof SocketTimeoutException
          || e.getCause() instanceof SocketException))
        .withDelay(Duration.ofMillis(retryInterval))
        .withMaxRetries(maxRetries);
  }

  RetryPolicy<Object> getRetryPolicy() {
    return retryPolicy;
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
  }

  @VisibleForTesting
  public void setSocketTimeOut(int socketTimeOut) {
    this.socketTimeOut = socketTimeOut;
  }
}
