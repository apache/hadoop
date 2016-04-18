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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.security.PrivilegedExceptionAction;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticator;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomains;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.codehaus.jackson.map.ObjectMapper;
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

@Private
@Evolving
public class TimelineClientImpl extends TimelineClient {

  private static final Log LOG = LogFactory.getLog(TimelineClientImpl.class);
  private static final String RESOURCE_URI_STR = "/ws/v1/timeline/";
  private static final Joiner JOINER = Joiner.on("");
  public final static int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000; // 1 minute

  private static Options opts;
  private static final String ENTITY_DATA_TYPE = "entity";
  private static final String DOMAIN_DATA_TYPE = "domain";

  static {
    opts = new Options();
    opts.addOption("put", true, "Put the timeline entities/domain in a JSON file");
    opts.getOption("put").setArgName("Path to the JSON file");
    opts.addOption(ENTITY_DATA_TYPE, false, "Specify the JSON file contains the entities");
    opts.addOption(DOMAIN_DATA_TYPE, false, "Specify the JSON file contains the domain");
    opts.addOption("help", false, "Print usage");
  }

  private Client client;
  private ConnectionConfigurator connConfigurator;
  private DelegationTokenAuthenticator authenticator;
  private DelegationTokenAuthenticatedURL.Token token;
  private URI resURI;
  private UserGroupInformation authUgi;
  private String doAsUser;
  private Configuration configuration;
  private float timelineServiceVersion;
  private TimelineWriter timelineWriter;

  @Private
  @VisibleForTesting
  TimelineClientConnectionRetry connectionRetry;

  // Abstract class for an operation that should be retried by timeline client
  @Private
  @VisibleForTesting
  public static abstract class TimelineClientRetryOp {
    // The operation that should be retried
    public abstract Object run() throws IOException;
    // The method to indicate if we should retry given the incoming exception
    public abstract boolean shouldRetryOn(Exception e);
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
      Preconditions.checkArgument(conf.getInt(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES) >= -1,
          "%s property value should be greater than or equal to -1",
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
      Preconditions
          .checkArgument(
              conf.getLong(
                  YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
                  YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS) > 0,
              "%s property value should be greater than zero",
              YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
      maxRetries = conf.getInt(
        YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
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
        LOG.info("Exception caught by TimelineClientConnectionRetry,"
              + " will try " + leftRetries + " more time(s).\nMessage: "
              + e.getMessage());
      } else {
        // note that maxRetries may be -1 at the very beginning
        LOG.info("ConnectionException caught by TimelineClientConnectionRetry,"
            + " will keep retrying.\nMessage: "
            + e.getMessage());
      }
    }
  }

  private class TimelineJerseyRetryFilter extends ClientFilter {
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
              && (e.getCause() instanceof ConnectException);
        }
      };
      try {
        return (ClientResponse) connectionRetry.retryOn(jerseyRetryOp);
      } catch (IOException e) {
        throw new ClientHandlerException("Jersey retry failed!\nMessage: "
              + e.getMessage());
      }
    }
  }

  public TimelineClientImpl() {
    super(TimelineClientImpl.class.getName());
  }

  protected void serviceInit(Configuration conf) throws Exception {
    this.configuration = conf;
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    UserGroupInformation realUgi = ugi.getRealUser();
    if (realUgi != null) {
      authUgi = realUgi;
      doAsUser = ugi.getShortUserName();
    } else {
      authUgi = ugi;
      doAsUser = null;
    }
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    connConfigurator = newConnConfigurator(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      authenticator = new KerberosDelegationTokenAuthenticator();
    } else {
      authenticator = new PseudoDelegationTokenAuthenticator();
    }
    authenticator.setConnectionConfigurator(connConfigurator);
    token = new DelegationTokenAuthenticatedURL.Token();

    connectionRetry = new TimelineClientConnectionRetry(conf);
    client = new Client(new URLConnectionClientHandler(
        new TimelineURLConnectionFactory()), cc);
    TimelineJerseyRetryFilter retryFilter = new TimelineJerseyRetryFilter();
    client.addFilter(retryFilter);

    if (YarnConfiguration.useHttps(conf)) {
      resURI = URI
          .create(JOINER.join("https://", conf.get(
              YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS),
              RESOURCE_URI_STR));
    } else {
      resURI = URI.create(JOINER.join("http://", conf.get(
          YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS),
          RESOURCE_URI_STR));
    }
    LOG.info("Timeline service address: " + resURI);
    timelineServiceVersion =
        conf.getFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_VERSION);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    timelineWriter = createTimelineWriter(
        configuration, authUgi, client, resURI);
  }

  protected TimelineWriter createTimelineWriter(Configuration conf,
      UserGroupInformation ugi, Client webClient, URI uri)
      throws IOException {
    if (Float.compare(this.timelineServiceVersion, 1.5f) == 0) {
      return new FileSystemTimelineWriter(
          conf, ugi, webClient, uri);
    } else {
      return new DirectTimelineWriter(ugi, webClient, uri);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.timelineWriter != null) {
      this.timelineWriter.close();
    }
    super.serviceStop();
  }

  @Override
  public void flush() throws IOException {
    if (timelineWriter != null) {
      timelineWriter.flush();
    }
  }

  @Override
  public TimelinePutResponse putEntities(
      TimelineEntity... entities) throws IOException, YarnException {
    return timelineWriter.putEntities(entities);
  }


  @Override
  public void putDomain(TimelineDomain domain) throws IOException,
      YarnException {
    timelineWriter.putDomain(domain);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException, YarnException {
    PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>> getDTAction =
        new PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>() {

          @Override
          public Token<TimelineDelegationTokenIdentifier> run()
              throws Exception {
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            return (Token) authUrl.getDelegationToken(
                resURI.toURL(), token, renewer, doAsUser);
          }
        };
    return (Token<TimelineDelegationTokenIdentifier>) operateDelegationToken(getDTAction);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long renewDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
          throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Long> renewDTAction =
        new PrivilegedExceptionAction<Long>() {

          @Override
          public Long run() throws Exception {
            // If the timeline DT to renew is different than cached, replace it.
            // Token to set every time for retry, because when exception happens,
            // DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty ? resURI
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR, null, null);
            return authUrl
                .renewDelegationToken(serviceURI.toURL(), token, doAsUser);
          }
        };
    return (Long) operateDelegationToken(renewDTAction);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void cancelDelegationToken(
      final Token<TimelineDelegationTokenIdentifier> timelineDT)
          throws IOException, YarnException {
    final boolean isTokenServiceAddrEmpty =
        timelineDT.getService().toString().isEmpty();
    final String scheme = isTokenServiceAddrEmpty ? null
        : (YarnConfiguration.useHttps(this.getConfig()) ? "https" : "http");
    final InetSocketAddress address = isTokenServiceAddrEmpty ? null
        : SecurityUtil.getTokenServiceAddr(timelineDT);
    PrivilegedExceptionAction<Void> cancelDTAction =
        new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception {
            // If the timeline DT to cancel is different than cached, replace it.
            // Token to set every time for retry, because when exception happens,
            // DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty ? resURI
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR, null, null);
            authUrl.cancelDelegationToken(serviceURI.toURL(), token, doAsUser);
            return null;
          }
        };
    operateDelegationToken(cancelDTAction);
  }

  @Override
  public String toString() {
    return super.toString() + " with timeline server " + resURI
        + " and writer " + timelineWriter;
  }

  private Object operateDelegationToken(
      final PrivilegedExceptionAction<?> action)
      throws IOException, YarnException {
    // Set up the retry operation
    TimelineClientRetryOp tokenRetryOp =
        createTimelineClientRetryOpForOperateDelegationToken(action);

    return connectionRetry.retryOn(tokenRetryOp);
  }

  private class TimelineURLConnectionFactory
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url) throws IOException {
      authUgi.checkTGTAndReloginFromKeytab();
      try {
        return new DelegationTokenAuthenticatedURL(
            authenticator, connConfigurator).openConnection(url, token,
              doAsUser);
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      } catch (AuthenticationException ae) {
        throw new IOException(ae);
      }
    }

  }

  private static ConnectionConfigurator newConnConfigurator(Configuration conf) {
    try {
      return newSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf);
    } catch (Exception e) {
      LOG.debug("Cannot load customized ssl related configuration. " +
          "Fallback to system-generic settings.", e);
      return DEFAULT_TIMEOUT_CONN_CONFIGURATOR;
    }
  }

  private static final ConnectionConfigurator DEFAULT_TIMEOUT_CONN_CONFIGURATOR =
      new ConnectionConfigurator() {
    @Override
    public HttpURLConnection configure(HttpURLConnection conn)
        throws IOException {
      setTimeouts(conn, DEFAULT_SOCKET_TIMEOUT);
      return conn;
    }
  };

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
        setTimeouts(conn, timeout);
        return conn;
      }
    };
  }

  private static void setTimeouts(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
  }

  public static void main(String[] argv) throws Exception {
    CommandLine cliParser = new GnuParser().parse(opts, argv);
    if (cliParser.hasOption("put")) {
      String path = cliParser.getOptionValue("put");
      if (path != null && path.length() > 0) {
        if (cliParser.hasOption(ENTITY_DATA_TYPE)) {
          putTimelineDataInJSONFile(path, ENTITY_DATA_TYPE);
          return;
        } else if (cliParser.hasOption(DOMAIN_DATA_TYPE)) {
          putTimelineDataInJSONFile(path, DOMAIN_DATA_TYPE);
          return;
        }
      }
    }
    printUsage();
  }

  /**
   * Put timeline data in a JSON file via command line.
   * 
   * @param path
   *          path to the timeline data JSON file
   * @param type
   *          the type of the timeline data in the JSON file
   */
  private static void putTimelineDataInJSONFile(String path, String type) {
    File jsonFile = new File(path);
    if (!jsonFile.exists()) {
      LOG.error("File [" + jsonFile.getAbsolutePath() + "] doesn't exist");
      return;
    }
    ObjectMapper mapper = new ObjectMapper();
    YarnJacksonJaxbJsonProvider.configObjectMapper(mapper);
    TimelineEntities entities = null;
    TimelineDomains domains = null;
    try {
      if (type.equals(ENTITY_DATA_TYPE)) {
        entities = mapper.readValue(jsonFile, TimelineEntities.class);
      } else if (type.equals(DOMAIN_DATA_TYPE)){
        domains = mapper.readValue(jsonFile, TimelineDomains.class);
      }
    } catch (Exception e) {
      LOG.error("Error when reading  " + e.getMessage());
      e.printStackTrace(System.err);
      return;
    }
    Configuration conf = new YarnConfiguration();
    TimelineClient client = TimelineClient.createTimelineClient();
    client.init(conf);
    client.start();
    try {
      if (UserGroupInformation.isSecurityEnabled()
          && conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, false)) {
        Token<TimelineDelegationTokenIdentifier> token =
            client.getDelegationToken(
                UserGroupInformation.getCurrentUser().getUserName());
        UserGroupInformation.getCurrentUser().addToken(token);
      }
      if (type.equals(ENTITY_DATA_TYPE)) {
        TimelinePutResponse response = client.putEntities(
            entities.getEntities().toArray(
                new TimelineEntity[entities.getEntities().size()]));
        if (response.getErrors().size() == 0) {
          LOG.info("Timeline entities are successfully put");
        } else {
          for (TimelinePutResponse.TimelinePutError error : response.getErrors()) {
            LOG.error("TimelineEntity [" + error.getEntityType() + ":" +
                error.getEntityId() + "] is not successfully put. Error code: " +
                error.getErrorCode());
          }
        }
      } else if (type.equals(DOMAIN_DATA_TYPE)) {
        boolean hasError = false;
        for (TimelineDomain domain : domains.getDomains()) {
          try {
            client.putDomain(domain);
          } catch (Exception e) {
            LOG.error("Error when putting domain " + domain.getId(), e);
            hasError = true;
          }
        }
        if (!hasError) {
          LOG.info("Timeline domains are successfully put");
        }
      }
    } catch(RuntimeException e) {
      LOG.error("Error when putting the timeline data", e);
    } catch (Exception e) {
      LOG.error("Error when putting the timeline data", e);
    } finally {
      client.stop();
    }
  }

  /**
   * Helper function to print out usage
   */
  private static void printUsage() {
    new HelpFormatter().printHelp("TimelineClient", opts);
  }

  @VisibleForTesting
  @Private
  public UserGroupInformation getUgi() {
    return authUgi;
  }

  @Override
  public TimelinePutResponse putEntities(ApplicationAttemptId appAttemptId,
      TimelineEntityGroupId groupId, TimelineEntity... entities)
      throws IOException, YarnException {
    if (Float.compare(this.timelineServiceVersion, 1.5f) != 0) {
      throw new YarnException(
        "This API is not supported under current Timeline Service Version: "
            + timelineServiceVersion);
    }

    return timelineWriter.putEntities(appAttemptId, groupId, entities);
  }

  @Override
  public void putDomain(ApplicationAttemptId appAttemptId,
      TimelineDomain domain) throws IOException, YarnException {
    if (Float.compare(this.timelineServiceVersion, 1.5f) != 0) {
      throw new YarnException(
        "This API is not supported under current Timeline Service Version: "
            + timelineServiceVersion);
    }
    timelineWriter.putDomain(appAttemptId, domain);
  }

  @Private
  @VisibleForTesting
  public void setTimelineWriter(TimelineWriter writer) {
    this.timelineWriter = writer;
  }

  @Private
  @VisibleForTesting
  public TimelineClientRetryOp
      createTimelineClientRetryOpForOperateDelegationToken(
          final PrivilegedExceptionAction<?> action) throws IOException {
    return new TimelineClientRetryOpForOperateDelegationToken(
        this.authUgi, action);
  }

  @Private
  @VisibleForTesting
  public class TimelineClientRetryOpForOperateDelegationToken
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
