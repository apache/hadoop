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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator;
import org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticator;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import com.sun.jersey.core.util.MultivaluedMapImpl;

@Private
@Evolving
public class TimelineClientImpl extends TimelineClient {

  private static final Log LOG = LogFactory.getLog(TimelineClientImpl.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String RESOURCE_URI_STR_V1 = "/ws/v1/timeline/";
  private static final String RESOURCE_URI_STR_V2 = "/ws/v2/timeline/";
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
  private UserGroupInformation authUgi;
  private String doAsUser;
  private Configuration configuration;
  private float timelineServiceVersion;
  private TimelineWriter timelineWriter;
  private SSLFactory sslFactory;

  private volatile String timelineServiceAddress;

  // Retry parameters for identifying new timeline service
  // TODO consider to merge with connection retry
  private int maxServiceRetries;
  private long serviceRetryInterval;
  private boolean timelineServiceV2 = false;

  @Private
  @VisibleForTesting
  TimelineClientConnectionRetry connectionRetry;

  private TimelineEntityDispatcher entityDispatcher;

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
              && (e.getCause() instanceof ConnectException ||
                  e.getCause() instanceof SocketTimeoutException);
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
    super(TimelineClientImpl.class.getName(), null);
  }

  public TimelineClientImpl(ApplicationId applicationId) {
    super(TimelineClientImpl.class.getName(), applicationId);
    this.timelineServiceV2 = true;
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
    connConfigurator = initConnConfigurator(conf);
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
    // TODO need to cleanup filter retry later.
    if (!timelineServiceV2) {
      client.addFilter(retryFilter);
    }

    // old version timeline service need to get address from configuration
    // while new version need to auto discovery (with retry).
    if (timelineServiceV2) {
      maxServiceRetries = conf.getInt(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_MAX_RETRIES,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_MAX_RETRIES);
      serviceRetryInterval = conf.getLong(
          YarnConfiguration.TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_RETRY_INTERVAL_MS);
      entityDispatcher = new TimelineEntityDispatcher(conf);
    } else {
      if (YarnConfiguration.useHttps(conf)) {
        setTimelineServiceAddress(conf.get(
            YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS));
      } else {
        setTimelineServiceAddress(conf.get(
            YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS));
      }
      timelineServiceVersion =
          conf.getFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION,
              YarnConfiguration.DEFAULT_TIMELINE_SERVICE_VERSION);
      LOG.info("Timeline service address: " + getTimelineServiceAddress());
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if (timelineServiceV2) {
      entityDispatcher.start();
    } else {
      timelineWriter = createTimelineWriter(configuration, authUgi, client,
          constructResURI(getConfig(), timelineServiceAddress, false));
    }
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
    if (timelineServiceV2) {
      entityDispatcher.stop();
    }
    if (this.sslFactory != null) {
      this.sslFactory.destroy();
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
  public void putEntities(
      org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity...
          entities) throws IOException, YarnException {
    if (!timelineServiceV2) {
      throw new YarnException("v.2 method is invoked on a v.1.x client");
    }
    entityDispatcher.dispatchEntities(true, entities);
  }

  @Override
  public void putEntitiesAsync(
      org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity...
          entities) throws IOException, YarnException {
    if (!timelineServiceV2) {
      throw new YarnException("v.2 method is invoked on a v.1.x client");
    }
    entityDispatcher.dispatchEntities(false, entities);
  }

  @Override
  public void putDomain(TimelineDomain domain) throws IOException,
      YarnException {
    timelineWriter.putDomain(domain);
  }

  // Used for new timeline service only
  @Private
  protected void putObjects(String path, MultivaluedMap<String, String> params,
      Object obj) throws IOException, YarnException {

    int retries = verifyRestEndPointAvailable();

    // timelineServiceAddress could be stale, add retry logic here.
    boolean needRetry = true;
    while (needRetry) {
      try {
        URI uri = constructResURI(getConfig(), timelineServiceAddress, true);
        putObjects(uri, path, params, obj);
        needRetry = false;
      } catch (IOException e) {
        // handle exception for timelineServiceAddress being updated.
        checkRetryWithSleep(retries, e);
        retries--;
      }
    }
  }

  private int verifyRestEndPointAvailable() throws YarnException {
    // timelineServiceAddress could haven't be initialized yet
    // or stale (only for new timeline service)
    int retries = pollTimelineServiceAddress(this.maxServiceRetries);
    if (timelineServiceAddress == null) {
      String errMessage = "TimelineClient has reached to max retry times : "
          + this.maxServiceRetries
          + ", but failed to fetch timeline service address. Please verify"
          + " Timeline Auxillary Service is configured in all the NMs";
      LOG.error(errMessage);
      throw new YarnException(errMessage);
    }
    return retries;
  }

  /**
   * Check if reaching to maximum of retries.
   * @param retries
   * @param e
   */
  private void checkRetryWithSleep(int retries, IOException e)
      throws YarnException, IOException {
    if (retries > 0) {
      try {
        Thread.sleep(this.serviceRetryInterval);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new YarnException("Interrupted while retrying to connect to ATS");
      }
    } else {
      StringBuilder msg =
          new StringBuilder("TimelineClient has reached to max retry times : ");
      msg.append(this.maxServiceRetries);
      msg.append(" for service address: ");
      msg.append(timelineServiceAddress);
      LOG.error(msg.toString());
      throw new IOException(msg.toString(), e);
    }
  }

  protected void putObjects(
      URI base, String path, MultivaluedMap<String, String> params, Object obj)
          throws IOException, YarnException {
    ClientResponse resp;
    try {
      resp = client.resource(base).path(path).queryParams(params)
          .accept(MediaType.APPLICATION_JSON)
          .type(MediaType.APPLICATION_JSON)
          .put(ClientResponse.class, obj);
    } catch (RuntimeException re) {
      // runtime exception is expected if the client cannot connect the server
      String msg =
          "Failed to get the response from the timeline server.";
      LOG.error(msg, re);
      throw new IOException(re);
    }
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() !=
            ClientResponse.Status.OK.getStatusCode()) {
      String msg = "Response from the timeline server is " +
          ((resp == null) ? "null":
          "not successful," + " HTTP error code: " + resp.getStatus()
          + ", Server response:\n" + resp.getEntity(String.class));
      LOG.error(msg);
      throw new YarnException(msg);
    }
  }

  @Override
  public void setTimelineServiceAddress(String address) {
    this.timelineServiceAddress = address;
  }

  private String getTimelineServiceAddress() {
    return this.timelineServiceAddress;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Token<TimelineDelegationTokenIdentifier> getDelegationToken(
      final String renewer) throws IOException, YarnException {
    PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>
        getDTAction =
        new PrivilegedExceptionAction<Token<TimelineDelegationTokenIdentifier>>() {

          @Override
          public Token<TimelineDelegationTokenIdentifier> run()
              throws Exception {
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            // TODO we should add retry logic here if timelineServiceAddress is
            // not available immediately.
            return (Token) authUrl.getDelegationToken(
                constructResURI(getConfig(),
                    getTimelineServiceAddress(), false).toURL(),
                token, renewer, doAsUser);
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
            // Token to set every time for retry, because when exception
            // happens, DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty ?
                constructResURI(getConfig(), getTimelineServiceAddress(), false)
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR_V1, null, null);
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
            // If the timeline DT to cancel is different than cached, replace
            // it.
            // Token to set every time for retry, because when exception
            // happens, DelegationTokenAuthenticatedURL will reset it to null;
            if (!timelineDT.equals(token.getDelegationToken())) {
              token.setDelegationToken((Token) timelineDT);
            }
            DelegationTokenAuthenticatedURL authUrl =
                new DelegationTokenAuthenticatedURL(authenticator,
                    connConfigurator);
            // If the token service address is not available, fall back to use
            // the configured service address.
            final URI serviceURI = isTokenServiceAddrEmpty ?
                constructResURI(getConfig(), getTimelineServiceAddress(), false)
                : new URI(scheme, null, address.getHostName(),
                address.getPort(), RESOURCE_URI_STR_V1, null, null);
            authUrl.cancelDelegationToken(serviceURI.toURL(), token, doAsUser);
            return null;
          }
        };
    operateDelegationToken(cancelDTAction);
  }

  @Override
  public String toString() {
    return super.toString() + " with timeline server "
        + constructResURI(getConfig(), getTimelineServiceAddress(), false)
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

  /**
   * Poll TimelineServiceAddress for maximum of retries times if it is null.
   *
   * @param retries
   * @return the left retry times
   * @throws IOException
   */
  private int pollTimelineServiceAddress(int retries) throws YarnException {
    while (timelineServiceAddress == null && retries > 0) {
      try {
        Thread.sleep(this.serviceRetryInterval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new YarnException("Interrupted while trying to connect ATS");
      }
      retries--;
    }
    return retries;
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

  private ConnectionConfigurator initConnConfigurator(Configuration conf) {
    try {
      return initSslConnConfigurator(DEFAULT_SOCKET_TIMEOUT, conf);
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

  private ConnectionConfigurator initSslConnConfigurator(final int timeout,
      Configuration conf) throws IOException, GeneralSecurityException {
    final SSLSocketFactory sf;
    final HostnameVerifier hv;

    sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    sslFactory.init();
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

  private static void setTimeouts(URLConnection connection, int socketTimeout) {
    connection.setConnectTimeout(socketTimeout);
    connection.setReadTimeout(socketTimeout);
  }

  private static URI constructResURI(
      Configuration conf, String address, boolean v2) {
    return URI.create(
        JOINER.join(YarnConfiguration.useHttps(conf) ? "https://" : "http://",
            address, v2 ? RESOURCE_URI_STR_V2 : RESOURCE_URI_STR_V1));
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
    YarnJacksonJaxbJsonProvider.configObjectMapper(MAPPER);
    TimelineEntities entities = null;
    TimelineDomains domains = null;
    try {
      if (type.equals(ENTITY_DATA_TYPE)) {
        entities = MAPPER.readValue(jsonFile, TimelineEntities.class);
      } else if (type.equals(DOMAIN_DATA_TYPE)){
        domains = MAPPER.readValue(jsonFile, TimelineDomains.class);
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

  private final class EntitiesHolder extends FutureTask<Void> {
    private final
        org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities
            entities;
    private final boolean isSync;

    EntitiesHolder(
        final
            org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities
                entities,
        final boolean isSync) {
      super(new Callable<Void>() {
        // publishEntities()
        public Void call() throws Exception {
          MultivaluedMap<String, String> params = new MultivaluedMapImpl();
          params.add("appid", getContextAppId().toString());
          params.add("async", Boolean.toString(!isSync));
          putObjects("entities", params, entities);
          return null;
        }
      });
      this.entities = entities;
      this.isSync = isSync;
    }

    public boolean isSync() {
      return isSync;
    }

    public org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities
        getEntities() {
      return entities;
    }
  }

  /**
   * This class is responsible for collecting the timeline entities and
   * publishing them in async.
   */
  private class TimelineEntityDispatcher {
    /**
     * Time period for which the timelineclient will wait for draining after
     * stop.
     */
    private static final long DRAIN_TIME_PERIOD = 2000L;

    private int numberOfAsyncsToMerge;
    private final BlockingQueue<EntitiesHolder> timelineEntityQueue;
    private ExecutorService executor;

    TimelineEntityDispatcher(Configuration conf) {
      timelineEntityQueue = new LinkedBlockingQueue<EntitiesHolder>();
      numberOfAsyncsToMerge =
          conf.getInt(YarnConfiguration.NUMBER_OF_ASYNC_ENTITIES_TO_MERGE,
              YarnConfiguration.DEFAULT_NUMBER_OF_ASYNC_ENTITIES_TO_MERGE);
    }

    Runnable createRunnable() {
      return new Runnable() {
        @Override
        public void run() {
          try {
            EntitiesHolder entitiesHolder;
            while (!Thread.currentThread().isInterrupted()) {
              // Merge all the async calls and make one push, but if its sync
              // call push immediately
              try {
                entitiesHolder = timelineEntityQueue.take();
              } catch (InterruptedException ie) {
                LOG.info("Timeline dispatcher thread was interrupted ");
                Thread.currentThread().interrupt();
                return;
              }
              if (entitiesHolder != null) {
                publishWithoutBlockingOnQueue(entitiesHolder);
              }
            }
          } finally {
            if (!timelineEntityQueue.isEmpty()) {
              LOG.info("Yet to publish " + timelineEntityQueue.size()
                  + " timelineEntities, draining them now. ");
            }
            // Try to drain the remaining entities to be published @ the max for
            // 2 seconds
            long timeTillweDrain =
                System.currentTimeMillis() + DRAIN_TIME_PERIOD;
            while (!timelineEntityQueue.isEmpty()) {
              publishWithoutBlockingOnQueue(timelineEntityQueue.poll());
              if (System.currentTimeMillis() > timeTillweDrain) {
                // time elapsed stop publishing further....
                if (!timelineEntityQueue.isEmpty()) {
                  LOG.warn("Time to drain elapsed! Remaining "
                      + timelineEntityQueue.size() + "timelineEntities will not"
                      + " be published");
                  // if some entities were not drained then we need interrupt
                  // the threads which had put sync EntityHolders to the queue.
                  EntitiesHolder nextEntityInTheQueue = null;
                  while ((nextEntityInTheQueue =
                      timelineEntityQueue.poll()) != null) {
                    nextEntityInTheQueue.cancel(true);
                  }
                }
                break;
              }
            }
          }
        }

        /**
         * Publishes the given EntitiesHolder and return immediately if sync
         * call, else tries to fetch the EntitiesHolder from the queue in non
         * blocking fashion and collate the Entities if possible before
         * publishing through REST.
         *
         * @param entitiesHolder
         */
        private void publishWithoutBlockingOnQueue(
            EntitiesHolder entitiesHolder) {
          if (entitiesHolder.isSync()) {
            entitiesHolder.run();
            return;
          }
          int count = 1;
          while (true) {
            // loop till we find a sync put Entities or there is nothing
            // to take
            EntitiesHolder nextEntityInTheQueue = timelineEntityQueue.poll();
            if (nextEntityInTheQueue == null) {
              // Nothing in the queue just publish and get back to the
              // blocked wait state
              entitiesHolder.run();
              break;
            } else if (nextEntityInTheQueue.isSync()) {
              // flush all the prev async entities first
              entitiesHolder.run();
              // and then flush the sync entity
              nextEntityInTheQueue.run();
              break;
            } else {
              // append all async entities together and then flush
              entitiesHolder.getEntities().addEntities(
                  nextEntityInTheQueue.getEntities().getEntities());
              count++;
              if (count == numberOfAsyncsToMerge) {
                // Flush the entities if the number of the async
                // putEntites merged reaches the desired limit. To avoid
                // collecting multiple entities and delaying for a long
                // time.
                entitiesHolder.run();
                break;
              }
            }
          }
        }
      };
    }

    public void dispatchEntities(boolean sync,
        org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity[]
            entitiesTobePublished) throws YarnException {
      if (executor.isShutdown()) {
        throw new YarnException("Timeline client is in the process of stopping,"
            + " not accepting any more TimelineEntities");
      }

      // wrap all TimelineEntity into TimelineEntities object
      org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntities
          entities =
              new org.apache.hadoop.yarn.api.records.timelineservice.
                  TimelineEntities();
      for (org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
               entity : entitiesTobePublished) {
        entities.addEntity(entity);
      }

      // created a holder and place it in queue
      EntitiesHolder entitiesHolder = new EntitiesHolder(entities, sync);
      try {
        timelineEntityQueue.put(entitiesHolder);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new YarnException(
            "Failed while adding entity to the queue for publishing", e);
      }

      if (sync) {
        // In sync call we need to wait till its published and if any error then
        // throw it back
        try {
          entitiesHolder.get();
        } catch (ExecutionException e) {
          throw new YarnException("Failed while publishing entity",
              e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new YarnException("Interrupted while publishing entity", e);
        }
      }
    }

    public void start() {
      executor = Executors.newSingleThreadExecutor();
      executor.execute(createRunnable());
    }

    public void stop() {
      LOG.info("Stopping TimelineClient.");
      executor.shutdownNow();
      try {
        executor.awaitTermination(DRAIN_TIME_PERIOD, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    }
  }
}
