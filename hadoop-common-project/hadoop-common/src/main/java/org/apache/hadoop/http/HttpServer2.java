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
package org.apache.hadoop.http;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ConfServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.jmx.JMXJsonServlet;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.session.AbstractSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.FilterMapping;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.ServletMapping;
import org.eclipse.jetty.util.ArrayUtil;
import org.eclipse.jetty.util.MultiException;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory "/static/" -> points to common static
 * files (src/webapps/static) "/" -> the jsp server code from
 * (src/webapps/<name>)
 *
 * This class is a fork of the old HttpServer. HttpServer exists for
 * compatibility reasons. See HBASE-10336 for more details.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class HttpServer2 implements FilterContainer {
  public static final Logger LOG = LoggerFactory.getLogger(HttpServer2.class);

  public static final String HTTP_SCHEME = "http";
  public static final String HTTPS_SCHEME = "https";

  public static final String HTTP_MAX_REQUEST_HEADER_SIZE_KEY =
      "hadoop.http.max.request.header.size";
  public static final int HTTP_MAX_REQUEST_HEADER_SIZE_DEFAULT = 65536;
  public static final String HTTP_MAX_RESPONSE_HEADER_SIZE_KEY =
      "hadoop.http.max.response.header.size";
  public static final int HTTP_MAX_RESPONSE_HEADER_SIZE_DEFAULT = 65536;

  public static final String HTTP_SOCKET_BACKLOG_SIZE_KEY =
      "hadoop.http.socket.backlog.size";
  public static final int HTTP_SOCKET_BACKLOG_SIZE_DEFAULT = 128;
  public static final String HTTP_MAX_THREADS_KEY = "hadoop.http.max.threads";
  public static final String HTTP_ACCEPTOR_COUNT_KEY =
      "hadoop.http.acceptor.count";
  // -1 to use default behavior of setting count based on CPU core count
  public static final int HTTP_ACCEPTOR_COUNT_DEFAULT = -1;
  public static final String HTTP_SELECTOR_COUNT_KEY =
      "hadoop.http.selector.count";
  // -1 to use default behavior of setting count based on CPU core count
  public static final int HTTP_SELECTOR_COUNT_DEFAULT = -1;
  // idle timeout in milliseconds
  public static final String HTTP_IDLE_TIMEOUT_MS_KEY =
      "hadoop.http.idle_timeout.ms";
  public static final int HTTP_IDLE_TIMEOUT_MS_DEFAULT = 10000;
  public static final String HTTP_TEMP_DIR_KEY = "hadoop.http.temp.dir";

  public static final String FILTER_INITIALIZER_PROPERTY
      = "hadoop.http.filter.initializers";

  // The ServletContext attribute where the daemon Configuration
  // gets stored.
  public static final String CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";
  public static final String ADMINS_ACL = "admins.acl";
  public static final String SPNEGO_FILTER = "SpnegoFilter";
  public static final String NO_CACHE_FILTER = "NoCacheFilter";

  public static final String BIND_ADDRESS = "bind.address";

  private final AccessControlList adminsAcl;

  protected final Server webServer;

  private final HandlerCollection handlers;

  private final List<ServerConnector> listeners = Lists.newArrayList();

  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final IntegerRanges portRanges;
  private final Map<ServletContextHandler, Boolean> defaultContexts =
      new HashMap<>();
  protected final List<String> filterNames = new ArrayList<>();
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";
  private final SignerSecretProvider secretProvider;
  private XFrameOption xFrameOption;
  private boolean xFrameOptionIsEnabled;
  public static final String HTTP_HEADER_PREFIX = "hadoop.http.header.";
  private static final String HTTP_HEADER_REGEX =
          "hadoop\\.http\\.header\\.([a-zA-Z\\-_]+)";
  static final String X_XSS_PROTECTION  =
          "X-XSS-Protection:1; mode=block";
  static final String X_CONTENT_TYPE_OPTIONS =
          "X-Content-Type-Options:nosniff";
  private static final String X_FRAME_OPTIONS = "X-FRAME-OPTIONS";
  private static final Pattern PATTERN_HTTP_HEADER_REGEX =
          Pattern.compile(HTTP_HEADER_REGEX);
  /**
   * Class to construct instances of HTTP server with specific options.
   */
  public static class Builder {
    private ArrayList<URI> endpoints = Lists.newArrayList();
    private String name;
    private Configuration conf;
    private Configuration sslConf;
    private String[] pathSpecs;
    private AccessControlList adminsAcl;
    private boolean securityEnabled = false;
    private String usernameConfKey;
    private String keytabConfKey;
    private boolean needsClientAuth;
    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType;

    private String keyStore;
    private String keyStorePassword;
    private String keyStoreType;

    // The -keypass option in keytool
    private String keyPassword;

    private boolean findPort;
    private IntegerRanges portRanges = null;

    private String hostName;
    private boolean disallowFallbackToRandomSignerSecretProvider;
    private String authFilterConfigurationPrefix = "hadoop.http.authentication.";
    private String excludeCiphers;

    private boolean xFrameEnabled;
    private XFrameOption xFrameOption = XFrameOption.SAMEORIGIN;

    public Builder setName(String name){
      this.name = name;
      return this;
    }

    /**
     * Add an endpoint that the HTTP server should listen to.
     *
     * @param endpoint
     *          the endpoint of that the HTTP server should listen to. The
     *          scheme specifies the protocol (i.e. HTTP / HTTPS), the host
     *          specifies the binding address, and the port specifies the
     *          listening port. Unspecified or zero port means that the server
     *          can listen to any port.
     */
    public Builder addEndpoint(URI endpoint) {
      endpoints.add(endpoint);
      return this;
    }

    /**
     * Set the hostname of the http server. The host name is used to resolve the
     * _HOST field in Kerberos principals. The hostname of the first listener
     * will be used if the name is unspecified.
     */
    public Builder hostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    public Builder trustStore(String location, String password, String type) {
      this.trustStore = location;
      this.trustStorePassword = password;
      this.trustStoreType = type;
      return this;
    }

    public Builder keyStore(String location, String password, String type) {
      this.keyStore = location;
      this.keyStorePassword = password;
      this.keyStoreType = type;
      return this;
    }

    public Builder keyPassword(String password) {
      this.keyPassword = password;
      return this;
    }

    /**
     * Specify whether the server should authorize the client in SSL
     * connections.
     */
    public Builder needsClientAuth(boolean value) {
      this.needsClientAuth = value;
      return this;
    }

    public Builder setFindPort(boolean findPort) {
      this.findPort = findPort;
      return this;
    }

    public Builder setPortRanges(IntegerRanges ranges) {
      this.portRanges = ranges;
      return this;
    }

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    /**
     * Specify the SSL configuration to load. This API provides an alternative
     * to keyStore/keyPassword/trustStore.
     */
    public Builder setSSLConf(Configuration sslCnf) {
      this.sslConf = sslCnf;
      return this;
    }

    public Builder setPathSpec(String[] pathSpec) {
      this.pathSpecs = pathSpec;
      return this;
    }

    public Builder setACL(AccessControlList acl) {
      this.adminsAcl = acl;
      return this;
    }

    public Builder setSecurityEnabled(boolean securityEnabled) {
      this.securityEnabled = securityEnabled;
      return this;
    }

    public Builder setUsernameConfKey(String usernameConfKey) {
      this.usernameConfKey = usernameConfKey;
      return this;
    }

    public Builder setKeytabConfKey(String keytabConfKey) {
      this.keytabConfKey = keytabConfKey;
      return this;
    }

    public Builder disallowFallbackToRandomSingerSecretProvider(boolean value) {
      this.disallowFallbackToRandomSignerSecretProvider = value;
      return this;
    }

    public Builder authFilterConfigurationPrefix(String value) {
      this.authFilterConfigurationPrefix = value;
      return this;
    }

    public Builder excludeCiphers(String pExcludeCiphers) {
      this.excludeCiphers = pExcludeCiphers;
      return this;
    }

    /**
     * Adds the ability to control X_FRAME_OPTIONS on HttpServer2.
     * @param xFrameEnabled - True enables X_FRAME_OPTIONS false disables it.
     * @return Builder.
     */
    public Builder configureXFrame(boolean xFrameEnabled) {
      this.xFrameEnabled = xFrameEnabled;
      return this;
    }

    /**
     * Sets a valid X-Frame-option that can be used by HttpServer2.
     * @param option - String DENY, SAMEORIGIN or ALLOW-FROM are the only valid
     *               options. Any other value will throw IllegalArgument
     *               Exception.
     * @return  Builder.
     */
    public Builder setXFrameOption(String option) {
      this.xFrameOption = XFrameOption.getEnum(option);
      return this;
    }

    /**
     * A wrapper of {@link Configuration#getPassword(String)}. It returns
     * <code>String</code> instead of <code>char[]</code>.
     *
     * @param conf the configuration
     * @param name the property name
     * @return the password string or null
     */
    private static String getPasswordString(Configuration conf, String name)
        throws IOException {
      char[] passchars = conf.getPassword(name);
      if (passchars == null) {
        return null;
      }
      return new String(passchars);
    }

    /**
     * Load SSL properties from the SSL configuration.
     */
    private void loadSSLConfiguration() throws IOException {
      if (sslConf == null) {
        return;
      }
      needsClientAuth = sslConf.getBoolean(
          SSLFactory.SSL_SERVER_NEED_CLIENT_AUTH,
          SSLFactory.SSL_SERVER_NEED_CLIENT_AUTH_DEFAULT);
      keyStore = sslConf.getTrimmed(SSLFactory.SSL_SERVER_KEYSTORE_LOCATION);
      if (keyStore == null || keyStore.isEmpty()) {
        throw new IOException(String.format("Property %s not specified",
            SSLFactory.SSL_SERVER_KEYSTORE_LOCATION));
      }
      keyStorePassword = getPasswordString(sslConf,
          SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD);
      if (keyStorePassword == null) {
        throw new IOException(String.format("Property %s not specified",
            SSLFactory.SSL_SERVER_KEYSTORE_PASSWORD));
      }
      keyStoreType = sslConf.get(SSLFactory.SSL_SERVER_KEYSTORE_TYPE,
          SSLFactory.SSL_SERVER_KEYSTORE_TYPE_DEFAULT);
      keyPassword = getPasswordString(sslConf,
          SSLFactory.SSL_SERVER_KEYSTORE_KEYPASSWORD);
      trustStore = sslConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_LOCATION);
      trustStorePassword = getPasswordString(sslConf,
          SSLFactory.SSL_SERVER_TRUSTSTORE_PASSWORD);
      trustStoreType = sslConf.get(SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE,
          SSLFactory.SSL_SERVER_TRUSTSTORE_TYPE_DEFAULT);
      excludeCiphers = sslConf.get(SSLFactory.SSL_SERVER_EXCLUDE_CIPHER_LIST);
    }

    public HttpServer2 build() throws IOException {
      Preconditions.checkNotNull(name, "name is not set");
      Preconditions.checkState(!endpoints.isEmpty(), "No endpoints specified");

      if (hostName == null) {
        hostName = endpoints.get(0).getHost();
      }

      if (this.conf == null) {
        conf = new Configuration();
      }

      HttpServer2 server = new HttpServer2(this);

      if (this.securityEnabled) {
        server.initSpnego(conf, hostName, usernameConfKey, keytabConfKey);
      }

      for (URI ep : endpoints) {
        if (HTTPS_SCHEME.equals(ep.getScheme())) {
          loadSSLConfiguration();
          break;
        }
      }

      int requestHeaderSize = conf.getInt(
          HTTP_MAX_REQUEST_HEADER_SIZE_KEY,
          HTTP_MAX_REQUEST_HEADER_SIZE_DEFAULT);
      int responseHeaderSize = conf.getInt(
          HTTP_MAX_RESPONSE_HEADER_SIZE_KEY,
          HTTP_MAX_RESPONSE_HEADER_SIZE_DEFAULT);
      int idleTimeout = conf.getInt(HTTP_IDLE_TIMEOUT_MS_KEY,
          HTTP_IDLE_TIMEOUT_MS_DEFAULT);

      HttpConfiguration httpConfig = new HttpConfiguration();
      httpConfig.setRequestHeaderSize(requestHeaderSize);
      httpConfig.setResponseHeaderSize(responseHeaderSize);
      httpConfig.setSendServerVersion(false);

      int backlogSize = conf.getInt(HTTP_SOCKET_BACKLOG_SIZE_KEY,
          HTTP_SOCKET_BACKLOG_SIZE_DEFAULT);

      for (URI ep : endpoints) {
        final ServerConnector connector;
        String scheme = ep.getScheme();
        if (HTTP_SCHEME.equals(scheme)) {
          connector = createHttpChannelConnector(server.webServer,
              httpConfig);
        } else if (HTTPS_SCHEME.equals(scheme)) {
          connector = createHttpsChannelConnector(server.webServer,
              httpConfig);
        } else {
          throw new HadoopIllegalArgumentException(
              "unknown scheme for endpoint:" + ep);
        }
        connector.setHost(ep.getHost());
        connector.setPort(ep.getPort() == -1 ? 0 : ep.getPort());
        connector.setAcceptQueueSize(backlogSize);
        connector.setIdleTimeout(idleTimeout);
        server.addListener(connector);
      }
      server.loadListeners();
      return server;
    }

    private ServerConnector createHttpChannelConnector(
        Server server, HttpConfiguration httpConfig) {
      ServerConnector conn = new ServerConnector(server,
          conf.getInt(HTTP_ACCEPTOR_COUNT_KEY, HTTP_ACCEPTOR_COUNT_DEFAULT),
          conf.getInt(HTTP_SELECTOR_COUNT_KEY, HTTP_SELECTOR_COUNT_DEFAULT));
      ConnectionFactory connFactory = new HttpConnectionFactory(httpConfig);
      conn.addConnectionFactory(connFactory);
      if(Shell.WINDOWS) {
        // result of setting the SO_REUSEADDR flag is different on Windows
        // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
        // without this 2 NN's can start on the same machine and listen on
        // the same port with indeterminate routing of incoming requests to them
        conn.setReuseAddress(false);
      }
      return conn;
    }

    private ServerConnector createHttpsChannelConnector(
        Server server, HttpConfiguration httpConfig) {
      httpConfig.setSecureScheme(HTTPS_SCHEME);
      httpConfig.addCustomizer(new SecureRequestCustomizer());
      ServerConnector conn = createHttpChannelConnector(server, httpConfig);

      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setNeedClientAuth(needsClientAuth);
      sslContextFactory.setKeyManagerPassword(keyPassword);
      if (keyStore != null) {
        sslContextFactory.setKeyStorePath(keyStore);
        sslContextFactory.setKeyStoreType(keyStoreType);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
      }
      if (trustStore != null) {
        sslContextFactory.setTrustStorePath(trustStore);
        sslContextFactory.setTrustStoreType(trustStoreType);
        sslContextFactory.setTrustStorePassword(trustStorePassword);
      }
      if(null != excludeCiphers && !excludeCiphers.isEmpty()) {
        sslContextFactory.setExcludeCipherSuites(
            StringUtils.getTrimmedStrings(excludeCiphers));
        LOG.info("Excluded Cipher List:" + excludeCiphers);
      }

      conn.addFirstConnectionFactory(new SslConnectionFactory(sslContextFactory,
          HttpVersion.HTTP_1_1.asString()));

      return conn;
    }
  }

  private HttpServer2(final Builder b) throws IOException {
    final String appDir = getWebAppsPath(b.name);
    this.webServer = new Server();
    this.adminsAcl = b.adminsAcl;
    this.handlers = new HandlerCollection();
    this.webAppContext = createWebAppContext(b, adminsAcl, appDir);
    this.xFrameOptionIsEnabled = b.xFrameEnabled;
    this.xFrameOption = b.xFrameOption;

    try {
      this.secretProvider =
          constructSecretProvider(b, webAppContext.getServletContext());
      this.webAppContext.getServletContext().setAttribute
          (AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE,
           secretProvider);
    } catch(IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }

    this.findPort = b.findPort;
    this.portRanges = b.portRanges;
    initializeWebServer(b.name, b.hostName, b.conf, b.pathSpecs);
  }

  private void initializeWebServer(String name, String hostName,
      Configuration conf, String[] pathSpecs)
      throws IOException {

    Preconditions.checkNotNull(webAppContext);

    int maxThreads = conf.getInt(HTTP_MAX_THREADS_KEY, -1);
    // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
    // default value (currently 250).

    QueuedThreadPool threadPool = (QueuedThreadPool) webServer.getThreadPool();
    threadPool.setDaemon(true);
    if (maxThreads != -1) {
      threadPool.setMaxThreads(maxThreads);
    }

    SessionManager sm = webAppContext.getSessionHandler().getSessionManager();
    if (sm instanceof AbstractSessionManager) {
      AbstractSessionManager asm = (AbstractSessionManager)sm;
      asm.setHttpOnly(true);
      asm.getSessionCookieConfig().setSecure(true);
    }

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    RequestLog requestLog = HttpRequestLog.getRequestLog(name);

    handlers.addHandler(contexts);
    if (requestLog != null) {
      RequestLogHandler requestLogHandler = new RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      handlers.addHandler(requestLogHandler);
    }
    handlers.addHandler(webAppContext);
    final String appDir = getWebAppsPath(name);
    addDefaultApps(contexts, appDir, conf);
    webServer.setHandler(handlers);

    Map<String, String> xFrameParams = setHeaders(conf);
    addGlobalFilter("safety", QuotingInputFilter.class.getName(), xFrameParams);
    final FilterInitializer[] initializers = getFilterInitializers(conf);
    if (initializers != null) {
      conf = new Configuration(conf);
      conf.set(BIND_ADDRESS, hostName);
      for (FilterInitializer c : initializers) {
        c.initFilter(this, conf);
      }
    }

    addDefaultServlets();

    if (pathSpecs != null) {
      for (String path : pathSpecs) {
        LOG.info("adding path spec: " + path);
        addFilterPathMapping(path, webAppContext);
      }
    }
  }

  private void addListener(ServerConnector connector) {
    listeners.add(connector);
  }

  private static WebAppContext createWebAppContext(Builder b,
      AccessControlList adminsAcl, final String appDir) {
    WebAppContext ctx = new WebAppContext();
    ctx.setDefaultsDescriptor(null);
    ServletHolder holder = new ServletHolder(new DefaultServlet());
    Map<String, String> params = ImmutableMap. <String, String> builder()
            .put("acceptRanges", "true")
            .put("dirAllowed", "false")
            .put("gzip", "true")
            .put("useFileMappedBuffer", "true")
            .build();
    holder.setInitParameters(params);
    ctx.setWelcomeFiles(new String[] {"index.html"});
    ctx.addServlet(holder, "/");
    ctx.setDisplayName(b.name);
    ctx.setContextPath("/");
    ctx.setWar(appDir + "/" + b.name);
    String tempDirectory = b.conf.get(HTTP_TEMP_DIR_KEY);
    if (tempDirectory != null && !tempDirectory.isEmpty()) {
      ctx.setTempDirectory(new File(tempDirectory));
      ctx.setAttribute("javax.servlet.context.tempdir", tempDirectory);
    }
    ctx.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, b.conf);
    ctx.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
    addNoCacheFilter(ctx);
    return ctx;
  }

  private static SignerSecretProvider constructSecretProvider(final Builder b,
      ServletContext ctx)
      throws Exception {
    final Configuration conf = b.conf;
    Properties config = getFilterProperties(conf,
                                            b.authFilterConfigurationPrefix);
    return AuthenticationFilter.constructSecretProvider(
        ctx, config, b.disallowFallbackToRandomSignerSecretProvider);
  }

  private static Properties getFilterProperties(Configuration conf, String
      prefix) {
    Properties prop = new Properties();
    Map<String, String> filterConfig = AuthenticationFilterInitializer
        .getFilterConfigMap(conf, prefix);
    prop.putAll(filterConfig);
    return prop;
  }

  private static void addNoCacheFilter(ServletContextHandler ctxt) {
    defineFilter(ctxt, NO_CACHE_FILTER, NoCacheFilter.class.getName(),
                 Collections.<String, String> emptyMap(), new String[] { "/*" });
  }

  /** Get an array of FilterConfiguration specified in the conf */
  private static FilterInitializer[] getFilterInitializers(Configuration conf) {
    if (conf == null) {
      return null;
    }

    Class<?>[] classes = conf.getClasses(FILTER_INITIALIZER_PROPERTY);
    if (classes == null) {
      return null;
    }

    FilterInitializer[] initializers = new FilterInitializer[classes.length];
    for(int i = 0; i < classes.length; i++) {
      initializers[i] = (FilterInitializer)ReflectionUtils.newInstance(
          classes[i], conf);
    }
    return initializers;
  }

  /**
   * Add default apps.
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(ContextHandlerCollection parent,
      final String appDir, Configuration conf) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined
    // and it's enabled.
    String logDir = System.getProperty("hadoop.log.dir");
    boolean logsEnabled = conf.getBoolean(
        CommonConfigurationKeys.HADOOP_HTTP_LOGS_ENABLED,
        CommonConfigurationKeys.HADOOP_HTTP_LOGS_ENABLED_DEFAULT);
    if (logDir != null && logsEnabled) {
      ServletContextHandler logContext =
          new ServletContextHandler(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      if (conf.getBoolean(
          CommonConfigurationKeys.HADOOP_JETTY_LOGS_SERVE_ALIASES,
          CommonConfigurationKeys.DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES)) {
        @SuppressWarnings("unchecked")
        Map<String, String> params = logContext.getInitParams();
        params.put("org.eclipse.jetty.servlet.Default.aliases", "true");
      }
      logContext.setDisplayName("logs");
      SessionHandler handler = new SessionHandler();
      SessionManager sm = handler.getSessionManager();
      if (sm instanceof AbstractSessionManager) {
        AbstractSessionManager asm = (AbstractSessionManager) sm;
        asm.setHttpOnly(true);
        asm.getSessionCookieConfig().setSecure(true);
      }
      logContext.setSessionHandler(handler);
      setContextAttributes(logContext, conf);
      addNoCacheFilter(logContext);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    ServletContextHandler staticContext =
        new ServletContextHandler(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    @SuppressWarnings("unchecked")
    Map<String, String> params = staticContext.getInitParams();
    params.put("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
    params.put("org.eclipse.jetty.servlet.Default.gzip", "true");
    SessionHandler handler = new SessionHandler();
    SessionManager sm = handler.getSessionManager();
    if (sm instanceof AbstractSessionManager) {
      AbstractSessionManager asm = (AbstractSessionManager) sm;
      asm.setHttpOnly(true);
      asm.getSessionCookieConfig().setSecure(true);
    }
    staticContext.setSessionHandler(handler);
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }

  private void setContextAttributes(ServletContextHandler context,
                                    Configuration conf) {
    context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    context.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
  }

  /**
   * Add default servlets.
   */
  protected void addDefaultServlets() {
    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
    addServlet("jmx", "/jmx", JMXJsonServlet.class);
    addServlet("conf", "/conf", ConfServlet.class);
  }

  public void addContext(ServletContextHandler ctxt, boolean isFiltered) {
    handlers.addHandler(ctxt);
    addNoCacheFilter(ctxt);
    defaultContexts.put(ctxt, isFiltered);
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Add a Jersey resource package.
   * @param packageName The Java package name containing the Jersey resource.
   * @param pathSpec The path spec for the servlet
   */
  public void addJerseyResourcePackage(final String packageName,
      final String pathSpec) {
    LOG.info("addJerseyResourcePackage: packageName=" + packageName
        + ", pathSpec=" + pathSpec);
    final ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
        "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages", packageName);
    webAppContext.addServlet(sh, pathSpec);
  }

  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Add an internal servlet in the server.
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters are not enabled.
   *
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addInternalServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
  }

  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication.
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled.
   *
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @param requireAuth Require Kerberos authenticate to access servlet
   */
  public void addInternalServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz, boolean requireAuth) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    // Jetty doesn't like the same path spec mapping to different servlets, so
    // if there's already a mapping for this pathSpec, remove it and assume that
    // the newest one is the one we want
    final ServletMapping[] servletMappings =
        webAppContext.getServletHandler().getServletMappings();
    for (int i = 0; i < servletMappings.length; i++) {
      if (servletMappings[i].containsPathSpec(pathSpec)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found existing " + servletMappings[i].getServletName() +
              " servlet at path " + pathSpec + "; will replace mapping" +
              " with " + holder.getName() + " servlet");
        }
        ServletMapping[] newServletMappings =
            ArrayUtil.removeFromArray(servletMappings, servletMappings[i]);
        webAppContext.getServletHandler()
            .setServletMappings(newServletMappings);
        break;
      }
    }
    webAppContext.addServlet(holder, pathSpec);

    if(requireAuth && UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Adding Kerberos (SPNEGO) filter to " + name);
      ServletHandler handler = webAppContext.getServletHandler();
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(SPNEGO_FILTER);
      fmap.setDispatches(FilterMapping.ALL);
      handler.addFilterMapping(fmap);
    }
  }

  /**
   * Add an internal servlet in the server, with initialization parameters.
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled.
   *
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @param params init parameters
   */
  public void addInternalServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz, Map<String, String> params) {
    // Jetty doesn't like the same path spec mapping to different servlets, so
    // if there's already a mapping for this pathSpec, remove it and assume that
    // the newest one is the one we want
    final ServletHolder sh = new ServletHolder(clazz);
    sh.setName(name);
    sh.setInitParameters(params);
    final ServletMapping[] servletMappings =
        webAppContext.getServletHandler().getServletMappings();
    for (int i = 0; i < servletMappings.length; i++) {
      if (servletMappings[i].containsPathSpec(pathSpec)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found existing " + servletMappings[i].getServletName() +
              " servlet at path " + pathSpec + "; will replace mapping" +
              " with " + sh.getName() + " servlet");
        }
        ServletMapping[] newServletMappings =
            ArrayUtil.removeFromArray(servletMappings, servletMappings[i]);
        webAppContext.getServletHandler()
            .setServletMappings(newServletMappings);
        break;
      }
    }
    webAppContext.addServlet(sh, pathSpec);
  }

  /**
   * Add the given handler to the front of the list of handlers.
   *
   * @param handler The handler to add
   */
  public void addHandlerAtFront(Handler handler) {
    Handler[] h = ArrayUtil.prependToArray(
        handler, this.handlers.getHandlers(), Handler.class);
    handlers.setHandlers(h);
  }

  /**
   * Add the given handler to the end of the list of handlers.
   *
   * @param handler The handler to add
   */
  public void addHandlerAtEnd(Handler handler) {
    handlers.addHandler(handler);
  }

  @Override
  public void addFilter(String name, String classname,
      Map<String, String> parameters) {

    FilterHolder filterHolder = getFilterHolder(name, classname, parameters);
    final String[] USER_FACING_URLS = { "*.html", "*.jsp" };
    FilterMapping fmap = getFilterMapping(name, USER_FACING_URLS);
    defineFilter(webAppContext, filterHolder, fmap);
    LOG.info(
        "Added filter " + name + " (class=" + classname + ") to context "
            + webAppContext.getDisplayName());
    final String[] ALL_URLS = { "/*" };
    fmap = getFilterMapping(name, ALL_URLS);
    for (Map.Entry<ServletContextHandler, Boolean> e
        : defaultContexts.entrySet()) {
      if (e.getValue()) {
        ServletContextHandler ctx = e.getKey();
        defineFilter(ctx, filterHolder, fmap);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + ctx.getDisplayName());
      }
    }
    filterNames.add(name);
  }

  @Override
  public void addGlobalFilter(String name, String classname,
      Map<String, String> parameters) {
    final String[] ALL_URLS = { "/*" };
    FilterHolder filterHolder = getFilterHolder(name, classname, parameters);
    FilterMapping fmap = getFilterMapping(name, ALL_URLS);
    defineFilter(webAppContext, filterHolder, fmap);
    for (ServletContextHandler ctx : defaultContexts.keySet()) {
      defineFilter(ctx, filterHolder, fmap);
    }
    LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  public static void defineFilter(ServletContextHandler ctx, String name,
      String classname, Map<String,String> parameters, String[] urls) {
    FilterHolder filterHolder = getFilterHolder(name, classname, parameters);
    FilterMapping fmap = getFilterMapping(name, urls);
    defineFilter(ctx, filterHolder, fmap);
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  private static void defineFilter(ServletContextHandler ctx,
                                   FilterHolder holder, FilterMapping fmap) {
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  private static FilterMapping getFilterMapping(String name, String[] urls) {
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(FilterMapping.ALL);
    fmap.setFilterName(name);
    return fmap;
  }

  private static FilterHolder getFilterHolder(String name, String classname,
      Map<String, String> parameters) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    return holder;
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(String pathSpec,
      ServletContextHandler webAppCtx) {
    ServletHandler handler = webAppCtx.getServletHandler();
    for(String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(FilterMapping.ALL);
      handler.addFilterMapping(fmap);
    }
  }

  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  public WebAppContext getWebAppContext(){
    return this.webAppContext;
  }

  /**
   * Get the pathname to the webapps files.
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found
   *   on CLASSPATH or in the development location.
   */
  protected String getWebAppsPath(String appName) throws FileNotFoundException {
    URL resourceUrl = null;
    File webResourceDevLocation = new File("src/main/webapps", appName);
    if (webResourceDevLocation.exists()) {
      LOG.info("Web server is in development mode. Resources "
          + "will be read from the source tree.");
      try {
        resourceUrl = webResourceDevLocation.getParentFile().toURI().toURL();
      } catch (MalformedURLException e) {
        throw new FileNotFoundException("Mailformed URL while finding the "
            + "web resource dir:" + e.getMessage());
      }
    } else {
      resourceUrl =
          getClass().getClassLoader().getResource("webapps/" + appName);

      if (resourceUrl == null) {
        throw new FileNotFoundException("webapps/" + appName +
            " not found in CLASSPATH");
      }
    }
    String urlString = resourceUrl.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * @return the port
   */
  @Deprecated
  public int getPort() {
    return ((ServerConnector)webServer.getConnectors()[0]).getLocalPort();
  }

  /**
   * Get the address that corresponds to a particular connector.
   *
   * @return the corresponding address for the connector, or null if there's no
   *         such connector or the connector is not bounded or was closed.
   */
  public InetSocketAddress getConnectorAddress(int index) {
    Preconditions.checkArgument(index >= 0);
    if (index > webServer.getConnectors().length)
      return null;

    ServerConnector c = (ServerConnector)webServer.getConnectors()[index];
    if (c.getLocalPort() == -1 || c.getLocalPort() == -2) {
      // The connector is not bounded or was closed
      return null;
    }

    return new InetSocketAddress(c.getHost(), c.getLocalPort());
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(int min, int max) {
    QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  private void initSpnego(Configuration conf, String hostName,
      String usernameConfKey, String keytabConfKey) throws IOException {
    Map<String, String> params = new HashMap<>();
    String principalInConf = conf.get(usernameConfKey);
    if (principalInConf != null && !principalInConf.isEmpty()) {
      params.put("kerberos.principal", SecurityUtil.getServerPrincipal(
          principalInConf, hostName));
    }
    String httpKeytab = conf.get(keytabConfKey);
    if (httpKeytab != null && !httpKeytab.isEmpty()) {
      params.put("kerberos.keytab", httpKeytab);
    }
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
    defineFilter(webAppContext, SPNEGO_FILTER,
                 AuthenticationFilter.class.getName(), params, null);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      try {
        openListeners();
        webServer.start();
      } catch (IOException ex) {
        LOG.info("HttpServer.start() threw a non Bind IOException", ex);
        throw ex;
      } catch (MultiException ex) {
        LOG.info("HttpServer.start() threw a MultiException", ex);
        throw ex;
      }
      // Make sure there is no handler failures.
      Handler[] hs = webServer.getHandlers();
      for (Handler handler : hs) {
        if (handler.isFailed()) {
          throw new IOException(
              "Problem in starting http server. Server handlers failed");
        }
      }
      // Make sure there are no errors initializing the context.
      Throwable unavailableException = webAppContext.getUnavailableException();
      if (unavailableException != null) {
        // Have to stop the webserver, or else its non-daemon threads
        // will hang forever.
        webServer.stop();
        throw new IOException("Unable to initialize WebAppContext",
            unavailableException);
      }
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException(
          "Interrupted while starting HTTP server").initCause(e);
    } catch (Exception e) {
      throw new IOException("Problem starting http server", e);
    }
  }

  private void loadListeners() {
    for (Connector c : listeners) {
      webServer.addConnector(c);
    }
  }

  /**
   * Bind listener by closing and opening the listener.
   * @param listener
   * @throws Exception
   */
  private static void bindListener(ServerConnector listener) throws Exception {
    // jetty has a bug where you can't reopen a listener that previously
    // failed to open w/o issuing a close first, even if the port is changed
    listener.close();
    listener.open();
    LOG.info("Jetty bound to port " + listener.getLocalPort());
  }

  /**
   * Create bind exception by wrapping the bind exception thrown.
   * @param listener
   * @param ex
   * @return
   */
  private static BindException constructBindException(ServerConnector listener,
      BindException ex) {
    BindException be = new BindException("Port in use: "
        + listener.getHost() + ":" + listener.getPort());
    if (ex != null) {
      be.initCause(ex);
    }
    return be;
  }

  /**
   * Bind using single configured port. If findPort is true, we will try to bind
   * after incrementing port till a free port is found.
   * @param listener jetty listener.
   * @param port port which is set in the listener.
   * @throws Exception
   */
  private void bindForSinglePort(ServerConnector listener, int port)
      throws Exception {
    while (true) {
      try {
        bindListener(listener);
        break;
      } catch (BindException ex) {
        if (port == 0 || !findPort) {
          throw constructBindException(listener, ex);
        }
      }
      // try the next port number
      listener.setPort(++port);
      Thread.sleep(100);
    }
  }

  /**
   * Bind using port ranges. Keep on looking for a free port in the port range
   * and throw a bind exception if no port in the configured range binds.
   * @param listener jetty listener.
   * @param startPort initial port which is set in the listener.
   * @throws Exception
   */
  private void bindForPortRange(ServerConnector listener, int startPort)
      throws Exception {
    BindException bindException = null;
    try {
      bindListener(listener);
      return;
    } catch (BindException ex) {
      // Ignore exception.
      bindException = ex;
    }
    for(Integer port : portRanges) {
      if (port == startPort) {
        continue;
      }
      Thread.sleep(100);
      listener.setPort(port);
      try {
        bindListener(listener);
        return;
      } catch (BindException ex) {
        // Ignore exception. Move to next port.
        bindException = ex;
      }
    }
    throw constructBindException(listener, bindException);
  }

  /**
   * Open the main listener for the server
   * @throws Exception
   */
  void openListeners() throws Exception {
    LOG.debug("opening listeners: {}", listeners);
    for (ServerConnector listener : listeners) {
      if (listener.getLocalPort() != -1 && listener.getLocalPort() != -2) {
        // This listener is either started externally or has been bound or was
        // closed
        continue;
      }
      int port = listener.getPort();
      if (portRanges != null && port != 0) {
        bindForPortRange(listener, port);
      } else {
        bindForSinglePort(listener, port);
      }
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    MultiException exception = null;
    for (ServerConnector c : listeners) {
      try {
        c.close();
      } catch (Exception e) {
        LOG.error(
            "Error while stopping listener for webapp"
                + webAppContext.getDisplayName(), e);
        exception = addMultiException(exception, e);
      }
    }

    try {
      // explicitly destroy the secret provider
      secretProvider.destroy();
      // clear & stop webAppContext attributes to avoid memory leaks.
      webAppContext.clearAttributes();
      webAppContext.stop();
    } catch (Exception e) {
      LOG.error("Error while stopping web app context for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error("Error while stopping web server for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    if (exception != null) {
      exception.ifExceptionThrow();
    }

  }

  private MultiException addMultiException(MultiException exception, Exception e) {
    if(exception == null){
      exception = new MultiException();
    }
    exception.add(e);
    return exception;
  }

  public void join() throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * @return true if the web server is started, false otherwise
   */
  public boolean isAlive() {
    return webServer != null && webServer.isStarted();
  }

  @Override
  public String toString() {
    Preconditions.checkState(!listeners.isEmpty());
    StringBuilder sb = new StringBuilder("HttpServer (")
        .append(isAlive() ? STATE_DESCRIPTION_ALIVE
                    : STATE_DESCRIPTION_NOT_LIVE)
        .append("), listening at:");
    for (ServerConnector l : listeners) {
      sb.append(l.getHost()).append(":").append(l.getPort()).append("/,");
    }
    return sb.toString();
  }

  /**
   * Checks the user has privileges to access to instrumentation servlets.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
   * (default value) it always returns TRUE.
   * <p/>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
   * it will check that if the current user is in the admin ACLS. If the user is
   * in the admin ACLs it returns TRUE, otherwise it returns FALSE.
   *
   * @param servletContext the servlet context.
   * @param request the servlet request.
   * @param response the servlet response.
   * @return TRUE/FALSE based on the logic decribed above.
   */
  public static boolean isInstrumentationAccessAllowed(
    ServletContext servletContext, HttpServletRequest request,
    HttpServletResponse response) throws IOException {
    Configuration conf =
      (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    boolean access = true;
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
      false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    }
    return access;
  }

  /**
   * Does the user sending the HttpServletRequest has the administrator ACLs? If
   * it isn't the case, response will be modified to send an error to the user.
   *
   * @param response used to send the error response if user does not have admin access.
   * @return true if admin-authorized, false otherwise
   * @throws IOException
   */
  public static boolean hasAdministratorAccess(
      ServletContext servletContext, HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Configuration conf =
        (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    // If there is no authorization, anybody has administrator access.
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      return true;
    }

    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
                         "Unauthenticated users are not " +
                         "authorized to access this page.");
      return false;
    }

    if (servletContext.getAttribute(ADMINS_ACL) != null &&
        !userHasAdministratorAccess(servletContext, remoteUser)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Unauthenticated users are not " +
              "authorized to access this page.");
      LOG.warn("User " + remoteUser + " is unauthorized to access the page "
          + request.getRequestURI() + ".");
      return false;
    }

    return true;
  }

  /**
   * Get the admin ACLs from the given ServletContext and check if the given
   * user is in the ACL.
   *
   * @param servletContext the context containing the admin ACL.
   * @param remoteUser the remote user to check for.
   * @return true if the user is present in the ACL, false if no ACL is set or
   *         the user is not present
   */
  public static boolean userHasAdministratorAccess(ServletContext servletContext,
      String remoteUser) {
    AccessControlList adminsAcl = (AccessControlList) servletContext
        .getAttribute(ADMINS_ACL);
    UserGroupInformation remoteUserUGI =
        UserGroupInformation.createRemoteUser(remoteUser);
    return adminsAcl != null && adminsAcl.isUserAllowed(remoteUserUGI);
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
      if (!HttpServer2.isInstrumentationAccessAllowed(getServletContext(),
                                                      request, response)) {
        return;
      }
      response.setContentType("text/plain; charset=UTF-8");
      try (PrintStream out = new PrintStream(
          response.getOutputStream(), false, "UTF-8")) {
        ReflectionUtils.printThreadInfo(out, "");
      }
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }
  }

  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks. It also
   * sets X-FRAME-OPTIONS in the header to mitigate clickjacking attacks.
   */
  public static class QuotingInputFilter implements Filter {

    private FilterConfig config;
    private Map<String, String> headerMap;

    public static class RequestQuoter extends HttpServletRequestWrapper {
      private final HttpServletRequest rawRequest;

      public RequestQuoter(HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }

      /**
       * Return the set of parameter names, quoting each name.
       */
      @SuppressWarnings("unchecked")
      @Override
      public Enumeration<String> getParameterNames() {
        return new Enumeration<String>() {
          private Enumeration<String> rawIterator =
            rawRequest.getParameterNames();
          @Override
          public boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }

      /**
       * Unquote the name and quote the value.
       */
      @Override
      public String getParameter(String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter
                                     (HtmlQuoting.unquoteHtmlChars(name)));
      }

      @Override
      public String[] getParameterValues(String name) {
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        String[] unquoteValue = rawRequest.getParameterValues(unquoteName);
        if (unquoteValue == null) {
          return null;
        }
        String[] result = new String[unquoteValue.length];
        for(int i=0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Map<String, String[]> getParameterMap() {
        Map<String, String[]> result = new HashMap<>();
        Map<String, String[]> raw = rawRequest.getParameterMap();
        for (Map.Entry<String,String[]> item: raw.entrySet()) {
          String[] rawValue = item.getValue();
          String[] cookedValue = new String[rawValue.length];
          for(int i=0; i< rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }

      /**
       * Quote the url so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public StringBuffer getRequestURL(){
        String url = rawRequest.getRequestURL().toString();
        return new StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }

      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public String getServerName() {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
      this.config = config;
      initHttpHeaderMap();
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain
                         ) throws IOException, ServletException {
      HttpServletRequestWrapper quoted =
        new RequestQuoter((HttpServletRequest) request);
      HttpServletResponse httpResponse = (HttpServletResponse) response;

      String mime = inferMimeType(request);
      if (mime == null) {
        httpResponse.setContentType("text/plain; charset=utf-8");
      } else if (mime.startsWith("text/html")) {
        // HTML with unspecified encoding, we want to
        // force HTML with utf-8 encoding
        // This is to avoid the following security issue:
        // http://openmya.hacker.jp/hasegawa/security/utf7cs.html
        httpResponse.setContentType("text/html; charset=utf-8");
      } else if (mime.startsWith("application/xml")) {
        httpResponse.setContentType("text/xml; charset=utf-8");
      }
      headerMap.forEach((k, v) -> httpResponse.addHeader(k, v));
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private String inferMimeType(ServletRequest request) {
      String path = ((HttpServletRequest)request).getRequestURI();
      ServletContextHandler.Context sContext =
          (ServletContextHandler.Context)config.getServletContext();
      String mime = sContext.getMimeType(path);
      return (mime == null) ? null : mime;
    }

    private void initHttpHeaderMap() {
      Enumeration<String> params = this.config.getInitParameterNames();
      headerMap = new HashMap<>();
      while (params.hasMoreElements()) {
        String key = params.nextElement();
        Matcher m = PATTERN_HTTP_HEADER_REGEX.matcher(key);
        if (m.matches()) {
          String headerKey = m.group(1);
          headerMap.put(headerKey, config.getInitParameter(key));
        }
      }
    }
  }
    /**
     * The X-FRAME-OPTIONS header in HTTP response to mitigate clickjacking
     * attack.
     */
  public enum XFrameOption {
    DENY("DENY"), SAMEORIGIN("SAMEORIGIN"), ALLOWFROM("ALLOW-FROM");

    XFrameOption(String name) {
      this.name = name;
    }

    private final String name;

    @Override
    public String toString() {
      return this.name;
    }

    /**
     * We cannot use valueOf since the AllowFrom enum differs from its value
     * Allow-From. This is a helper method that does exactly what valueof does,
     * but allows us to handle the AllowFrom issue gracefully.
     *
     * @param value - String must be DENY, SAMEORIGIN or ALLOW-FROM.
     * @return XFrameOption or throws IllegalException.
     */
    private static XFrameOption getEnum(String value) {
      Preconditions.checkState(value != null && !value.isEmpty());
      for (XFrameOption xoption : values()) {
        if (value.equals(xoption.toString())) {
          return xoption;
        }
      }
      throw new IllegalArgumentException("Unexpected value in xFrameOption.");
    }
  }


  private Map<String, String> setHeaders(Configuration conf) {
    Map<String, String> xFrameParams = new HashMap<>();
    Map<String, String> headerConfigMap =
            conf.getValByRegex(HTTP_HEADER_REGEX);

    xFrameParams.putAll(getDefaultHeaders());
    if(this.xFrameOptionIsEnabled) {
      xFrameParams.put(HTTP_HEADER_PREFIX+X_FRAME_OPTIONS,
              this.xFrameOption.toString());
    }
    xFrameParams.putAll(headerConfigMap);
    return xFrameParams;
  }

  private Map<String, String> getDefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    String[] splitVal = X_CONTENT_TYPE_OPTIONS.split(":");
    headers.put(HTTP_HEADER_PREFIX + splitVal[0],
            splitVal[1]);
    splitVal = X_XSS_PROTECTION.split(":");
    headers.put(HTTP_HEADER_PREFIX + splitVal[0],
            splitVal[1]);
    return headers;
  }
}
