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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ConfServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.jmx.JMXJsonServlet;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.metrics.MetricsServlet;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.mortbay.io.Buffer;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.MimeTypes;
import org.mortbay.jetty.RequestLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionManager;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.AbstractSessionManager;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.jersey.spi.container.servlet.ServletContainer;

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
  public static final Log LOG = LogFactory.getLog(HttpServer2.class);

  static final String FILTER_INITIALIZER_PROPERTY
      = "hadoop.http.filter.initializers";
  static final String HTTP_MAX_THREADS = "hadoop.http.max.threads";

  // The ServletContext attribute where the daemon Configuration
  // gets stored.
  public static final String CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";
  public static final String ADMINS_ACL = "admins.acl";
  public static final String SPNEGO_FILTER = "SpnegoFilter";
  public static final String NO_CACHE_FILTER = "NoCacheFilter";

  public static final String BIND_ADDRESS = "bind.address";

  private final AccessControlList adminsAcl;

  protected final Server webServer;

  private static class ListenerInfo {
    /**
     * Boolean flag to determine whether the HTTP server should clean up the
     * listener in stop().
     */
    private final boolean isManaged;
    private final Connector listener;
    private ListenerInfo(boolean isManaged, Connector listener) {
      this.isManaged = isManaged;
      this.listener = listener;
    }
  }

  private final List<ListenerInfo> listeners = Lists.newArrayList();

  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final Map<Context, Boolean> defaultContexts =
      new HashMap<Context, Boolean>();
  protected final List<String> filterNames = new ArrayList<String>();
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  /**
   * Class to construct instances of HTTP server with specific options.
   */
  public static class Builder {
    private ArrayList<URI> endpoints = Lists.newArrayList();
    private Connector connector;
    private String name;
    private Configuration conf;
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

    private String hostName;

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

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setConnector(Connector connector) {
      this.connector = connector;
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

    public HttpServer2 build() throws IOException {
      if (this.name == null) {
        throw new HadoopIllegalArgumentException("name is not set");
      }

      if (endpoints.size() == 0 && connector == null) {
        throw new HadoopIllegalArgumentException("No endpoints specified");
      }

      if (hostName == null) {
        hostName = endpoints.size() == 0 ? connector.getHost() : endpoints.get(
            0).getHost();
      }

      if (this.conf == null) {
        conf = new Configuration();
      }

      HttpServer2 server = new HttpServer2(this);

      if (this.securityEnabled) {
        server.initSpnego(conf, hostName, usernameConfKey, keytabConfKey);
      }

      if (connector != null) {
        server.addUnmanagedListener(connector);
      }

      for (URI ep : endpoints) {
        Connector listener = null;
        String scheme = ep.getScheme();
        if ("http".equals(scheme)) {
          listener = HttpServer2.createDefaultChannelConnector();
        } else if ("https".equals(scheme)) {
          SslSocketConnector c = new SslSocketConnector();
          c.setNeedClientAuth(needsClientAuth);
          c.setKeyPassword(keyPassword);

          if (keyStore != null) {
            c.setKeystore(keyStore);
            c.setKeystoreType(keyStoreType);
            c.setPassword(keyStorePassword);
          }

          if (trustStore != null) {
            c.setTruststore(trustStore);
            c.setTruststoreType(trustStoreType);
            c.setTrustPassword(trustStorePassword);
          }
          listener = c;

        } else {
          throw new HadoopIllegalArgumentException(
              "unknown scheme for endpoint:" + ep);
        }
        listener.setHost(ep.getHost());
        listener.setPort(ep.getPort() == -1 ? 0 : ep.getPort());
        server.addManagedListener(listener);
      }
      server.loadListeners();
      return server;
    }
  }

  private HttpServer2(final Builder b) throws IOException {
    final String appDir = getWebAppsPath(b.name);
    this.webServer = new Server();
    this.adminsAcl = b.adminsAcl;
    this.webAppContext = createWebAppContext(b.name, b.conf, adminsAcl, appDir);
    this.findPort = b.findPort;
    initializeWebServer(b.name, b.hostName, b.conf, b.pathSpecs);
  }

  private void initializeWebServer(String name, String hostName,
      Configuration conf, String[] pathSpecs)
      throws FileNotFoundException, IOException {

    Preconditions.checkNotNull(webAppContext);

    int maxThreads = conf.getInt(HTTP_MAX_THREADS, -1);
    // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
    // default value (currently 250).
    QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool()
        : new QueuedThreadPool(maxThreads);
    threadPool.setDaemon(true);
    webServer.setThreadPool(threadPool);

    SessionManager sm = webAppContext.getSessionHandler().getSessionManager();
    if (sm instanceof AbstractSessionManager) {
      AbstractSessionManager asm = (AbstractSessionManager)sm;
      asm.setHttpOnly(true);
      asm.setSecureCookies(true);
    }

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    RequestLog requestLog = HttpRequestLog.getRequestLog(name);

    if (requestLog != null) {
      RequestLogHandler requestLogHandler = new RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      HandlerCollection handlers = new HandlerCollection();
      handlers.setHandlers(new Handler[] {contexts, requestLogHandler});
      webServer.setHandler(handlers);
    } else {
      webServer.setHandler(contexts);
    }

    final String appDir = getWebAppsPath(name);

    webServer.addHandler(webAppContext);

    addDefaultApps(contexts, appDir, conf);

    addGlobalFilter("safety", QuotingInputFilter.class.getName(), null);
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

  private void addUnmanagedListener(Connector connector) {
    listeners.add(new ListenerInfo(false, connector));
  }

  private void addManagedListener(Connector connector) {
    listeners.add(new ListenerInfo(true, connector));
  }

  private static WebAppContext createWebAppContext(String name,
      Configuration conf, AccessControlList adminsAcl, final String appDir) {
    WebAppContext ctx = new WebAppContext();
    ctx.setDisplayName(name);
    ctx.setContextPath("/");
    ctx.setWar(appDir + "/" + name);
    ctx.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    ctx.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
    addNoCacheFilter(ctx);
    return ctx;
  }

  private static void addNoCacheFilter(WebAppContext ctxt) {
    defineFilter(ctxt, NO_CACHE_FILTER, NoCacheFilter.class.getName(),
        Collections.<String, String> emptyMap(), new String[] { "/*" });
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  public Connector createBaseListener(Configuration conf) {
    return HttpServer2.createDefaultChannelConnector();
  }

  @InterfaceAudience.Private
  public static Connector createDefaultChannelConnector() {
    SelectChannelConnector ret = new SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    if(Shell.WINDOWS) {
      // result of setting the SO_REUSEADDR flag is different on Windows
      // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
      // without this 2 NN's can start on the same machine and listen on
      // the same port with indeterminate routing of incoming requests to them
      ret.setReuseAddress(false);
    }
    ret.setHeaderBufferSize(1024*64);
    return ret;
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
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined.
    String logDir = System.getProperty("hadoop.log.dir");
    if (logDir != null) {
      Context logContext = new Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      if (conf.getBoolean(
          CommonConfigurationKeys.HADOOP_JETTY_LOGS_SERVE_ALIASES,
          CommonConfigurationKeys.DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES)) {
        @SuppressWarnings("unchecked")
        Map<String, String> params = logContext.getInitParams();
        params.put(
            "org.mortbay.jetty.servlet.Default.aliases", "true");
      }
      logContext.setDisplayName("logs");
      setContextAttributes(logContext, conf);
      addNoCacheFilter(webAppContext);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    Context staticContext = new Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }

  private void setContextAttributes(Context context, Configuration conf) {
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
    addServlet("metrics", "/metrics", MetricsServlet.class);
    addServlet("jmx", "/jmx", JMXJsonServlet.class);
    addServlet("conf", "/conf", ConfServlet.class);
  }

  public void addContext(Context ctxt, boolean isFiltered) {
    webServer.addHandler(ctxt);
    addNoCacheFilter(webAppContext);
    defaultContexts.put(ctxt, isFiltered);
  }

  /**
   * Add a context
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping
   * @throws IOException
   */
  protected void addContext(String pathSpec, String dir, boolean isFiltered) throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new RuntimeException("Couldn't find handler");
    }
    WebAppContext webAppCtx = new WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, true);
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
   +   * servlets added using this method, filters (except internal Kerberos
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
    webAppContext.addServlet(holder, pathSpec);

    if(requireAuth && UserGroupInformation.isSecurityEnabled()) {
       LOG.info("Adding Kerberos (SPNEGO) filter to " + name);
       ServletHandler handler = webAppContext.getServletHandler();
       FilterMapping fmap = new FilterMapping();
       fmap.setPathSpec(pathSpec);
       fmap.setFilterName(SPNEGO_FILTER);
       fmap.setDispatches(Handler.ALL);
       handler.addFilterMapping(fmap);
    }
  }

  @Override
  public void addFilter(String name, String classname,
      Map<String, String> parameters) {

    final String[] USER_FACING_URLS = { "*.html", "*.jsp" };
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
    LOG.info("Added filter " + name + " (class=" + classname
        + ") to context " + webAppContext.getDisplayName());
    final String[] ALL_URLS = { "/*" };
    for (Map.Entry<Context, Boolean> e : defaultContexts.entrySet()) {
      if (e.getValue()) {
        Context ctx = e.getKey();
        defineFilter(ctx, name, classname, parameters, ALL_URLS);
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
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    for (Context ctx : defaultContexts.keySet()) {
      defineFilter(ctx, name, classname, parameters, ALL_URLS);
    }
    LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  public static void defineFilter(Context ctx, String name,
      String classname, Map<String,String> parameters, String[] urls) {

    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(String pathSpec,
      Context webAppCtx) {
    ServletHandler handler = webAppCtx.getServletHandler();
    for(String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
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
   * @throws FileNotFoundException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected String getWebAppsPath(String appName) throws FileNotFoundException {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null)
      throw new FileNotFoundException("webapps/" + appName
          + " not found in CLASSPATH");
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * @return the port
   */
  @Deprecated
  public int getPort() {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Get the address that corresponds to a particular connector.
   *
   * @return the corresponding address for the connector, or null if there's no
   *         such connector or the connector is not bounded.
   */
  public InetSocketAddress getConnectorAddress(int index) {
    Preconditions.checkArgument(index >= 0);
    if (index > webServer.getConnectors().length)
      return null;

    Connector c = webServer.getConnectors()[index];
    if (c.getLocalPort() == -1) {
      // The connector is not bounded
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
    Map<String, String> params = new HashMap<String, String>();
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
      Handler[] handlers = webServer.getHandlers();
      for (int i = 0; i < handlers.length; i++) {
        if (handlers[i].isFailed()) {
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
    for (ListenerInfo li : listeners) {
      webServer.addConnector(li.listener);
    }
  }

  /**
   * Open the main listener for the server
   * @throws Exception
   */
  void openListeners() throws Exception {
    for (ListenerInfo li : listeners) {
      Connector listener = li.listener;
      if (!li.isManaged || li.listener.getLocalPort() != -1) {
        // This listener is either started externally or has been bound
        continue;
      }
      int port = listener.getPort();
      while (true) {
        // jetty has a bug where you can't reopen a listener that previously
        // failed to open w/o issuing a close first, even if the port is changed
        try {
          listener.close();
          listener.open();
          LOG.info("Jetty bound to port " + listener.getLocalPort());
          break;
        } catch (BindException ex) {
          if (port == 0 || !findPort) {
            BindException be = new BindException("Port in use: "
                + listener.getHost() + ":" + listener.getPort());
            be.initCause(ex);
            throw be;
          }
        }
        // try the next port number
        listener.setPort(++port);
        Thread.sleep(100);
      }
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    MultiException exception = null;
    for (ListenerInfo li : listeners) {
      if (!li.isManaged) {
        continue;
      }

      try {
        li.listener.close();
      } catch (Exception e) {
        LOG.error(
            "Error while stopping listener for webapp"
                + webAppContext.getDisplayName(), e);
        exception = addMultiException(exception, e);
      }
    }

    try {
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

  /**
   * Return the host and port of the HttpServer, if live
   * @return the classname and any HTTP URL
   */
  @Override
  public String toString() {
    if (listeners.size() == 0) {
      return "Inactive HttpServer";
    } else {
      StringBuilder sb = new StringBuilder("HttpServer (")
        .append(isAlive() ? STATE_DESCRIPTION_ALIVE : STATE_DESCRIPTION_NOT_LIVE).append("), listening at:");
      for (ListenerInfo li : listeners) {
        Connector l = li.listener;
        sb.append(l.getHost()).append(":").append(l.getPort()).append("/,");
      }
      return sb.toString();
    }
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
   * @param servletContext
   * @param request
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
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "User "
          + remoteUser + " is unauthorized to access this page.");
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
      PrintWriter out = response.getWriter();
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }
  }

  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks.
   */
  public static class QuotingInputFilter implements Filter {
    private FilterConfig config;

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
        Map<String, String[]> result = new HashMap<String,String[]>();
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
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private String inferMimeType(ServletRequest request) {
      String path = ((HttpServletRequest)request).getRequestURI();
      ContextHandler.SContext sContext = (ContextHandler.SContext)config.getServletContext();
      MimeTypes mimes = sContext.getContextHandler().getMimeTypes();
      Buffer mimeBuffer = mimes.getMimeByExtension(path);
      return (mimeBuffer == null) ? null : mimeBuffer.toString();
    }

  }
}
