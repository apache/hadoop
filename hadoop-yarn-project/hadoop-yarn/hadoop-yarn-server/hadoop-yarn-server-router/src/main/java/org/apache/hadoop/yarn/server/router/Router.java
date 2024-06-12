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

package org.apache.hadoop.yarn.server.router;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebAppUtil;
import org.apache.hadoop.yarn.server.router.cleaner.SubClusterCleaner;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.rmadmin.RouterRMAdminService;
import org.apache.hadoop.yarn.server.router.webapp.RouterWebApp;
import org.apache.hadoop.yarn.server.webproxy.FedAppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.hadoop.yarn.webapp.util.WebServiceClient;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_DEREGISTER_SUBCLUSTER_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_DEREGISTER_SUBCLUSTER_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_SCHEDULED_EXECUTOR_THREADS;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_ROUTER_SCHEDULED_EXECUTOR_THREADS;

/**
 * The router is a stateless YARN component which is the entry point to the
 * cluster. It can be deployed on multiple nodes behind a Virtual IP (VIP) with
 * a LoadBalancer.
 *
 * The Router exposes the ApplicationClientProtocol (RPC and REST) to the
 * outside world, transparently hiding the presence of ResourceManager(s), which
 * allows users to request and update reservations, submit and kill
 * applications, and request status on running applications.
 *
 * In addition, it exposes the ResourceManager Admin API.
 *
 * This provides a placeholder for throttling mis-behaving clients (YARN-1546)
 * and masks the access to multiple RMs (YARN-3659).
 */
public class Router extends CompositeService {

  private static final Logger LOG = LoggerFactory.getLogger(Router.class);
  private static CompositeServiceShutdownHook routerShutdownHook;
  private Configuration conf;
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private JvmPauseMonitor pauseMonitor;
  @VisibleForTesting
  protected RouterClientRMService clientRMProxyService;
  @VisibleForTesting
  protected RouterRMAdminService rmAdminProxyService;
  private WebApp webApp;
  @VisibleForTesting
  protected String webAppAddress;
  private static long clusterTimeStamp = System.currentTimeMillis();
  private FedAppReportFetcher fetcher = null;
  private static final String CMD_FORMAT_STATE_STORE = "-format-state-store";
  private static final String CMD_REMOVE_APPLICATION_FROM_STATE_STORE =
      "-remove-application-from-state-store";

  /**
   * Priority of the Router shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final String METRICS_NAME = "Router";

  private static final String UI2_WEBAPP_NAME = "/ui2";

  private ScheduledThreadPoolExecutor scheduledExecutorService;
  private SubClusterCleaner subClusterCleaner;

  public Router() {
    super(Router.class.getName());
  }

  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(this.conf, YarnConfiguration.ROUTER_KEYTAB,
        YarnConfiguration.ROUTER_PRINCIPAL, getHostName(this.conf));
  }

  @Override
  protected void serviceInit(Configuration config) throws Exception {
    this.conf = config;
    UserGroupInformation.setConfiguration(this.conf);
    // ClientRM Proxy
    clientRMProxyService = createClientRMProxyService();
    addService(clientRMProxyService);
    // RMAdmin Proxy
    rmAdminProxyService = createRMAdminProxyService();
    addService(rmAdminProxyService);
    // WebService
    webAppAddress = WebAppUtils.getWebAppBindURL(this.conf,
        YarnConfiguration.ROUTER_BIND_HOST,
        WebAppUtils.getRouterWebAppURLWithoutScheme(this.conf));
    // Metrics
    DefaultMetricsSystem.initialize(METRICS_NAME);
    JvmMetrics jm = JvmMetrics.initSingleton("Router", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);

    // Initialize subClusterCleaner
    this.subClusterCleaner = new SubClusterCleaner(this.conf);
    int scheduledExecutorThreads = conf.getInt(ROUTER_SCHEDULED_EXECUTOR_THREADS,
        DEFAULT_ROUTER_SCHEDULED_EXECUTOR_THREADS);
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(scheduledExecutorThreads);

    WebServiceClient.initialize(config);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed Router login", e);
    }
    boolean isDeregisterSubClusterEnabled = this.conf.getBoolean(
        ROUTER_DEREGISTER_SUBCLUSTER_ENABLED, DEFAULT_ROUTER_DEREGISTER_SUBCLUSTER_ENABLED);
    if (isDeregisterSubClusterEnabled) {
      long scCleanerIntervalMs = this.conf.getTimeDuration(ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME,
          DEFAULT_ROUTER_SUBCLUSTER_CLEANER_INTERVAL_TIME, TimeUnit.MILLISECONDS);
      this.scheduledExecutorService.scheduleAtFixedRate(this.subClusterCleaner,
          0, scCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled SubClusterCleaner With Interval: {}.",
          DurationFormatUtils.formatDurationISO(scCleanerIntervalMs));
    }
    startWepApp();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (isStopping.getAndSet(true)) {
      return;
    }
    super.serviceStop();
    DefaultMetricsSystem.shutdown();
    WebServiceClient.destroy();
  }

  protected void shutDown() {
    new Thread(Router.this::stop).start();
  }

  protected RouterClientRMService createClientRMProxyService() {
    return new RouterClientRMService();
  }

  protected RouterRMAdminService createRMAdminProxyService() {
    return new RouterRMAdminService();
  }

  @Private
  public WebApp getWebapp() {
    return this.webApp;
  }

  @VisibleForTesting
  public void startWepApp() {

    // Initialize RouterWeb's CrossOrigin capability.
    boolean enableCors = conf.getBoolean(YarnConfiguration.ROUTER_WEBAPP_ENABLE_CORS_FILTER,
        YarnConfiguration.DEFAULT_ROUTER_WEBAPP_ENABLE_CORS_FILTER);
    if (enableCors) {
      conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
          + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
    }

    LOG.info("Instantiating RouterWebApp at {}.", webAppAddress);

    RMWebAppUtil.setupSecurityAndFilters(conf, null);

    Builder<Object> builder =
        WebApps.$for("cluster", null, null, "ws").with(conf).at(webAppAddress);
    if (RouterServerUtil.isRouterWebProxyEnable(conf)) {
      fetcher = new FedAppReportFetcher(conf);
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME, ProxyUriUtils.PROXY_PATH_SPEC,
          WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String proxyHostAndPort = getProxyHostAndPort(conf);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
    }
    webApp = builder.start(new RouterWebApp(this), getUIWebAppContext());
  }

  private WebAppContext getUIWebAppContext() {
    WebAppContext uiWebAppContext = null;
    boolean isWebUI2Enabled = conf.getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
        YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE);

    if(isWebUI2Enabled) {
      String onDiskPath = conf.get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);
      uiWebAppContext = new WebAppContext();
      uiWebAppContext.setContextPath(UI2_WEBAPP_NAME);

      if (null == onDiskPath) {
        String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
        URL url = getClass().getClassLoader().getResource(war);
        if (null == url) {
          onDiskPath = getWebAppsPath("ui2");
        } else {
          onDiskPath = url.getFile();
        }
      }

      if (onDiskPath == null || onDiskPath.isEmpty()) {
        LOG.error("No war file or webapps found for yarn federation!");
      } else {
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
          LOG.info("Using war file at: {}.", onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
          LOG.info("Using webapps at: {}.", onDiskPath);
        }
      }
    }
    return uiWebAppContext;
  }

  private String getWebAppsPath(String appName) {
    URL url = getClass().getClassLoader().getResource("webapps/" + appName);
    if (url == null) {
      return "";
    }
    return url.toString();
  }

  public static String getProxyHostAndPort(Configuration conf) {
    String addr = conf.get(YarnConfiguration.PROXY_ADDRESS);
    if(addr == null || addr.isEmpty()) {
      InetSocketAddress address = conf.getSocketAddr(YarnConfiguration.ROUTER_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_PORT);
      addr = WebAppUtils.getResolvedAddress(address);
    }
    return addr;
  }

  public static void main(String[] argv) {
    Configuration conf = new YarnConfiguration();
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(Router.class, argv, LOG);
    Router router = new Router();
    try {
      GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
      argv = hParser.getRemainingArgs();
      if (argv.length > 1) {
        executeRouterCommand(conf, argv);
      } else {
        // Remove the old hook if we are rebooting.
        if (null != routerShutdownHook) {
          ShutdownHookManager.get().removeShutdownHook(routerShutdownHook);
        }
        routerShutdownHook = new CompositeServiceShutdownHook(router);
        ShutdownHookManager.get().addShutdownHook(routerShutdownHook, SHUTDOWN_HOOK_PRIORITY);
        router.init(conf);
        router.start();
      }
    } catch (Throwable t) {
      LOG.error("Error starting Router", t);
      System.exit(-1);
    }
  }

  @VisibleForTesting
  public RouterClientRMService getClientRMProxyService() {
    return clientRMProxyService;
  }

  @VisibleForTesting
  public RouterRMAdminService getRmAdminProxyService() {
    return rmAdminProxyService;
  }

  /**
   * Returns the hostname for this Router. If the hostname is not
   * explicitly configured in the given config, then it is determined.
   *
   * @param config configuration
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the hostname cannot be determined
   */
  private String getHostName(Configuration config)
      throws UnknownHostException {
    String name = config.get(YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_HOSTNAME_KEY);
    if (name == null) {
      name = InetAddress.getLocalHost().getHostName();
    }
    return name;
  }

  public static long getClusterTimeStamp() {
    return clusterTimeStamp;
  }

  @VisibleForTesting
  public FedAppReportFetcher getFetcher() {
    return fetcher;
  }

  @VisibleForTesting
  public static void removeApplication(Configuration conf, String applicationId)
      throws Exception {
    FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(conf);
    ApplicationId removeAppId = ApplicationId.fromString(applicationId);
    LOG.info("Deleting application {} from state store.", removeAppId);
    facade.deleteApplicationHomeSubCluster(removeAppId);
    LOG.info("Application is deleted from state store");
  }

  private static void handFormatStateStore(Configuration conf) {
    try {
      System.out.println("Deleting Federation state store.");
      FederationStateStoreFacade facade = FederationStateStoreFacade.getInstance(conf);
      System.out.println("Federation state store has been cleaned.");
      facade.deleteStore();
    } catch (Exception e) {
      System.err.println("Delete Federation state store error, exception = " + e);
    }
  }

  private static void handRemoveApplicationFromStateStore(Configuration conf,
      String applicationId) {
    try {
      removeApplication(conf, applicationId);
      System.out.println("Application " + applicationId + " is deleted from state store");
    } catch (Exception e) {
      System.err.println("Application " + applicationId + " error, exception = " + e);
    }
  }

  private static void executeRouterCommand(Configuration conf, String[] args) {
    // Step1. Define Options.
    Options opts = new Options();
    Option formatStateStoreOpt = new Option("format-state-store",  false,
        " Formats the FederationStateStore. " +
        "This will clear the FederationStateStore and " +
        "is useful if past applications are no longer needed. " +
        "This should be run only when the Router is not running.");
    Option removeApplicationFromStateStoreOpt = new Option("remove-application-from-state-store",
        false, " Remove the application from FederationStateStore. " +
         " This should be run only when the Router is not running. ");
    opts.addOption(formatStateStoreOpt);
    opts.addOption(removeApplicationFromStateStoreOpt);

    // Step2. Parse Options.
    try {
      String cmd = args[0];

      CommandLine cliParser = new DefaultParser().parse(opts, args);

      if (CMD_FORMAT_STATE_STORE.equals(cmd)) {
        handFormatStateStore(conf);
      } else if (CMD_REMOVE_APPLICATION_FROM_STATE_STORE.equals(cmd)) {
        if (cliParser.hasOption(removeApplicationFromStateStoreOpt)) {
          String applicationId = cliParser.getOptionValue(removeApplicationFromStateStoreOpt);
          handRemoveApplicationFromStateStore(conf, applicationId);
        } else {
          System.err.println("remove-application-from-state-store requires application arg.");
        }
      } else {
        System.out.println("No related commands found.");
        printUsage(System.err);
      }
    } catch (MissingArgumentException ex) {
      System.out.println("Missing argument for options.");
      printUsage(System.err);
    } catch (ParseException e) {
      System.out.println("Parsing of a command-line error.");
      printUsage(System.err);
    }
  }

  private static void printUsage(PrintStream out) {
    out.println("Usage: yarn router [-format-state-store] | " +
        "[-remove-application-from-state-store <appId>]");
  }
}
