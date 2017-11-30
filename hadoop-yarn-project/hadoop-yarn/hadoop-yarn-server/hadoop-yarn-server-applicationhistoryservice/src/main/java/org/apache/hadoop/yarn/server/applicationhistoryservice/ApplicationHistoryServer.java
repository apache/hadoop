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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.timeline.LeveldbTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineDataManager;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.timeline.security.TimelineACLsManager;
import org.apache.hadoop.yarn.server.timeline.security.TimelineV1DelegationTokenSecretManagerService;
import org.apache.hadoop.yarn.server.timeline.webapp.CrossOriginFilterInitializer;
import org.apache.hadoop.yarn.server.util.timeline.TimelineServerUtils;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * History server that keeps track of all types of history in the cluster.
 * Application specific history to start with.
 */
public class ApplicationHistoryServer extends CompositeService {

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationHistoryServer.class);

  private ApplicationHistoryClientService ahsClientService;
  private ApplicationACLsManager aclsManager;
  private ApplicationHistoryManager historyManager;
  private TimelineStore timelineStore;
  private TimelineV1DelegationTokenSecretManagerService secretManagerService;
  private TimelineDataManager timelineDataManager;
  private WebApp webApp;
  private JvmPauseMonitor pauseMonitor;

  public ApplicationHistoryServer() {
    super(ApplicationHistoryServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // do security login first.
    try {
      doSecureLogin(conf);
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }
    // init timeline services
    timelineStore = createTimelineStore(conf);
    addIfService(timelineStore);
    secretManagerService = createTimelineDelegationTokenSecretManagerService(conf);
    addService(secretManagerService);
    timelineDataManager = createTimelineDataManager(conf);
    addService(timelineDataManager);

    // init generic history service afterwards
    aclsManager = createApplicationACLsManager(conf);
    historyManager = createApplicationHistoryManager(conf);
    ahsClientService = createApplicationHistoryClientService(historyManager);
    addService(ahsClientService);
    addService((Service) historyManager);

    DefaultMetricsSystem.initialize("ApplicationHistoryServer");
    JvmMetrics jm = JvmMetrics.initSingleton("ApplicationHistoryServer", null);
    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    jm.setPauseMonitor(pauseMonitor);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    startWebApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    DefaultMetricsSystem.shutdown();
    super.serviceStop();
  }

  @Private
  @VisibleForTesting
  ApplicationHistoryClientService getClientService() {
    return this.ahsClientService;
  }

  private InetSocketAddress getListenerAddress() {
    return this.webApp.httpServer().getConnectorAddress(0);
  }

  @Private
  @VisibleForTesting
  public int getPort() {
    return this.getListenerAddress().getPort();
  }

  /**
   * @return ApplicationTimelineStore
   */
  @Private
  @VisibleForTesting
  public TimelineStore getTimelineStore() {
    return timelineStore;
  }

  @Private
  @VisibleForTesting
  ApplicationHistoryManager getApplicationHistoryManager() {
    return this.historyManager;
  }

  static ApplicationHistoryServer launchAppHistoryServer(String[] args) {
    Thread
      .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ApplicationHistoryServer.class, args,
      LOG);
    ApplicationHistoryServer appHistoryServer = null;
    try {
      appHistoryServer = new ApplicationHistoryServer();
      ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(appHistoryServer),
        SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration();
      new GenericOptionsParser(conf, args);
      appHistoryServer.init(conf);
      appHistoryServer.start();
    } catch (Throwable t) {
      LOG.error("Error starting ApplicationHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting ApplicationHistoryServer");
    }
    return appHistoryServer;
  }

  public static void main(String[] args) {
    launchAppHistoryServer(args);
  }

  private ApplicationHistoryClientService
      createApplicationHistoryClientService(
          ApplicationHistoryManager historyManager) {
    return new ApplicationHistoryClientService(historyManager);
  }

  private ApplicationACLsManager createApplicationACLsManager(
      Configuration conf) {
    return new ApplicationACLsManager(conf);
  }

  private ApplicationHistoryManager createApplicationHistoryManager(
      Configuration conf) {
    // Backward compatibility:
    // APPLICATION_HISTORY_STORE is neither null nor empty, it means that the
    // user has enabled it explicitly.
    if (conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE) == null ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).length() == 0 ||
        conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).equals(
            NullApplicationHistoryStore.class.getName())) {
      return new ApplicationHistoryManagerOnTimelineStore(
          timelineDataManager, aclsManager);
    } else {
      LOG.warn("The filesystem based application history store is deprecated.");
      return new ApplicationHistoryManagerImpl();
    }
  }

  private TimelineStore createTimelineStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
        YarnConfiguration.TIMELINE_SERVICE_STORE, LeveldbTimelineStore.class,
        TimelineStore.class), conf);
  }

  private TimelineV1DelegationTokenSecretManagerService
      createTimelineDelegationTokenSecretManagerService(Configuration conf) {
    return new TimelineV1DelegationTokenSecretManagerService();
  }

  private TimelineDataManager createTimelineDataManager(Configuration conf) {
    TimelineACLsManager aclsMgr = new TimelineACLsManager(conf);
    aclsMgr.setTimelineStore(timelineStore);
    return new TimelineDataManager(timelineStore, aclsMgr);
  }

  @SuppressWarnings("unchecked")
  private void startWebApp() {
    Configuration conf = getConfig();
    // Always load pseudo authentication filter to parse "user.name" in an URL
    // to identify a HTTP request's user in insecure mode.
    // When Kerberos authentication type is set (i.e., secure mode is turned on),
    // the customized filter will be loaded by the timeline server to do Kerberos
    // + DT authentication.
    String initializers = conf.get("hadoop.http.filter.initializers", "");
    Set<String> defaultInitializers = new LinkedHashSet<String>();
    // Add CORS filter
    if (!initializers.contains(CrossOriginFilterInitializer.class.getName())) {
      if(conf.getBoolean(YarnConfiguration.
          TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED,
          YarnConfiguration.
          TIMELINE_SERVICE_HTTP_CROSS_ORIGIN_ENABLED_DEFAULT)) {
        if (initializers.contains(
            HttpCrossOriginFilterInitializer.class.getName())) {
          initializers = initializers.replaceAll(
              HttpCrossOriginFilterInitializer.class.getName(),
              CrossOriginFilterInitializer.class.getName());
        } else {
          defaultInitializers.add(CrossOriginFilterInitializer.class.getName());
        }
      }
    }
    TimelineServerUtils.addTimelineAuthFilter(
        initializers, defaultInitializers, secretManagerService);
    TimelineServerUtils.setTimelineFilters(
        conf, initializers, defaultInitializers);
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
                          YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
                          WebAppUtils.getAHSWebAppURLWithoutScheme(conf));
    try {
      AHSWebApp ahsWebApp =
          new AHSWebApp(timelineDataManager, ahsClientService);
      webApp =
          WebApps
            .$for("applicationhistory", ApplicationHistoryClientService.class,
                ahsClientService, "ws")
             .with(conf)
              .withAttribute(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
                 conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS))
              .withCSRFProtection(YarnConfiguration.TIMELINE_CSRF_PREFIX)
              .withXFSProtection(YarnConfiguration.TIMELINE_XFS_PREFIX)
              .at(bindAddress).build(ahsWebApp);
       HttpServer2 httpServer = webApp.httpServer();

       String[] names = conf.getTrimmedStrings(
           YarnConfiguration.TIMELINE_SERVICE_UI_NAMES);
       WebAppContext webAppContext = httpServer.getWebAppContext();

      for (String name : names) {
        String webPath = conf.get(
            YarnConfiguration.TIMELINE_SERVICE_UI_WEB_PATH_PREFIX + name);
        String onDiskPath = conf.get(
            YarnConfiguration.TIMELINE_SERVICE_UI_ON_DISK_PATH_PREFIX + name);
        WebAppContext uiWebAppContext = new WebAppContext();
        uiWebAppContext.setContextPath(webPath);
        if (onDiskPath.endsWith(".war")) {
          uiWebAppContext.setWar(onDiskPath);
        } else {
          uiWebAppContext.setResourceBase(onDiskPath);
        }
        final String[] ALL_URLS = {"/*"};
        FilterHolder[] filterHolders =
            webAppContext.getServletHandler().getFilters();
        for (FilterHolder filterHolder : filterHolders) {
          if (!"guice".equals(filterHolder.getName())) {
            HttpServer2.defineFilter(uiWebAppContext, filterHolder.getName(),
                filterHolder.getClassName(), filterHolder.getInitParameters(),
                ALL_URLS);
          }
        }
        LOG.info("Hosting " + name + " from " + onDiskPath + " at " + webPath);
        httpServer.addHandlerAtFront(uiWebAppContext);
      }
       httpServer.start();
       conf.updateConnectAddr(YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
        YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS,
        this.getListenerAddress());
       LOG.info("Instantiating AHSWebApp at " + getPort());
    } catch (Exception e) {
      String msg = "AHSWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  private void doSecureLogin(Configuration conf) throws IOException {
    InetSocketAddress socAddr = getBindAddress(conf);
    SecurityUtil.login(conf, YarnConfiguration.TIMELINE_SERVICE_KEYTAB,
        YarnConfiguration.TIMELINE_SERVICE_PRINCIPAL, socAddr.getHostName());
  }

  /**
   * Retrieve the timeline server bind address from configuration
   *
   * @param conf
   * @return InetSocketAddress
   */
  private static InetSocketAddress getBindAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ADDRESS,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_PORT);
  }
}
