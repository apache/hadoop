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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.TimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.timeline.LeveldbTimelineStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * History server that keeps track of all types of history in the cluster.
 * Application specific history to start with.
 */
public class ApplicationHistoryServer extends CompositeService {

  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  private static final Log LOG = LogFactory
    .getLog(ApplicationHistoryServer.class);

  ApplicationHistoryClientService ahsClientService;
  ApplicationHistoryManager historyManager;
  TimelineStore timelineStore;
  private WebApp webApp;

  public ApplicationHistoryServer() {
    super(ApplicationHistoryServer.class.getName());
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    historyManager = createApplicationHistory();
    ahsClientService = createApplicationHistoryClientService(historyManager);
    addService(ahsClientService);
    addService((Service) historyManager);
    timelineStore = createTimelineStore(conf);
    addIfService(timelineStore);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    DefaultMetricsSystem.initialize("ApplicationHistoryServer");
    JvmMetrics.initSingleton("ApplicationHistoryServer", null);

    startWebApp();
    super.serviceStart();
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
  public ApplicationHistoryClientService getClientService() {
    return this.ahsClientService;
  }

  protected ApplicationHistoryClientService
      createApplicationHistoryClientService(
          ApplicationHistoryManager historyManager) {
    return new ApplicationHistoryClientService(historyManager);
  }

  protected ApplicationHistoryManager createApplicationHistory() {
    return new ApplicationHistoryManagerImpl();
  }

  protected ApplicationHistoryManager getApplicationHistory() {
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
      appHistoryServer.init(conf);
      appHistoryServer.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting ApplicationHistoryServer", t);
      ExitUtil.terminate(-1, "Error starting ApplicationHistoryServer");
    }
    return appHistoryServer;
  }

  public static void main(String[] args) {
    launchAppHistoryServer(args);
  }

  protected ApplicationHistoryManager createApplicationHistoryManager(
      Configuration conf) {
    return new ApplicationHistoryManagerImpl();
  }

  protected TimelineStore createTimelineStore(
      Configuration conf) {
    return ReflectionUtils.newInstance(conf.getClass(
        YarnConfiguration.TIMELINE_SERVICE_STORE, LeveldbTimelineStore.class,
        TimelineStore.class), conf);
  }

  protected void startWebApp() {
    String bindAddress = WebAppUtils.getAHSWebAppURLWithoutScheme(getConfig());
    LOG.info("Instantiating AHSWebApp at " + bindAddress);
    try {
      webApp =
          WebApps
            .$for("applicationhistory", ApplicationHistoryClientService.class,
              ahsClientService, "ws")
            .with(getConfig())
            .withHttpSpnegoPrincipalKey(
              YarnConfiguration.TIMELINE_SERVICE_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
              YarnConfiguration.TIMELINE_SERVICE_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .at(bindAddress)
            .start(new AHSWebApp(historyManager, timelineStore));
    } catch (Exception e) {
      String msg = "AHSWebApp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }
  /**
   * @return ApplicationTimelineStore
   */
  @Private
  @VisibleForTesting
  public TimelineStore getTimelineStore() {
    return timelineStore;
  }
}
