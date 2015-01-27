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

package org.apache.hadoop.yarn.server.timelineservice.aggregator;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The top-level server for the per-node timeline aggregator service. Currently
 * it is defined as an auxiliary service to accommodate running within another
 * daemon (e.g. node manager).
 */
@Private
@Unstable
public class PerNodeAggregatorServer extends AuxiliaryService {
  private static final Log LOG =
      LogFactory.getLog(PerNodeAggregatorServer.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private final AppLevelServiceManager serviceManager;
  private WebApp webApp;

  public PerNodeAggregatorServer() {
    // use the same singleton
    this(AppLevelServiceManager.getInstance());
  }

  @VisibleForTesting
  PerNodeAggregatorServer(AppLevelServiceManager serviceManager) {
    super("timeline_aggregator");
    this.serviceManager = serviceManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    serviceManager.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    serviceManager.start();
    startWebApp();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    // stop the service manager
    serviceManager.stop();
    super.serviceStop();
  }

  private void startWebApp() {
    Configuration conf = getConfig();
    // use the same ports as the old ATS for now; we could create new properties
    // for the new timeline service if needed
    String bindAddress = WebAppUtils.getWebAppBindURL(conf,
                          YarnConfiguration.TIMELINE_SERVICE_BIND_HOST,
                          WebAppUtils.getAHSWebAppURLWithoutScheme(conf));
    LOG.info("Instantiating the per-node aggregator webapp at " + bindAddress);
    try {
      webApp =
          WebApps
            .$for("timeline", null, null, "ws")
            .with(conf).at(bindAddress).start(
                new TimelineServiceWebApp());
    } catch (Exception e) {
      String msg = "The per-node aggregator webapp failed to start.";
      LOG.error(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  private static class TimelineServiceWebApp
      extends WebApp implements YarnWebParams {
    @Override
    public void setup() {
      bind(PerNodeAggregatorWebService.class);
      // bind to the global singleton
      bind(AppLevelServiceManager.class).
          toProvider(AppLevelServiceManagerProvider.class);
    }
  }

  // these methods can be used as the basis for future service methods if the
  // per-node aggregator runs separate from the node manager
  /**
   * Creates and adds an app level aggregator service for the specified
   * application id. The service is also initialized and started. If the service
   * already exists, no new service is created.
   *
   * @return whether it was added successfully
   */
  public boolean addApplication(ApplicationId appId) {
    String appIdString = appId.toString();
    return serviceManager.addService(appIdString);
  }

  /**
   * Removes the app level aggregator service for the specified application id.
   * The service is also stopped as a result. If the service does not exist, no
   * change is made.
   *
   * @return whether it was removed successfully
   */
  public boolean removeApplication(ApplicationId appId) {
    String appIdString = appId.toString();
    return serviceManager.removeService(appIdString);
  }

  /**
   * Creates and adds an app level aggregator service for the specified
   * application id. The service is also initialized and started. If the service
   * already exists, no new service is created.
   */
  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    // intercept the event of the AM container being created and initialize the
    // app level aggregator service
    if (isApplicationMaster(context)) {
      ApplicationId appId = context.getContainerId().
          getApplicationAttemptId().getApplicationId();
      addApplication(appId);
    }
  }

  /**
   * Removes the app level aggregator service for the specified application id.
   * The service is also stopped as a result. If the service does not exist, no
   * change is made.
   */
  @Override
  public void stopContainer(ContainerTerminationContext context) {
    // intercept the event of the AM container being stopped and remove the app
    // level aggregator service
    if (isApplicationMaster(context)) {
      ApplicationId appId = context.getContainerId().
          getApplicationAttemptId().getApplicationId();
      removeApplication(appId);
    }
  }

  private boolean isApplicationMaster(ContainerContext context) {
    // TODO this is based on a (shaky) assumption that the container id (the
    // last field of the full container id) for an AM is always 1
    // we want to make this much more reliable
    ContainerId containerId = context.getContainerId();
    return containerId.getContainerId() == 1L;
  }

  @VisibleForTesting
  boolean hasApplication(String appId) {
    return serviceManager.hasService(appId);
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
  }

  @Override
  public ByteBuffer getMetaData() {
    // TODO currently it is not used; we can return a more meaningful data when
    // we connect it with an AM
    return ByteBuffer.allocate(0);
  }

  @VisibleForTesting
  static PerNodeAggregatorServer launchServer(String[] args) {
    Thread
      .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(PerNodeAggregatorServer.class, args,
        LOG);
    PerNodeAggregatorServer server = null;
    try {
      server = new PerNodeAggregatorServer();
      ShutdownHookManager.get().addShutdownHook(new ShutdownHook(server),
          SHUTDOWN_HOOK_PRIORITY);
      YarnConfiguration conf = new YarnConfiguration();
      server.init(conf);
      server.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting PerNodeAggregatorServer", t);
      ExitUtil.terminate(-1, "Error starting PerNodeAggregatorServer");
    }
    return server;
  }

  private static class ShutdownHook implements Runnable {
    private final PerNodeAggregatorServer server;

    public ShutdownHook(PerNodeAggregatorServer server) {
      this.server = server;
    }

    public void run() {
      server.stop();
    }
  }

  public static void main(String[] args) {
    launchServer(args);
  }
}
