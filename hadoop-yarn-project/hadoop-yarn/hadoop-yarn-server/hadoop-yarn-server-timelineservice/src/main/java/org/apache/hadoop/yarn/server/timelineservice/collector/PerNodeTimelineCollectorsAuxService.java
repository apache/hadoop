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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerType;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The top-level server for the per-node timeline collector manager. Currently
 * it is defined as an auxiliary service to accommodate running within another
 * daemon (e.g. node manager).
 */
@Private
@Unstable
public class PerNodeTimelineCollectorsAuxService extends AuxiliaryService {
  private static final Logger LOG =
      LoggerFactory.getLogger(PerNodeTimelineCollectorsAuxService.class);
  private static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private final NodeTimelineCollectorManager collectorManager;
  private long collectorLingerPeriod;
  private ScheduledExecutorService scheduler;
  private Map<ApplicationId, Set<ContainerId>> appIdToContainerId =
      new ConcurrentHashMap<>();

  public PerNodeTimelineCollectorsAuxService() {
    this(new NodeTimelineCollectorManager(true));
  }

  @VisibleForTesting PerNodeTimelineCollectorsAuxService(
      NodeTimelineCollectorManager collectorsManager) {
    super("timeline_collector");
    this.collectorManager = collectorsManager;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (!YarnConfiguration.timelineServiceV2Enabled(conf)) {
      throw new YarnException(
          "Looks like timeline_collector is set as an auxillary service in "
              + YarnConfiguration.NM_AUX_SERVICES
              + ". But Timeline service v2 is not enabled,"
              + " so timeline_collector needs to be removed"
              + " from that list of auxillary services.");
    }
    collectorLingerPeriod =
        conf.getLong(YarnConfiguration.ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS,
            YarnConfiguration.DEFAULT_ATS_APP_COLLECTOR_LINGER_PERIOD_IN_MS);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    collectorManager.init(conf);
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    collectorManager.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    scheduler.shutdown();
    if (!scheduler.awaitTermination(collectorLingerPeriod,
        TimeUnit.MILLISECONDS)) {
      LOG.warn(
          "Scheduler terminated before removing the application collectors");
    }
    collectorManager.stop();
    super.serviceStop();
  }

  // these methods can be used as the basis for future service methods if the
  // per-node collector runs separate from the node manager
  /**
   * Creates and adds an app level collector for the specified application id.
   * The collector is also initialized and started. If the service already
   * exists, no new service is created.
   *
   * @param appId Application Id to be added.
   * @param user Application Master container user.
   * @return whether it was added successfully
   */
  public boolean addApplication(ApplicationId appId, String user) {
    AppLevelTimelineCollector collector =
        new AppLevelTimelineCollectorWithAgg(appId, user);
    return (collectorManager.putIfAbsent(appId, collector)
        == collector);
  }

  /**
   * Removes the app level collector for the specified application id. The
   * collector is also stopped as a result. If the collector does not exist, no
   * change is made.
   *
   * @param appId Application Id to be removed.
   * @return whether it was removed successfully
   */
  public boolean removeApplication(ApplicationId appId) {
    return collectorManager.remove(appId);
  }

  /**
   * Creates and adds an app level collector for the specified application id.
   * The collector is also initialized and started. If the collector already
   * exists, no new collector is created.
   */
  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    // intercept the event of the AM container being created and initialize the
    // app level collector service
    if (context.getContainerType() == ContainerType.APPLICATION_MASTER) {
      ApplicationId appId = context.getContainerId().
          getApplicationAttemptId().getApplicationId();
      synchronized (appIdToContainerId) {
        Set<ContainerId> masterContainers = appIdToContainerId.get(appId);
        if (masterContainers == null) {
          masterContainers = new HashSet<>();
          appIdToContainerId.put(appId, masterContainers);
        }
        masterContainers.add(context.getContainerId());
        addApplication(appId, context.getUser());
      }
    }
  }

  /**
   * Removes the app level collector for the specified application id. The
   * collector is also stopped as a result. If the collector does not exist, no
   * change is made.
   */
  @Override
  public void stopContainer(ContainerTerminationContext context) {
    // intercept the event of the AM container being stopped and remove the app
    // level collector service
    if (context.getContainerType() == ContainerType.APPLICATION_MASTER) {
      final ContainerId containerId = context.getContainerId();
      removeApplicationCollector(containerId);
    }
  }

  @VisibleForTesting
  protected Future removeApplicationCollector(final ContainerId containerId) {
    final ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();
    return scheduler.schedule(new Runnable() {
      public void run() {
        synchronized (appIdToContainerId) {
          Set<ContainerId> masterContainers = appIdToContainerId.get(appId);
          if (masterContainers == null) {
            LOG.info("Stop container for " + containerId
                + " is called before initializing container.");
            return;
          }
          masterContainers.remove(containerId);
          if (masterContainers.size() == 0) {
            // remove only if it is last master container
            removeApplication(appId);
            appIdToContainerId.remove(appId);
          }
        }
      }
    }, collectorLingerPeriod, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean hasApplication(ApplicationId appId) {
    return collectorManager.containsTimelineCollector(appId);
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
  public static PerNodeTimelineCollectorsAuxService
      launchServer(String[] args, NodeTimelineCollectorManager collectorManager,
      Configuration conf) {
    Thread
      .setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(
        PerNodeTimelineCollectorsAuxService.class, args, LOG);
    PerNodeTimelineCollectorsAuxService auxService = null;
    try {
      auxService = collectorManager == null ?
          new PerNodeTimelineCollectorsAuxService(
              new NodeTimelineCollectorManager(false)) :
          new PerNodeTimelineCollectorsAuxService(collectorManager);
      ShutdownHookManager.get().addShutdownHook(new ShutdownHook(auxService),
          SHUTDOWN_HOOK_PRIORITY);
      auxService.init(conf);
      auxService.start();
    } catch (Throwable t) {
      LOG.error("Error starting PerNodeTimelineCollectorServer", t);
      ExitUtil.terminate(-1, "Error starting PerNodeTimelineCollectorServer");
    }
    return auxService;
  }

  private static class ShutdownHook implements Runnable {
    private final PerNodeTimelineCollectorsAuxService auxService;

    public ShutdownHook(PerNodeTimelineCollectorsAuxService auxService) {
      this.auxService = auxService;
    }

    public void run() {
      auxService.stop();
    }
  }

  public static void main(String[] args) {
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
    conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
    launchServer(args, null, conf);
  }
}
