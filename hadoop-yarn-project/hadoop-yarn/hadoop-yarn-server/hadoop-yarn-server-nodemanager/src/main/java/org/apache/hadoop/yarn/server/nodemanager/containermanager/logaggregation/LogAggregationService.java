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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.token.SecretManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileControllerFactory;
import org.apache.hadoop.yarn.server.api.ContainerLogContext;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LogAggregationService extends AbstractService implements
    LogHandler {

  private static final Logger LOG =
       LoggerFactory.getLogger(LogAggregationService.class);
  private static final long MIN_LOG_ROLLING_INTERVAL = 3600;
  // This configuration is for debug and test purpose. By setting
  // this configuration as true. We can break the lower bound of
  // NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS.
  private static final String NM_LOG_AGGREGATION_DEBUG_ENABLED
      = YarnConfiguration.NM_PREFIX + "log-aggregation.debug-enabled";
  private long rollingMonitorInterval;

  private final Context context;
  private final DeletionService deletionService;
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  private NodeId nodeId;

  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  // Holds applications whose aggregation is disable due to invalid Token
  private final Set<ApplicationId> invalidTokenApps;

  @VisibleForTesting
  ExecutorService threadPool;
  
  public LogAggregationService(Dispatcher dispatcher, Context context,
      DeletionService deletionService, LocalDirsHandlerService dirsHandler) {
    super(LogAggregationService.class.getName());
    this.dispatcher = dispatcher;
    this.context = context;
    this.deletionService = deletionService;
    this.dirsHandler = dirsHandler;
    this.appLogAggregators =
        new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.invalidTokenApps = ConcurrentHashMap.newKeySet();
  }

  protected void serviceInit(Configuration conf) throws Exception {
    int threadPoolSize = getAggregatorThreadPoolSize(conf);
    this.threadPool = HadoopExecutors.newFixedThreadPool(threadPoolSize,
        new ThreadFactoryBuilder()
            .setNameFormat("LogAggregationService #%d")
            .build());

    rollingMonitorInterval = conf.getLong(
        YarnConfiguration.NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS,
        YarnConfiguration.DEFAULT_NM_LOG_AGGREGATION_ROLL_MONITORING_INTERVAL_SECONDS);

    boolean logAggregationDebugMode =
        conf.getBoolean(NM_LOG_AGGREGATION_DEBUG_ENABLED, false);

    if (rollingMonitorInterval > 0
        && rollingMonitorInterval < MIN_LOG_ROLLING_INTERVAL) {
      if (logAggregationDebugMode) {
        LOG.info("Log aggregation debug mode enabled. rollingMonitorInterval = "
            + rollingMonitorInterval);
      } else {
        LOG.warn("rollingMonitorInterval should be more than or equal to {} " +
                "seconds. Using {} seconds instead.",
                MIN_LOG_ROLLING_INTERVAL, MIN_LOG_ROLLING_INTERVAL);
        this.rollingMonitorInterval = MIN_LOG_ROLLING_INTERVAL;
      }
    } else if (rollingMonitorInterval <= 0) {
      LOG.info("rollingMonitorInterval is set as " + rollingMonitorInterval
          + ". The log rolling monitoring interval is disabled. "
          + "The logs will be aggregated after this application is finished.");
    } else {
      LOG.info("rollingMonitorInterval is set as " + rollingMonitorInterval
          + ". The logs will be aggregated every " + rollingMonitorInterval
          + " seconds");
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // NodeId is only available during start, the following cannot be moved
    // anywhere else.
    this.nodeId = this.context.getNodeId();
    super.serviceStart();
  }
  
  @Override
  protected void serviceStop() throws Exception {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    stopAggregators();
    super.serviceStop();
  }
   
  private void stopAggregators() {
    threadPool.shutdown();
    boolean supervised = getConfig().getBoolean(
        YarnConfiguration.NM_RECOVERY_SUPERVISED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED);
    // if recovery on restart is supported then leave outstanding aggregations
    // to the next restart
    boolean shouldAbort = context.getNMStateStore().canRecover()
        && !context.getDecommissioned() && supervised;
    // politely ask to finish
    for (AppLogAggregator aggregator : appLogAggregators.values()) {
      if (shouldAbort) {
        aggregator.abortLogAggregation();
      } else {
        aggregator.finishLogAggregation();
      }
    }
    while (!threadPool.isTerminated()) { // wait for all threads to finish
      for (ApplicationId appId : appLogAggregators.keySet()) {
        LOG.info("Waiting for aggregation to complete for " + appId);
      }
      try {
        if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
          threadPool.shutdownNow(); // send interrupt to hurry them along
        }
      } catch (InterruptedException e) {
        LOG.warn("Aggregation stop interrupted!");
        break;
      }
    }
    for (ApplicationId appId : appLogAggregators.keySet()) {
      LOG.warn("Some logs may not have been aggregated for " + appId);
    }
  }

  @SuppressWarnings("unchecked")
  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      long recoveredLogInitedTime) {
    ApplicationEvent eventResponse;
    try {
      initAppAggregator(appId, user, credentials, appAcls,
          logAggregationContext, recoveredLogInitedTime);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
    } catch (YarnRuntimeException e) {
      LOG.warn("Application failed to init aggregation", e);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED);
    }
    this.dispatcher.getEventHandler().handle(eventResponse);
  }
  
  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  protected void initAppAggregator(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      long recoveredLogInitedTime) {

    // Get user's FileSystem credentials
    final UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    if (credentials != null) {
      userUgi.addCredentials(credentials);
    }

    LogAggregationFileController logAggregationFileController =
        getLogAggregationFileController(getConfig());
    logAggregationFileController.verifyAndCreateRemoteLogDir();
    // New application
    final AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.dispatcher, this.deletionService,
            getConfig(), appId, userUgi, this.nodeId, dirsHandler,
            logAggregationFileController.getRemoteNodeLogFileForApp(appId,
            user, nodeId), appAcls, logAggregationContext, this.context,
            getLocalFileContext(getConfig()), this.rollingMonitorInterval,
            recoveredLogInitedTime, logAggregationFileController);
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnRuntimeException("Duplicate initApp for " + appId);
    }
    // wait until check for existing aggregator to create dirs
    YarnRuntimeException appDirException = null;
    try {
      // Create the app dir
      logAggregationFileController.createAppDir(user, appId, userUgi);
    } catch (Exception e) {
      appLogAggregator.disableLogAggregation();

      // add to disabled aggregators if due to InvalidToken
      if (e.getCause() instanceof SecretManager.InvalidToken) {
        invalidTokenApps.add(appId);
      }
      if (!(e instanceof YarnRuntimeException)) {
        appDirException = new YarnRuntimeException(e);
      } else {
        appDirException = (YarnRuntimeException)e;
      }
    }

    // TODO Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    Runnable aggregatorWrapper = new Runnable() {
      public void run() {
        try {
          appLogAggregator.run();
        } finally {
          appLogAggregators.remove(appId);
          closeFileSystems(userUgi);
        }
      }
    };
    this.threadPool.execute(aggregatorWrapper);

    if (appDirException != null) {
      throw appDirException;
    }
  }

  protected void closeFileSystems(final UserGroupInformation userUgi) {
    try {
      FileSystem.closeAllForUGI(userUgi);
    } catch (IOException e) {
      LOG.warn("Failed to close filesystems: ", e);
    }
  }

  // for testing only
  @Private
  int getNumAggregators() {
    return this.appLogAggregators.size();
  }

  private void stopContainer(ContainerId containerId,
      ContainerType containerType, int exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.
    AppLogAggregator aggregator = this.appLogAggregators.get(
        containerId.getApplicationAttemptId().getApplicationId());
    if (aggregator == null) {
      LOG.warn("Log aggregation is not initialized for " + containerId
          + ", did it fail to start?");
      return;
    }
    aggregator.startContainerLogAggregation(
        new ContainerLogContext(containerId, containerType, exitCode));
  }

  @SuppressWarnings("unchecked")
  private void stopApp(ApplicationId appId) {

    // App is complete. Finish up any containers' pending log aggregation and
    // close the application specific logFile.
    try {
      AppLogAggregator aggregator = this.appLogAggregators.get(appId);
      if (aggregator == null) {
        LOG.warn("Log aggregation is not initialized for " + appId
            + ", did it fail to start?");
        this.dispatcher.getEventHandler().handle(new ApplicationEvent(appId,
            ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED));
        return;
      }
      aggregator.finishLogAggregation();
    } finally {
      // Remove invalid Token Apps
      invalidTokenApps.remove(appId);
    }
  }

  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartEvent =
            (LogHandlerAppStartedEvent) event;
        initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
            appStartEvent.getCredentials(),
            appStartEvent.getApplicationAcls(),
            appStartEvent.getLogAggregationContext(),
            appStartEvent.getRecoveredAppLogInitedTime());
        break;
      case CONTAINER_FINISHED:
        LogHandlerContainerFinishedEvent containerFinishEvent =
            (LogHandlerContainerFinishedEvent) event;
        stopContainer(containerFinishEvent.getContainerId(),
            containerFinishEvent.getContainerType(),
            containerFinishEvent.getExitCode());
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        stopApp(appFinishedEvent.getApplicationId());
        break;
      case LOG_AGG_TOKEN_UPDATE:
        checkAndEnableAppAggregators();
        break;
      default:
        ; // Ignore
    }

  }

  private void checkAndEnableAppAggregators() {
    for (ApplicationId appId : invalidTokenApps) {
      try {
        AppLogAggregator aggregator = appLogAggregators.get(appId);
        if (aggregator != null) {
          Credentials credentials =
              context.getSystemCredentialsForApps().get(appId);
          if (credentials != null) {
            // Create the app dir again with
            LogAggregationFileController logAggregationFileController =
                getLogAggregationFileController(getConfig());
            UserGroupInformation userUgi =
                aggregator.updateCredentials(credentials);
            logAggregationFileController
                .createAppDir(userUgi.getShortUserName(), appId, userUgi);
            aggregator.enableLogAggregation();
          }
          invalidTokenApps.remove(appId);
          LOG.info("LogAggregation enabled for application {}", appId);
        }
      } catch (Exception e) {
        //Ignore exception
        LOG.warn("Enable aggregators failed {}", appId);
      }
    }
  }

  @Override
  public Set<ApplicationId> getInvalidTokenApps() {
    return invalidTokenApps;
  }

  @VisibleForTesting
  public ConcurrentMap<ApplicationId, AppLogAggregator> getAppLogAggregators() {
    return this.appLogAggregators;
  }

  @VisibleForTesting
  public NodeId getNodeId() {
    return this.nodeId;
  }


  private int getAggregatorThreadPoolSize(Configuration conf) {
    int threadPoolSize;
    try {
      threadPoolSize = conf.getInt(YarnConfiguration
          .NM_LOG_AGGREGATION_THREAD_POOL_SIZE,
          YarnConfiguration.DEFAULT_NM_LOG_AGGREGATION_THREAD_POOL_SIZE);
    } catch (NumberFormatException ex) {
      LOG.warn("Invalid thread pool size. Setting it to the default value " +
          "in YarnConfiguration");
      threadPoolSize = YarnConfiguration.
          DEFAULT_NM_LOG_AGGREGATION_THREAD_POOL_SIZE;
    }
    if(threadPoolSize <= 0) {
      LOG.warn("Invalid thread pool size. Setting it to the default value " +
          "in YarnConfiguration");
      threadPoolSize = YarnConfiguration.
          DEFAULT_NM_LOG_AGGREGATION_THREAD_POOL_SIZE;
    }
    return threadPoolSize;
  }

  @VisibleForTesting
  public LogAggregationFileController getLogAggregationFileController(
      Configuration conf) {
    LogAggregationFileControllerFactory factory
        = new LogAggregationFileControllerFactory(conf);
    LogAggregationFileController logAggregationFileController = factory
        .getFileControllerForWrite();
    return logAggregationFileController;
  }
}
