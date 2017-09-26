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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStopMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEventType;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;

public class ContainerImpl implements Container {

  private static final class ReInitializationContext {
    private final ContainerLaunchContext newLaunchContext;
    private final ResourceSet newResourceSet;

    // Rollback state
    private final ContainerLaunchContext oldLaunchContext;
    private final ResourceSet oldResourceSet;

    private boolean isRollback = false;

    private ReInitializationContext(ContainerLaunchContext newLaunchContext,
        ResourceSet newResourceSet,
        ContainerLaunchContext oldLaunchContext,
        ResourceSet oldResourceSet) {
      this.newLaunchContext = newLaunchContext;
      this.newResourceSet = newResourceSet;
      this.oldLaunchContext = oldLaunchContext;
      this.oldResourceSet = oldResourceSet;
    }

    private boolean canRollback() {
      return (oldLaunchContext != null);
    }

    private ResourceSet mergedResourceSet(ResourceSet current) {
      if (isRollback) {
        // No merging should be done for rollback
        return newResourceSet;
      }
      if (current == newResourceSet) {
        // This happens during a restart
        return current;
      }
      return ResourceSet.merge(current, newResourceSet);
    }

    private ReInitializationContext createContextForRollback() {
      ReInitializationContext cntxt = new ReInitializationContext(
          oldLaunchContext, oldResourceSet, null, null);
      cntxt.isRollback = true;
      return cntxt;
    }
  }

  private final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private final Lock readLock;
  private final Lock writeLock;
  private final Dispatcher dispatcher;
  private final NMStateStoreService stateStore;
  private final Credentials credentials;
  private final NodeManagerMetrics metrics;
  private volatile ContainerLaunchContext launchContext;
  private volatile ContainerTokenIdentifier containerTokenIdentifier;
  private final ContainerId containerId;
  private final String user;
  private int version;
  private int exitCode = ContainerExitStatus.INVALID;
  private final StringBuilder diagnostics;
  private final int diagnosticsMaxSize;
  private boolean wasLaunched;
  private long containerLocalizationStartTime;
  private long containerLaunchStartTime;
  private ContainerMetrics containerMetrics;
  private static Clock clock = SystemClock.getInstance();
  private ContainerRetryContext containerRetryContext;
  // remaining retries to relaunch container if needed
  private int remainingRetryAttempts;
  private String workDir;
  private String logDir;
  private String host;
  private String ips;
  private volatile ReInitializationContext reInitContext;
  private volatile boolean isReInitializing = false;
  private volatile boolean isMarkeForKilling = false;

  /** The NM-wide configuration - not specific to this container */
  private final Configuration daemonConf;
  private final long startTime;

  private static final Logger LOG =
       LoggerFactory.getLogger(ContainerImpl.class);

  // whether container has been recovered after a restart
  private RecoveredContainerStatus recoveredStatus =
      RecoveredContainerStatus.REQUESTED;
  // whether container was marked as killed after recovery
  private boolean recoveredAsKilled = false;
  private Context context;
  private ResourceSet resourceSet;

  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context) {
    this(conf, dispatcher, launchContext, creds, metrics,
        containerTokenIdentifier, context, SystemClock.getInstance().getTime());
  }

  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context,
      long startTs) {
    this.startTime = startTs;
    this.daemonConf = conf;
    this.dispatcher = dispatcher;
    this.stateStore = context.getNMStateStore();
    this.version = containerTokenIdentifier.getVersion();
    this.launchContext = launchContext;

    this.diagnosticsMaxSize = conf.getInt(
        YarnConfiguration.NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE,
        YarnConfiguration.DEFAULT_NM_CONTAINER_DIAGNOSTICS_MAXIMUM_SIZE);
    this.containerTokenIdentifier = containerTokenIdentifier;
    this.containerId = containerTokenIdentifier.getContainerID();
    this.diagnostics = new StringBuilder();
    this.credentials = creds;
    this.metrics = metrics;
    user = containerTokenIdentifier.getApplicationSubmitter();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.context = context;
    boolean containerMetricsEnabled =
        conf.getBoolean(YarnConfiguration.NM_CONTAINER_METRICS_ENABLE,
            YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_ENABLE);

    if (containerMetricsEnabled) {
      long flushPeriod =
          conf.getLong(YarnConfiguration.NM_CONTAINER_METRICS_PERIOD_MS,
              YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_PERIOD_MS);
      long unregisterDelay = conf.getLong(
          YarnConfiguration.NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS,
          YarnConfiguration.DEFAULT_NM_CONTAINER_METRICS_UNREGISTER_DELAY_MS);
      containerMetrics = ContainerMetrics
          .forContainer(containerId, flushPeriod, unregisterDelay);
      containerMetrics.recordStartTime(clock.getTime());
    }

    // Configure the Retry Context
    this.containerRetryContext = configureRetryContext(
        conf, launchContext, this.containerId);
    this.remainingRetryAttempts = this.containerRetryContext.getMaxRetries();
    stateMachine = stateMachineFactory.make(this, ContainerState.NEW,
        context.getContainerStateTransitionListener());
    this.context = context;
    this.resourceSet = new ResourceSet();
  }

  private static ContainerRetryContext configureRetryContext(
      Configuration conf, ContainerLaunchContext launchContext,
      ContainerId containerId) {
    ContainerRetryContext context;
    if (launchContext != null
        && launchContext.getContainerRetryContext() != null) {
      context = launchContext.getContainerRetryContext();
    } else {
      context = ContainerRetryContext.NEVER_RETRY_CONTEXT;
    }
    int minimumRestartInterval = conf.getInt(
        YarnConfiguration.NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_CONTAINER_RETRY_MINIMUM_INTERVAL_MS);
    if (context.getRetryPolicy() != ContainerRetryPolicy.NEVER_RETRY
        && context.getRetryInterval() < minimumRestartInterval) {
      LOG.info("Set restart interval to minimum value " + minimumRestartInterval
          + "ms for container " + containerId);
      context.setRetryInterval(minimumRestartInterval);
    }
    return context;
  }

  // constructor for a recovered container
  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context,
      RecoveredContainerState rcs) {
    this(conf, dispatcher, launchContext, creds, metrics,
        containerTokenIdentifier, context, rcs.getStartTime());
    this.recoveredStatus = rcs.getStatus();
    this.exitCode = rcs.getExitCode();
    this.recoveredAsKilled = rcs.getKilled();
    this.diagnostics.append(rcs.getDiagnostics());
    this.version = rcs.getVersion();
    this.remainingRetryAttempts = rcs.getRemainingRetryAttempts();
    this.workDir = rcs.getWorkDir();
    this.logDir = rcs.getLogDir();
  }

  private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION =
      new ContainerDiagnosticsUpdateTransition();

  // State Machine for each container.
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW,
        EnumSet.of(ContainerState.LOCALIZING,
            ContainerState.SCHEDULED,
            ContainerState.LOCALIZATION_FAILED,
            ContainerState.DONE),
        ContainerEventType.INIT_CONTAINER, new RequestResourcesTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER, new KillOnNewTransition())

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.SCHEDULED),
        ContainerEventType.RESOURCE_LOCALIZED, new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceFailedTransition())
    .addTransition(ContainerState.LOCALIZING, ContainerState.LOCALIZING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER,
        new KillBeforeRunningTransition())

    // From LOCALIZATION_FAILED State
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        new LocalizationFailedToDoneTransition())
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    // container not launched so kill is a no-op
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.KILL_CONTAINER)
    // container cleanup triggers a release of all resources
    // regardless of whether they were localized or not
    // LocalizedResource handles release event in all states
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_LOCALIZED)
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED)

    // From SCHEDULED State
    .addTransition(ContainerState.SCHEDULED, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.SCHEDULED, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.SCHEDULED, ContainerState.SCHEDULED,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.SCHEDULED, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER,
        new KillBeforeRunningTransition())

    // From RUNNING State
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(true))
    .addTransition(ContainerState.RUNNING,
        EnumSet.of(ContainerState.RELAUNCHING,
            ContainerState.SCHEDULED,
            ContainerState.EXITED_WITH_FAILURE),
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new RetryFailureTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.REINITIALIZING,
        ContainerEventType.REINITIALIZE_CONTAINER,
        new ReInitializeContainerTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.REINITIALIZING,
        ContainerEventType.ROLLBACK_REINIT,
        new RollbackContainerTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new ResourceLocalizedWhileRunningTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceLocalizationFailedWhileRunningTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.RUNNING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new KilledExternallyTransition())

    // From REINITIALIZING State
    .addTransition(ContainerState.REINITIALIZING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(true))
    .addTransition(ContainerState.REINITIALIZING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.REINITIALIZING,
        ContainerState.REINITIALIZING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new ResourceLocalizedWhileReInitTransition())
    .addTransition(ContainerState.REINITIALIZING, ContainerState.RUNNING,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceLocalizationFailedWhileReInitTransition())
    .addTransition(ContainerState.REINITIALIZING,
        ContainerState.REINITIALIZING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.REINITIALIZING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())
    .addTransition(ContainerState.REINITIALIZING,
        ContainerState.SCHEDULED,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new KilledForReInitializationTransition())

    // From RELAUNCHING State
    .addTransition(ContainerState.RELAUNCHING, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.RELAUNCHING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.RELAUNCHING, ContainerState.RELAUNCHING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.RELAUNCHING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From CONTAINER_EXITED_WITH_SUCCESS State
    .addTransition(ContainerState.EXITED_WITH_SUCCESS, ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        new ExitedWithSuccessToDoneTransition())
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.KILL_CONTAINER)

    // From EXITED_WITH_FAILURE State
    .addTransition(ContainerState.EXITED_WITH_FAILURE, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new ExitedWithFailureToDoneTransition())
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
                   ContainerState.EXITED_WITH_FAILURE,
                   ContainerEventType.KILL_CONTAINER)

    // From KILLING State.
    .addTransition(ContainerState.KILLING,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new ContainerKilledTransition())
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new LocalizedResourceDuringKillTransition())
    .addTransition(ContainerState.KILLING, 
        ContainerState.KILLING, 
        ContainerEventType.RESOURCE_FAILED)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(false))
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(false))
    .addTransition(ContainerState.KILLING,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new KillingToDoneTransition())
    // Handle a launched container during killing stage is a no-op
    // as cleanup container is always handled after launch container event
    // in the container launcher
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.CONTAINER_LAUNCHED)

    // From CONTAINER_CLEANEDUP_AFTER_KILL State.
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new ContainerCleanedupAfterKillToDoneTransition())
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        EnumSet.of(ContainerEventType.KILL_CONTAINER,
            ContainerEventType.RESOURCE_FAILED,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE))

    // From DONE
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.INIT_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    // This transition may result when
    // we notify container of failed localization if localizer thread (for
    // that container) fails for some reason
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        EnumSet.of(ContainerEventType.RESOURCE_FAILED,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE))

    // create the topology tables
    .installTopology();

  private final StateMachine<ContainerState, ContainerEventType, ContainerEvent>
    stateMachine;

  public org.apache.hadoop.yarn.api.records.ContainerState getCurrentState() {
    switch (stateMachine.getCurrentState()) {
    case NEW:
    case LOCALIZING:
    case LOCALIZATION_FAILED:
    case SCHEDULED:
      return org.apache.hadoop.yarn.api.records.ContainerState.SCHEDULED;
    case RUNNING:
    case RELAUNCHING:
    case REINITIALIZING:
    case EXITED_WITH_SUCCESS:
    case EXITED_WITH_FAILURE:
    case KILLING:
    case CONTAINER_CLEANEDUP_AFTER_KILL:
    case CONTAINER_RESOURCES_CLEANINGUP:
      return org.apache.hadoop.yarn.api.records.ContainerState.RUNNING;
    case DONE:
    default:
      return org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE;
    }
  }

  public NMTimelinePublisher getNMTimelinePublisher() {
    return context.getNMTimelinePublisher();
  }

  @Override
  public String getUser() {
    this.readLock.lock();
    try {
      return this.user;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<Path, List<String>> getLocalizedResources() {
    this.readLock.lock();
    try {
      if (ContainerState.SCHEDULED == getContainerState()
          || ContainerState.RELAUNCHING == getContainerState()) {
        return resourceSet.getLocalizedResources();
      } else {
        return null;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Credentials getCredentials() {
    this.readLock.lock();
    try {
      return credentials;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    this.readLock.lock();
    try {
      return launchContext;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerStatus cloneAndGetContainerStatus() {
    this.readLock.lock();
    try {
      ContainerStatus status = BuilderUtils.newContainerStatus(this.containerId,
          getCurrentState(), diagnostics.toString(), exitCode, getResource(),
          this.containerTokenIdentifier.getExecutionType());
      status.setIPs(ips == null ? null : Arrays.asList(ips.split(",")));
      status.setHost(host);
      return status;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public NMContainerStatus getNMContainerStatus() {
    this.readLock.lock();
    try {
      return NMContainerStatus.newInstance(this.containerId, this.version,
          getCurrentState(), getResource(), diagnostics.toString(), exitCode,
          containerTokenIdentifier.getPriority(),
          containerTokenIdentifier.getCreationTime(),
          containerTokenIdentifier.getNodeLabelExpression(),
          containerTokenIdentifier.getExecutionType());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public long getContainerStartTime() {
    return this.startTime;
  }

  @Override
  public Resource getResource() {
    return Resources.clone(
        this.containerTokenIdentifier.getResource());
  }

  @Override
  public ContainerTokenIdentifier getContainerTokenIdentifier() {
    this.readLock.lock();
    try {
      return this.containerTokenIdentifier;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public void setContainerTokenIdentifier(ContainerTokenIdentifier token) {
    this.writeLock.lock();
    try {
      this.containerTokenIdentifier = token;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String getWorkDir() {
    return workDir;
  }

  @Override
  public void setWorkDir(String workDir) {
    this.workDir = workDir;
  }

  @Override
  public void setIpAndHost(String[] ipAndHost) {
    this.ips = ipAndHost[0];
    this.host = ipAndHost[1];
  }

  @Override
  public String getLogDir() {
    return logDir;
  }

  @Override
  public void setLogDir(String logDir) {
    this.logDir = logDir;
  }

  @Override
  public ResourceSet getResourceSet() {
    return this.resourceSet;
  }

  @SuppressWarnings("unchecked")
  private void sendFinishedEvents() {
    // Inform the application
    @SuppressWarnings("rawtypes")
    EventHandler eventHandler = dispatcher.getEventHandler();

    ContainerStatus containerStatus = cloneAndGetContainerStatus();
    eventHandler.handle(
        new ApplicationContainerFinishedEvent(containerStatus, startTime));

    // Tell the scheduler the container is Done
    eventHandler.handle(new ContainerSchedulerEvent(this,
        ContainerSchedulerEventType.CONTAINER_COMPLETED));
    // Remove the container from the resource-monitor
    eventHandler.handle(new ContainerStopMonitoringEvent(containerId));
    // Tell the logService too
    eventHandler.handle(new LogHandlerContainerFinishedEvent(
        containerId, containerTokenIdentifier.getContainerType(), exitCode));
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  @Override
  public void sendLaunchEvent() {
    ContainersLauncherEventType launcherEvent =
        ContainersLauncherEventType.LAUNCH_CONTAINER;
    if (recoveredStatus == RecoveredContainerStatus.LAUNCHED) {
      // try to recover a container that was previously launched
      launcherEvent = ContainersLauncherEventType.RECOVER_CONTAINER;
    }
    containerLaunchStartTime = clock.getTime();
    dispatcher.getEventHandler().handle(
        new ContainersLauncherEvent(this, launcherEvent));
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendScheduleEvent() {
    dispatcher.getEventHandler().handle(
        new ContainerSchedulerEvent(this,
            ContainerSchedulerEventType.SCHEDULE_CONTAINER)
    );
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  @Override
  public void sendKillEvent(int exitStatus, String description) {
    this.isMarkeForKilling = true;
    dispatcher.getEventHandler().handle(
        new ContainerKillEvent(containerId, exitStatus, description));
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendRelaunchEvent() {
    ContainersLauncherEventType launcherEvent =
        ContainersLauncherEventType.RELAUNCH_CONTAINER;
    dispatcher.getEventHandler().handle(
        new ContainersLauncherEvent(this, launcherEvent));
  }

  // Inform the ContainersMonitor to start monitoring the container's
  // resource usage.
  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendContainerMonitorStartEvent() {
    long launchDuration = clock.getTime() - containerLaunchStartTime;
    metrics.addContainerLaunchDuration(launchDuration);

    long pmemBytes = getResource().getMemorySize() * 1024 * 1024L;
    float pmemRatio = daemonConf.getFloat(
        YarnConfiguration.NM_VMEM_PMEM_RATIO,
        YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
    long vmemBytes = (long) (pmemRatio * pmemBytes);
    int cpuVcores = getResource().getVirtualCores();
    long localizationDuration = containerLaunchStartTime -
        containerLocalizationStartTime;
    dispatcher.getEventHandler().handle(
        new ContainerStartMonitoringEvent(containerId,
        vmemBytes, pmemBytes, cpuVcores, launchDuration,
        localizationDuration));
  }

  private void addDiagnostics(String... diags) {
    for (String s : diags) {
      this.diagnostics.append("[" + dateFormat.format(new Date()) + "]" + s);
    }
    if (diagnostics.length() > diagnosticsMaxSize) {
      diagnostics.delete(0, diagnostics.length() - diagnosticsMaxSize);
    }
    try {
      stateStore.storeContainerDiagnostics(containerId, diagnostics);
    } catch (IOException e) {
      LOG.warn("Unable to update diagnostics in state store for "
          + containerId, e);
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  public void cleanup() {
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc =
        resourceSet.getAllResourcesByVisibility();
    dispatcher.getEventHandler().handle(
        new ContainerLocalizationCleanupEvent(this, rsrc));
  }

  static class ContainerTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Just drain the event and change the state.
    }

  }

  /**
   * State transition when a NEW container receives the INIT_CONTAINER
   * message.
   * 
   * If there are resources to localize, sends a
   * ContainerLocalizationRequest (LOCALIZE_CONTAINER_RESOURCES)
   * to the ResourceLocalizationManager and enters LOCALIZING state.
   * 
   * If there are no resources to localize, sends LAUNCH_CONTAINER event
   * and enters SCHEDULED state directly.
   * 
   * If there are any invalid resources specified, enters LOCALIZATION_FAILED
   * directly.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class RequestResourcesTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      if (container.recoveredStatus == RecoveredContainerStatus.COMPLETED) {
        container.sendFinishedEvents();
        return ContainerState.DONE;
      } else if (container.recoveredAsKilled &&
          container.recoveredStatus == RecoveredContainerStatus.REQUESTED) {
        // container was killed but never launched
        container.metrics.killedContainer();
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
        container.metrics.releaseContainer(
            container.containerTokenIdentifier.getResource());
        container.sendFinishedEvents();
        return ContainerState.DONE;
      }

      final ContainerLaunchContext ctxt = container.launchContext;
      container.metrics.initingContainer();

      container.dispatcher.getEventHandler().handle(new AuxServicesEvent
          (AuxServicesEventType.CONTAINER_INIT, container));

      // Inform the AuxServices about the opaque serviceData
      Map<String,ByteBuffer> csd = ctxt.getServiceData();
      if (csd != null) {
        // This can happen more than once per Application as each container may
        // have distinct service data
        for (Map.Entry<String,ByteBuffer> service : csd.entrySet()) {
          container.dispatcher.getEventHandler().handle(
              new AuxServicesEvent(AuxServicesEventType.APPLICATION_INIT,
                  container.user, container.containerId
                      .getApplicationAttemptId().getApplicationId(),
                  service.getKey().toString(), service.getValue()));
        }
      }

      container.containerLocalizationStartTime = clock.getTime();

      // Send requests for public, private resources
      Map<String,LocalResource> cntrRsrc = ctxt.getLocalResources();
      if (!cntrRsrc.isEmpty()) {
        try {
          Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
              container.resourceSet.addResources(ctxt.getLocalResources());
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(container, req));
        } catch (URISyntaxException e) {
          // malformed resource; abort container launch
          LOG.warn("Failed to parse resource-request", e);
          container.cleanup();
          container.metrics.endInitingContainer();
          return ContainerState.LOCALIZATION_FAILED;
        }
        return ContainerState.LOCALIZING;
      } else {
        container.sendScheduleEvent();
        container.metrics.endInitingContainer();
        return ContainerState.SCHEDULED;
      }
    }
  }

  /**
   * Transition when one of the requested resources for this container
   * has been successfully localized.
   */
  static class LocalizedTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @SuppressWarnings("unchecked")
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      LocalResourceRequest resourceRequest = rsrcEvent.getResource();
      Path location = rsrcEvent.getLocation();
      Set<String> syms =
          container.resourceSet.resourceLocalized(resourceRequest, location);
      if (null == syms) {
        LOG.info("Localized resource " + resourceRequest +
            " for container " + container.containerId);
        return ContainerState.LOCALIZING;
      }

      // check to see if this resource should be uploaded to the shared cache
      // as well
      if (shouldBeUploadedToSharedCache(container, resourceRequest)) {
        container.resourceSet.getResourcesToBeUploaded()
            .put(resourceRequest, location);
      }
      if (!container.resourceSet.getPendingResources().isEmpty()) {
        return ContainerState.LOCALIZING;
      }

      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(LocalizationEventType.
              CONTAINER_RESOURCES_LOCALIZED, container));

      container.sendScheduleEvent();
      container.metrics.endInitingContainer();

      // If this is a recovered container that has already launched, skip
      // uploading resources to the shared cache. We do this to avoid uploading
      // the same resources multiple times. The tradeoff is that in the case of
      // a recovered container, there is a chance that resources don't get
      // uploaded into the shared cache. This is OK because resources are not
      // acknowledged by the SCM until they have been uploaded by the node
      // manager.
      if (container.recoveredStatus != RecoveredContainerStatus.LAUNCHED
          && container.recoveredStatus != RecoveredContainerStatus.COMPLETED) {
        // kick off uploads to the shared cache
        container.dispatcher.getEventHandler().handle(
            new SharedCacheUploadEvent(
                container.resourceSet.getResourcesToBeUploaded(), container
                .getLaunchContext(), container.getUser(),
                SharedCacheUploadEventType.UPLOAD));
      }

      return ContainerState.SCHEDULED;
    }
  }

  /**
   * Transition to start the Re-Initialization process.
   */
  static class ReInitializeContainerTransition extends ContainerTransition {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.reInitContext = createReInitContext(container, event);
      try {
        // 'reInitContext.newResourceSet' can be
        // a) current container resourceSet (In case of Restart)
        // b) previous resourceSet (In case of RollBack)
        // c) An actual NEW resourceSet (In case of Upgrade/ReInit)
        //
        // In cases a) and b) Container can immediately be cleaned up since
        // we are sure the resources are already available (we check the
        // pendingResources to verify that nothing more is needed). So we can
        // kill the container immediately
        ResourceSet newResourceSet = container.reInitContext.newResourceSet;
        if (!newResourceSet.getPendingResources().isEmpty()) {
          container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(
                  container, newResourceSet.getAllResourcesByVisibility()));
        } else {
          // We are not waiting on any resources, so...
          // Kill the current container.
          container.dispatcher.getEventHandler().handle(
              new ContainersLauncherEvent(container,
                  ContainersLauncherEventType.CLEANUP_CONTAINER_FOR_REINIT));
        }
        container.metrics.reInitingContainer();
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.START_CONTAINER_REINIT, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
      } catch (Exception e) {
        LOG.error("Container [" + container.getContainerId() + "]" +
            " re-initialization failure..", e);
        container.addDiagnostics("Error re-initializing due to" +
            "[" + e.getMessage() + "]");
      }
    }

    protected ReInitializationContext createReInitContext(
        ContainerImpl container, ContainerEvent event) {
      ContainerReInitEvent reInitEvent = (ContainerReInitEvent)event;
      if (reInitEvent.getReInitLaunchContext() == null) {
        // This is a Restart...
        // We also need to make sure that if Rollback is possible, the
        // rollback state should be retained in the
        // oldLaunchContext and oldResourceSet
        container.addDiagnostics("Container will be Restarted.\n");
        return new ReInitializationContext(
            container.launchContext, container.resourceSet,
            container.canRollback() ?
                container.reInitContext.oldLaunchContext : null,
            container.canRollback() ?
                container.reInitContext.oldResourceSet : null);
      } else {
        container.addDiagnostics("Container will be Re-initialized.\n");
        return new ReInitializationContext(
            reInitEvent.getReInitLaunchContext(),
            reInitEvent.getResourceSet(),
            // If AutoCommit is turned on, then no rollback can happen...
            // So don't need to store the previous context.
            (reInitEvent.isAutoCommit() ? null : container.launchContext),
            (reInitEvent.isAutoCommit() ? null : container.resourceSet));
      }
    }
  }

  /**
   * Transition to start the Rollback process.
   */
  static class RollbackContainerTransition extends
      ReInitializeContainerTransition {

    @Override
    protected ReInitializationContext createReInitContext(ContainerImpl
        container, ContainerEvent event) {
      container.addDiagnostics("Container upgrade will be Rolled-back.\n");
      LOG.warn("Container [" + container.getContainerId() + "]" +
          " about to be explicitly Rolledback !!");
      return container.reInitContext.createContextForRollback();
    }
  }

  /**
   * Resource requested for Container Re-initialization has been localized.
   * If all dependencies are met, then restart Container with new bits.
   */
  static class ResourceLocalizedWhileReInitTransition
      extends ContainerTransition {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent =
          (ContainerResourceLocalizedEvent) event;
      container.reInitContext.newResourceSet.resourceLocalized(
          rsrcEvent.getResource(), rsrcEvent.getLocation());
      // Check if all ResourceLocalization has completed
      if (container.reInitContext.newResourceSet.getPendingResources()
          .isEmpty()) {
        // Kill the current container.
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER_FOR_REINIT));
      }
    }
  }

  /**
   * Resource is localized while the container is running - create symlinks.
   */
  static class ResourceLocalizedWhileRunningTransition
      extends ContainerTransition {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent =
          (ContainerResourceLocalizedEvent) event;
      Set<String> links = container.resourceSet.resourceLocalized(
          rsrcEvent.getResource(), rsrcEvent.getLocation());
      if (links == null) {
        return;
      }
      // creating symlinks.
      for (String link : links) {
        try {
          String linkFile = new Path(container.workDir, link).toString();
          if (new File(linkFile).exists()) {
            LOG.info("Symlink file already exists: " + linkFile);
          } else {
            container.context.getContainerExecutor()
                .symLink(rsrcEvent.getLocation().toString(), linkFile);
            LOG.info("Created symlink: " + linkFile + " -> " + rsrcEvent
                .getLocation());
          }
        } catch (IOException e) {
          String message = String
              .format("Error when creating symlink %s -> %s", link,
                  rsrcEvent.getLocation());
          LOG.error(message, e);
        }
      }
    }
  }

  /**
   * Resource localization failed while the container is running.
   */
  static class ResourceLocalizationFailedWhileRunningTransition
      extends ContainerTransition {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceFailedEvent failedEvent =
          (ContainerResourceFailedEvent) event;
      container.resourceSet
          .resourceLocalizationFailed(failedEvent.getResource());
      container.addDiagnostics(failedEvent.getDiagnosticMessage());
    }
  }

  /**
   * Resource localization failed while the container is reinitializing.
   */
  static class ResourceLocalizationFailedWhileReInitTransition
      extends ContainerTransition {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceFailedEvent failedEvent =
          (ContainerResourceFailedEvent) event;
      container.resourceSet.resourceLocalizationFailed(
          failedEvent.getResource());
      container.addDiagnostics("Container aborting re-initialization.. "
          + failedEvent.getDiagnosticMessage());
      LOG.error("Container [" + container.getContainerId() + "] Re-init" +
          " failed !! Resource [" + failedEvent.getResource() + "] could" +
          " not be localized !!");
      container.reInitContext = null;
    }
  }

  /**
   * Transition from SCHEDULED state to RUNNING state upon receiving
   * a CONTAINER_LAUNCHED event.
   */
  static class LaunchTransition extends ContainerTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.sendContainerMonitorStartEvent();
      container.metrics.runningContainer();
      container.wasLaunched  = true;

      if (container.isReInitializing()) {
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.FINISH_CONTAINER_REINIT, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
      }
      container.setIsReInitializing(false);
      // Check if this launch was due to a re-initialization.
      // If autocommit == true, then wipe the re-init context. This ensures
      // that any subsequent failures do not trigger a rollback.
      if (container.reInitContext != null
          && !container.reInitContext.canRollback()) {
        container.reInitContext = null;
      }

      if (container.recoveredAsKilled) {
        LOG.info("Killing " + container.containerId
            + " due to recovered as killed");
        container.addDiagnostics("Container recovered as killed.\n");
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }
    }
  }

  /**
   * Transition from RUNNING or KILLING state to
   * EXITED_WITH_SUCCESS state upon EXITED_WITH_SUCCESS message.
   */
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithSuccessTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithSuccessTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {

      container.setIsReInitializing(false);
      // Set exit code to 0 on success    	
      container.exitCode = 0;
    	
      // TODO: Add containerWorkDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE state upon
   * CONTAINER_EXITED_WITH_FAILURE state.
   **/
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithFailureTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithFailureTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.setIsReInitializing(false);
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = exitEvent.getExitCode();
      if (exitEvent.getDiagnosticInfo() != null) {
        container.addDiagnostics(exitEvent.getDiagnosticInfo(), "\n");
      }

      // TODO: Add containerWorkDir to the deletion service.
      // TODO: Add containerOuputDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE or RELAUNCHING state upon
   * CONTAINER_EXITED_WITH_FAILURE state.
   **/
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class RetryFailureTransition implements
      MultipleArcTransition<ContainerImpl, ContainerEvent, ContainerState> {

    @Override
    public ContainerState transition(final ContainerImpl container,
        ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = exitEvent.getExitCode();
      if (exitEvent.getDiagnosticInfo() != null) {
        if (container.containerRetryContext.getRetryPolicy()
            != ContainerRetryPolicy.NEVER_RETRY) {
          int n = container.containerRetryContext.getMaxRetries()
              - container.remainingRetryAttempts;
          container.addDiagnostics("Diagnostic message from attempt "
              + n + " : ", "\n");
        }
        container.addDiagnostics(exitEvent.getDiagnosticInfo(), "\n");
      }

      if (container.shouldRetry(container.exitCode)) {
        if (container.remainingRetryAttempts > 0) {
          container.remainingRetryAttempts--;
          try {
            container.stateStore.storeContainerRemainingRetryAttempts(
                container.getContainerId(), container.remainingRetryAttempts);
          } catch (IOException e) {
            LOG.warn(
                "Unable to update remainingRetryAttempts in state store for "
                    + container.getContainerId(), e);
          }
        }
        doRelaunch(container, container.remainingRetryAttempts,
            container.containerRetryContext.getRetryInterval());
        return ContainerState.RELAUNCHING;
      } else if (container.canRollback()) {
        // Rollback is possible only if the previous launch context is
        // available.
        container.addDiagnostics("Container Re-init Auto Rolled-Back.");
        LOG.info("Rolling back Container reInitialization for [" +
            container.getContainerId() + "] !!");
        container.reInitContext =
            container.reInitContext.createContextForRollback();
        container.metrics.rollbackContainerOnFailure();
        container.metrics.reInitingContainer();
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.START_CONTAINER_REINIT, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
        new KilledForReInitializationTransition().transition(container, event);
        return ContainerState.SCHEDULED;
      } else {
        new ExitedWithFailureTransition(true).transition(container, event);
        return ContainerState.EXITED_WITH_FAILURE;
      }
    }

    private void doRelaunch(final ContainerImpl container,
        int remainingRetryAttempts, final int retryInterval) {
      LOG.info("Relaunching Container " + container.getContainerId()
          + ". Remaining retry attempts(after relaunch) : "
          + remainingRetryAttempts + ". Interval between retries is "
          + retryInterval + "ms");
      container.wasLaunched  = false;
      container.metrics.endRunningContainer();
      if (retryInterval == 0) {
        container.sendRelaunchEvent();
      } else {
        // wait for some time, then send launch event
        new Thread() {
          @Override
          public void run() {
            try {
              Thread.sleep(retryInterval);
              container.sendRelaunchEvent();
            } catch (InterruptedException e) {
              return;
            }
          }
        }.start();
      }
    }
  }

  @Override
  public boolean isRetryContextSet() {
    return containerRetryContext.getRetryPolicy()
        != ContainerRetryPolicy.NEVER_RETRY;
  }

  @Override
  public boolean shouldRetry(int errorCode) {
    return shouldRetry(errorCode, containerRetryContext,
        remainingRetryAttempts);
  }

  public static boolean shouldRetry(int errorCode,
      ContainerRetryContext retryContext, int remainingRetryAttempts) {
    if (errorCode == ExitCode.SUCCESS.getExitCode()
        || errorCode == ExitCode.FORCE_KILLED.getExitCode()
        || errorCode == ExitCode.TERMINATED.getExitCode()) {
      return false;
    }

    ContainerRetryPolicy retryPolicy = retryContext.getRetryPolicy();
    if (retryPolicy == ContainerRetryPolicy.RETRY_ON_ALL_ERRORS
        || (retryPolicy == ContainerRetryPolicy.RETRY_ON_SPECIFIC_ERROR_CODES
        && retryContext.getErrorCodes() != null
        && retryContext.getErrorCodes().contains(errorCode))) {
      return remainingRetryAttempts > 0
          || remainingRetryAttempts == ContainerRetryContext.RETRY_FOREVER;
    }

    return false;
  }
  /**
   * Transition to EXITED_WITH_FAILURE
   */
  static class KilledExternallyTransition extends ExitedWithFailureTransition {
    KilledExternallyTransition() {
      super(true);
    }

    @Override
    public void transition(ContainerImpl container,
        ContainerEvent event) {
      super.transition(container, event);
      container.addDiagnostics("Killed by external signal\n");
    }
  }

  /**
   * Transition to SCHEDULED and wait for RE-LAUNCH
   */
  static class KilledForReInitializationTransition extends ContainerTransition {

    @Override
    public void transition(ContainerImpl container,
        ContainerEvent event) {
      LOG.info("Relaunching Container [" + container.getContainerId()
          + "] for re-initialization !!");
      container.wasLaunched  = false;
      container.metrics.endRunningContainer();

      container.launchContext = container.reInitContext.newLaunchContext;

      // Re configure the Retry Context
      container.containerRetryContext =
          configureRetryContext(container.context.getConf(),
              container.launchContext, container.containerId);
      // Reset the retry attempts since its a fresh start
      container.remainingRetryAttempts =
          container.containerRetryContext.getMaxRetries();

      container.resourceSet =
          container.reInitContext.mergedResourceSet(container.resourceSet);
      container.isMarkeForKilling = false;
      // Ensure Resources are decremented.
      container.dispatcher.getEventHandler().handle(
          new ContainerSchedulerEvent(container,
          ContainerSchedulerEventType.CONTAINER_COMPLETED));
      container.sendScheduleEvent();
    }
  }

  /**
   * Transition from LOCALIZING to LOCALIZATION_FAILED upon receiving
   * RESOURCE_FAILED event.
   */
  static class ResourceFailedTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {

      ContainerResourceFailedEvent rsrcFailedEvent =
          (ContainerResourceFailedEvent) event;
      container.addDiagnostics(rsrcFailedEvent.getDiagnosticMessage(), "\n");

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
    }
  }

  /**
   * Transition from LOCALIZING to KILLING upon receiving
   * KILL_CONTAINER event.
   */
  static class KillBeforeRunningTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.exitCode = killEvent.getContainerExitStatus();
      container.addDiagnostics(killEvent.getDiagnostic(), "\n");
      container.addDiagnostics("Container is killed before being launched.\n");
    }
  }

  /**
   * Remain in KILLING state when receiving a RESOURCE_LOCALIZED request
   * while in the process of killing.
   */
  static class LocalizedResourceDuringKillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent =
          (ContainerResourceLocalizedEvent) event;
      container.resourceSet
          .resourceLocalized(rsrcEvent.getResource(), rsrcEvent.getLocation());
    }
  }

  /**
   * Transitions upon receiving KILL_CONTAINER.
   * - SCHEDULED -> KILLING.
   * - RUNNING -> KILLING.
   * - REINITIALIZING -> KILLING.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class KillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Kill the process/process-grp
      container.setIsReInitializing(false);
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.CLEANUP_CONTAINER));
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.addDiagnostics(killEvent.getDiagnostic(), "\n");
      container.exitCode = killEvent.getContainerExitStatus();
    }
  }

  /**
   * Transition from KILLING to CONTAINER_CLEANEDUP_AFTER_KILL
   * upon receiving CONTAINER_KILLED_ON_REQUEST.
   */
  static class ContainerKilledTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      if (container.hasDefaultExitCode()) {
        container.exitCode = exitEvent.getExitCode();
      }

      if (exitEvent.getDiagnosticInfo() != null) {
        container.addDiagnostics(exitEvent.getDiagnosticInfo(), "\n");
      }

      // The process/process-grp is killed. Decrement reference counts and
      // cleanup resources
      container.cleanup();
    }
  }

  /**
   * Handle the following transitions:
   * - {LOCALIZATION_FAILED, EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE,
   *    KILLING, CONTAINER_CLEANEDUP_AFTER_KILL}
   *   -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ContainerDoneTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    @SuppressWarnings("unchecked")
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.releaseContainer(
          container.containerTokenIdentifier.getResource());
      if (container.containerMetrics != null) {
        container.containerMetrics
            .recordFinishTimeAndExitCode(clock.getTime(), container.exitCode);
        container.containerMetrics.finished();
      }
      container.sendFinishedEvents();

      // if the current state is NEW it means the CONTAINER_INIT was never
      // sent for the event, thus no need to send the CONTAINER_STOP
      if (container.getCurrentState()
          != org.apache.hadoop.yarn.api.records.ContainerState.NEW) {
        container.dispatcher.getEventHandler().handle(new AuxServicesEvent
            (AuxServicesEventType.CONTAINER_STOP, container));
      }
      container.context.getNodeStatusUpdater().sendOutofBandHeartBeat();
    }
  }

  /**
   * Handle the following transition:
   * - NEW -> DONE upon KILL_CONTAINER
   */
  static class KillOnNewTransition extends ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.recoveredStatus == RecoveredContainerStatus.COMPLETED) {
        container.sendFinishedEvents();
      } else {
        ContainerKillEvent killEvent = (ContainerKillEvent) event;
        container.exitCode = killEvent.getContainerExitStatus();
        container.addDiagnostics(killEvent.getDiagnostic(), "\n");
        container.addDiagnostics("Container is killed before being launched.\n");
        container.metrics.killedContainer();
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
        super.transition(container, event);
      }
    }
  }

  /**
   * Handle the following transition:
   * - LOCALIZATION_FAILED -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class LocalizationFailedToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.failedContainer();
      NMAuditLogger.logFailure(container.user,
          AuditConstants.FINISH_FAILED_CONTAINER, "ContainerImpl",
          "Container failed with state: " + container.getContainerState(),
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - EXITED_WITH_SUCCESS -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ExitedWithSuccessToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.wasLaunched) {
        container.metrics.endRunningContainer();
      } else {
        LOG.warn("Container exited with success despite being killed and not" +
            "actually running");
      }
      container.metrics.completedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_SUCCESS_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - EXITED_WITH_FAILURE -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ExitedWithFailureToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.wasLaunched) {
        container.metrics.endRunningContainer();
      }
      container.metrics.failedContainer();
      NMAuditLogger.logFailure(container.user,
          AuditConstants.FINISH_FAILED_CONTAINER, "ContainerImpl",
          "Container failed with state: " + container.getContainerState(),
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - KILLING -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class KillingToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.killedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * CONTAINER_CLEANEDUP_AFTER_KILL -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ContainerCleanedupAfterKillToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.wasLaunched) {
        container.metrics.endRunningContainer();
      }
      container.metrics.killedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Update diagnostics, staying in the same state.
   */
  static class ContainerDiagnosticsUpdateTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerDiagnosticsUpdateEvent updateEvent =
          (ContainerDiagnosticsUpdateEvent) event;
      container.addDiagnostics(updateEvent.getDiagnosticsUpdate(), "\n");
    }
  }

  @Override
  public void handle(ContainerEvent event) {
    try {
      this.writeLock.lock();

      ContainerId containerID = event.getContainerID();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing " + containerID + " of type " + event.getType());
      }
      ContainerState oldState = stateMachine.getCurrentState();
      ContainerState newState = null;
      try {
        newState =
            stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitionException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]," +
            " container: [" + containerID + "]", e);
      }
      if (newState != null && oldState != newState) {
        LOG.info("Container " + containerID + " transitioned from "
            + oldState
            + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    this.readLock.lock();
    try {
      return this.containerId.toString();
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean hasDefaultExitCode() {
    return (this.exitCode == ContainerExitStatus.INVALID);
  }

  /**
   * Returns whether the specific resource should be uploaded to the shared
   * cache.
   */
  private static boolean shouldBeUploadedToSharedCache(ContainerImpl container,
      LocalResourceRequest resource) {
    return container.resourceSet.getResourcesUploadPolicies().get(resource);
  }

  @VisibleForTesting
  ContainerRetryContext getContainerRetryContext() {
    return containerRetryContext;
  }

  @Override
  public Priority getPriority() {
    return containerTokenIdentifier.getPriority();
  }

  @Override
  public boolean isRunning() {
    return getContainerState() == ContainerState.RUNNING;
  }

  @Override
  public void setIsReInitializing(boolean isReInitializing) {
    if (this.isReInitializing && !isReInitializing) {
      metrics.endReInitingContainer();
    }
    this.isReInitializing = isReInitializing;
  }

  @Override
  public boolean isReInitializing() {
    return this.isReInitializing;
  }

  @Override
  public boolean isMarkedForKilling() {
    return this.isMarkeForKilling;
  }

  @Override
  public boolean canRollback() {
    return (this.reInitContext != null)
        && (this.reInitContext.canRollback());
  }

  @Override
  public void commitUpgrade() {
    this.reInitContext = null;
  }

  @Override
  public boolean isRecovering() {
    boolean isRecovering = (
        recoveredStatus != RecoveredContainerStatus.REQUESTED &&
        getContainerState() == ContainerState.NEW);
    return isRecovering;
  }
}
