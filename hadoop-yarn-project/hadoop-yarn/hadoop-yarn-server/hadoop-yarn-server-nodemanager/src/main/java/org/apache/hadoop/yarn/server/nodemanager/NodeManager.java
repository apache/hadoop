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

package org.apache.hadoop.yarn.server.nodemanager;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.NodeHealthScriptRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.collectormanager.NMCollectorService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ConfigurationNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.ScriptBasedNodeLabelsProvider;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMLeveldbStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.state.MultiStateTransitionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeManager extends CompositeService 
    implements EventHandler<NodeManagerEvent> {

  /**
   * Node manager return status codes.
   */
  public enum NodeManagerStatus {
    NO_ERROR(0),
    EXCEPTION(1);

    private int exitCode;

    NodeManagerStatus(int exitCode) {
      this.exitCode = exitCode;
    }

    public int getExitCode() {
      return exitCode;
    }
  }

  /**
   * Priority of the NodeManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Logger LOG =
       LoggerFactory.getLogger(NodeManager.class);
  private static long nmStartupTime = System.currentTimeMillis();
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  private JvmPauseMonitor pauseMonitor;
  private ApplicationACLsManager aclsManager;
  private NodeHealthCheckerService nodeHealthChecker;
  private NodeLabelsProvider nodeLabelsProvider;
  private LocalDirsHandlerService dirsHandler;
  private Context context;
  private AsyncDispatcher dispatcher;
  private ContainerManagerImpl containerManager;
  // the NM collector service is set only if the timeline service v.2 is enabled
  private NMCollectorService nmCollectorService;
  private NodeStatusUpdater nodeStatusUpdater;
  private NodeResourceMonitor nodeResourceMonitor;
  private static CompositeServiceShutdownHook nodeManagerShutdownHook;
  private NMStateStoreService nmStore = null;
  
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private boolean rmWorkPreservingRestartEnabled;
  private boolean shouldExitOnShutdownEvent = false;

  private NMLogAggregationStatusTracker nmLogAggregationStatusTracker;
  /**
   * Default Container State transition listener.
   */
  public static class DefaultContainerStateListener extends
      MultiStateTransitionListener
          <ContainerImpl, ContainerEvent, ContainerState>
      implements ContainerStateTransitionListener {
    @Override
    public void init(Context context) {}
  }

  public NodeManager() {
    super(NodeManager.class.getName());
  }

  public static long getNMStartupTime() {
    return nmStartupTime;
  }

  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
        metrics, nodeLabelsProvider);
  }

  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker,
      NodeLabelsProvider nodeLabelsProvider) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
        metrics, nodeLabelsProvider);
  }

  protected NodeLabelsProvider createNodeLabelsProvider(Configuration conf)
      throws IOException {
    NodeLabelsProvider provider = null;
    String providerString =
        conf.get(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG, null);
    if (providerString == null || providerString.trim().length() == 0) {
      // Seems like Distributed Node Labels configuration is not enabled
      return provider;
    }
    switch (providerString.trim().toLowerCase()) {
    case YarnConfiguration.CONFIG_NODE_LABELS_PROVIDER:
      provider = new ConfigurationNodeLabelsProvider();
      break;
    case YarnConfiguration.SCRIPT_NODE_LABELS_PROVIDER:
      provider = new ScriptBasedNodeLabelsProvider();
      break;
    default:
      try {
        Class<? extends NodeLabelsProvider> labelsProviderClass =
            conf.getClass(YarnConfiguration.NM_NODE_LABELS_PROVIDER_CONFIG,
                null, NodeLabelsProvider.class);
        provider = labelsProviderClass.newInstance();
      } catch (InstantiationException | IllegalAccessException
          | RuntimeException e) {
        LOG.error("Failed to create NodeLabelsProvider based on Configuration",
            e);
        throw new IOException(
            "Failed to create NodeLabelsProvider : " + e.getMessage(), e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Distributed Node Labels is enabled"
          + " with provider class as : " + provider.getClass().toString());
    }
    return provider;
  }

  protected NodeResourceMonitor createNodeResourceMonitor() {
    return new NodeResourceMonitorImpl(context);
  }

  protected ContainerManagerImpl createContainerManager(Context context,
      ContainerExecutor exec, DeletionService del,
      NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
        metrics, dirsHandler);
  }

  protected NMCollectorService createNMCollectorService(Context ctxt) {
    return new NMCollectorService(ctxt);
  }

  protected WebServer createWebServer(Context nmContext,
      ResourceView resourceView, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new WebServer(nmContext, resourceView, aclsManager, dirsHandler);
  }

  protected DeletionService createDeletionService(ContainerExecutor exec) {
    return new DeletionService(exec, nmStore);
  }

  protected NMContext createNMContext(
      NMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInNM nmTokenSecretManager,
      NMStateStoreService stateStore, boolean isDistSchedulerEnabled,
      Configuration conf) {
    List<ContainerStateTransitionListener> listeners =
        conf.getInstances(
            YarnConfiguration.NM_CONTAINER_STATE_TRANSITION_LISTENERS,
        ContainerStateTransitionListener.class);
    NMContext nmContext = new NMContext(containerTokenSecretManager,
        nmTokenSecretManager, dirsHandler, aclsManager, stateStore,
        isDistSchedulerEnabled, conf);
    nmContext.setNodeManagerMetrics(metrics);
    DefaultContainerStateListener defaultListener =
        new DefaultContainerStateListener();
    nmContext.setContainerStateTransitionListener(defaultListener);
    defaultListener.init(nmContext);
    for (ContainerStateTransitionListener listener : listeners) {
      listener.init(nmContext);
      defaultListener.addListener(listener);
    }
    return nmContext;
  }

  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(getConfig(), YarnConfiguration.NM_KEYTAB,
        YarnConfiguration.NM_PRINCIPAL);
  }

  private void initAndStartRecoveryStore(Configuration conf)
      throws IOException {
    boolean recoveryEnabled = conf.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    if (recoveryEnabled) {
      FileSystem recoveryFs = FileSystem.getLocal(conf);
      String recoveryDirName = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
      if (recoveryDirName == null) {
        throw new IllegalArgumentException("Recovery is enabled but " +
            YarnConfiguration.NM_RECOVERY_DIR + " is not set.");
      }
      Path recoveryRoot = new Path(recoveryDirName);
      recoveryFs.mkdirs(recoveryRoot, new FsPermission((short)0700));
      nmStore = new NMLeveldbStateStoreService();
    } else {
      nmStore = new NMNullStateStoreService();
    }
    nmStore.init(conf);
    nmStore.start();
  }

  private void stopRecoveryStore() throws IOException {
    if (null != nmStore) {
      nmStore.stop();
      if (null != context) {
        if (context.getDecommissioned() && nmStore.canRecover()) {
          LOG.info("Removing state store due to decommission");
          Configuration conf = getConfig();
          Path recoveryRoot =
              new Path(conf.get(YarnConfiguration.NM_RECOVERY_DIR));
          LOG.info("Removing state store at " + recoveryRoot
              + " due to decommission");
          FileSystem recoveryFs = FileSystem.getLocal(conf);
          if (!recoveryFs.delete(recoveryRoot, true)) {
            LOG.warn("Unable to delete " + recoveryRoot);
          }
        }
      }
    }
  }

  private void recoverTokens(NMTokenSecretManagerInNM nmTokenSecretManager,
      NMContainerTokenSecretManager containerTokenSecretManager)
          throws IOException {
    if (nmStore.canRecover()) {
      nmTokenSecretManager.recover();
      containerTokenSecretManager.recover();
    }
  }

  public static NodeHealthScriptRunner getNodeHealthScriptRunner(Configuration conf) {
    String nodeHealthScript = 
        conf.get(YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH);
    if(!NodeHealthScriptRunner.shouldRun(nodeHealthScript)) {
      LOG.info("Node Manager health check script is not available "
          + "or doesn't have execute permission, so not "
          + "starting the node health script runner.");
      return null;
    }
    long nmCheckintervalTime = conf.getLong(
        YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
    long scriptTimeout = conf.getLong(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS);
    String[] scriptArgs = conf.getStrings(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS, new String[] {});
    return new NodeHealthScriptRunner(nodeHealthScript,
        nmCheckintervalTime, scriptTimeout, scriptArgs);
  }

  @VisibleForTesting
  protected ResourcePluginManager createResourcePluginManager() {
    return new ResourcePluginManager();
  }

  @VisibleForTesting
  protected ContainerExecutor createContainerExecutor(Configuration conf) {
    return ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
            DefaultContainerExecutor.class, ContainerExecutor.class), conf);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    rmWorkPreservingRestartEnabled = conf.getBoolean(YarnConfiguration
            .RM_WORK_PRESERVING_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);

    try {
      initAndStartRecoveryStore(conf);
    } catch (IOException e) {
      String recoveryDirName = conf.get(YarnConfiguration.NM_RECOVERY_DIR);
      throw new
          YarnRuntimeException("Unable to initialize recovery directory at "
              + recoveryDirName, e);
    }

    NMContainerTokenSecretManager containerTokenSecretManager =
        new NMContainerTokenSecretManager(conf, nmStore);

    NMTokenSecretManagerInNM nmTokenSecretManager =
        new NMTokenSecretManagerInNM(nmStore);

    recoverTokens(nmTokenSecretManager, containerTokenSecretManager);
    
    this.aclsManager = new ApplicationACLsManager(conf);

    this.dirsHandler = new LocalDirsHandlerService(metrics);

    boolean isDistSchedulingEnabled =
        conf.getBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED,
            YarnConfiguration.DEFAULT_DIST_SCHEDULING_ENABLED);

    this.context = createNMContext(containerTokenSecretManager,
        nmTokenSecretManager, nmStore, isDistSchedulingEnabled, conf);

    ResourcePluginManager pluginManager = createResourcePluginManager();
    pluginManager.initialize(context);
    ((NMContext)context).setResourcePluginManager(pluginManager);

    ContainerExecutor exec = createContainerExecutor(conf);
    try {
      exec.init(context);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to initialize container executor", e);
    }    
    DeletionService del = createDeletionService(exec);
    addService(del);

    // NodeManager level dispatcher
    this.dispatcher = new AsyncDispatcher("NM Event dispatcher");

    nodeHealthChecker =
        new NodeHealthCheckerService(
            getNodeHealthScriptRunner(conf), dirsHandler);
    addService(nodeHealthChecker);


    ((NMContext)context).setContainerExecutor(exec);
    ((NMContext)context).setDeletionService(del);

    nodeLabelsProvider = createNodeLabelsProvider(conf);

    if (null == nodeLabelsProvider) {
      nodeStatusUpdater =
          createNodeStatusUpdater(context, dispatcher, nodeHealthChecker);
    } else {
      addIfService(nodeLabelsProvider);
      nodeStatusUpdater =
          createNodeStatusUpdater(context, dispatcher, nodeHealthChecker,
              nodeLabelsProvider);
    }

    nodeResourceMonitor = createNodeResourceMonitor();
    addService(nodeResourceMonitor);
    ((NMContext) context).setNodeResourceMonitor(nodeResourceMonitor);

    containerManager =
        createContainerManager(context, exec, del, nodeStatusUpdater,
        this.aclsManager, dirsHandler);
    addService(containerManager);
    ((NMContext) context).setContainerManager(containerManager);

    this.nmLogAggregationStatusTracker = createNMLogAggregationStatusTracker(
        context);
    addService(nmLogAggregationStatusTracker);
    ((NMContext)context).setNMLogAggregationStatusTracker(
        this.nmLogAggregationStatusTracker);

    WebServer webServer = createWebServer(context, containerManager
        .getContainersMonitor(), this.aclsManager, dirsHandler);
    addService(webServer);
    ((NMContext) context).setWebServer(webServer);

    ((NMContext) context).setQueueableContainerAllocator(
        new OpportunisticContainerAllocator(
            context.getContainerTokenSecretManager()));

    dispatcher.register(ContainerManagerEventType.class, containerManager);
    dispatcher.register(NodeManagerEventType.class, this);
    addService(dispatcher);

    pauseMonitor = new JvmPauseMonitor();
    addService(pauseMonitor);
    metrics.getJvmMetrics().setPauseMonitor(pauseMonitor);

    DefaultMetricsSystem.initialize("NodeManager");

    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      this.nmCollectorService = createNMCollectorService(context);
      addService(nmCollectorService);
    }

    // StatusUpdater should be added last so that it get started last 
    // so that we make sure everything is up before registering with RM. 
    addService(nodeStatusUpdater);
    ((NMContext) context).setNodeStatusUpdater(nodeStatusUpdater);
    nmStore.setNodeStatusUpdater(nodeStatusUpdater);

    // Do secure login before calling init for added services.
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed NodeManager login", e);
    }

    super.serviceInit(conf);
    // TODO add local dirs to del
  }

  @Override
  protected void serviceStop() throws Exception {
    if (isStopping.getAndSet(true)) {
      return;
    }
    try {
      super.serviceStop();
      DefaultMetricsSystem.shutdown();

      // Cleanup ResourcePluginManager
      ResourcePluginManager rpm = context.getResourcePluginManager();
      if (rpm != null) {
        rpm.cleanup();
      }
    } finally {
      // YARN-3641: NM's services stop get failed shouldn't block the
      // release of NMLevelDBStore.
      stopRecoveryStore();
    }
  }

  public String getName() {
    return "NodeManager";
  }

  protected void shutDown(final int exitCode) {
    new Thread() {
      @Override
      public void run() {
        try {
          NodeManager.this.stop();
        } catch (Throwable t) {
          LOG.error("Error while shutting down NodeManager", t);
        } finally {
          if (shouldExitOnShutdownEvent
              && !ShutdownHookManager.get().isShutdownInProgress()) {
            ExitUtil.terminate(exitCode);
          }
        }
      }
    }.start();
  }

  protected void resyncWithRM() {
    //we do not want to block dispatcher thread here
    new Thread() {
      @Override
      public void run() {
        try {
          if (!rmWorkPreservingRestartEnabled) {
            LOG.info("Cleaning up running containers on resync");
            containerManager.cleanupContainersOnNMResync();
            // Clear all known collectors for resync.
            if (context.getKnownCollectors() != null) {
              context.getKnownCollectors().clear();
            }
          } else {
            LOG.info("Preserving containers on resync");
            // Re-register known timeline collectors.
            reregisterCollectors();
          }
          ((NodeStatusUpdaterImpl) nodeStatusUpdater)
            .rebootNodeStatusUpdaterAndRegisterWithRM();
        } catch (YarnRuntimeException e) {
          LOG.error("Error while rebooting NodeStatusUpdater.", e);
          shutDown(NodeManagerStatus.EXCEPTION.getExitCode());
        }
      }
    }.start();
  }

  /**
   * Reregisters all collectors known by this node to the RM. This method is
   * called when the RM needs to resync with the node.
   */
  protected void reregisterCollectors() {
    Map<ApplicationId, AppCollectorData> knownCollectors
        = context.getKnownCollectors();
    if (knownCollectors == null) {
      return;
    }
    ConcurrentMap<ApplicationId, AppCollectorData> registeringCollectors
        = context.getRegisteringCollectors();
    for (Map.Entry<ApplicationId, AppCollectorData> entry
        : knownCollectors.entrySet()) {
      Application app = context.getApplications().get(entry.getKey());
      if ((app != null)
          && !ApplicationState.FINISHED.equals(app.getApplicationState())) {
        registeringCollectors.putIfAbsent(entry.getKey(), entry.getValue());
        AppCollectorData data = entry.getValue();
        if (LOG.isDebugEnabled()) {
          LOG.debug(entry.getKey() + " : " + data.getCollectorAddr() + "@<"
              + data.getRMIdentifier() + ", " + data.getVersion() + ">");
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove collector data for done app " + entry.getKey());
        }
      }
    }
    knownCollectors.clear();
  }

  public static class NMContext implements Context {

    private NodeId nodeId = null;

    private Configuration conf = null;

    private NodeManagerMetrics metrics = null;

    protected final ConcurrentMap<ApplicationId, Application> applications =
        new ConcurrentHashMap<ApplicationId, Application>();

    private volatile Map<ApplicationId, Credentials> systemCredentials =
        new HashMap<ApplicationId, Credentials>();

    protected final ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentSkipListMap<ContainerId, Container>();

    private ConcurrentMap<ApplicationId, AppCollectorData>
        registeringCollectors;

    private ConcurrentMap<ApplicationId, AppCollectorData> knownCollectors;

    protected final ConcurrentMap<ContainerId,
        org.apache.hadoop.yarn.api.records.Container> increasedContainers =
            new ConcurrentHashMap<>();

    private final NMContainerTokenSecretManager containerTokenSecretManager;
    private final NMTokenSecretManagerInNM nmTokenSecretManager;
    private ContainerManager containerManager;
    private NodeResourceMonitor nodeResourceMonitor;
    private final LocalDirsHandlerService dirsHandler;
    private final ApplicationACLsManager aclsManager;
    private WebServer webServer;
    private final NodeHealthStatus nodeHealthStatus = RecordFactoryProvider
        .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);
    private final NMStateStoreService stateStore;
    private boolean isDecommissioned = false;
    private final ConcurrentLinkedQueue<LogAggregationReport>
        logAggregationReportForApps;
    private NodeStatusUpdater nodeStatusUpdater;
    private final boolean isDistSchedulingEnabled;
    private DeletionService deletionService;

    private OpportunisticContainerAllocator containerAllocator;

    private ContainerExecutor executor;

    private NMTimelinePublisher nmTimelinePublisher;

    private ContainerStateTransitionListener containerStateTransitionListener;

    private ResourcePluginManager resourcePluginManager;

    private NMLogAggregationStatusTracker nmLogAggregationStatusTracker;

    public NMContext(NMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInNM nmTokenSecretManager,
        LocalDirsHandlerService dirsHandler, ApplicationACLsManager aclsManager,
        NMStateStoreService stateStore, boolean isDistSchedulingEnabled,
        Configuration conf) {
      if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
        this.registeringCollectors = new ConcurrentHashMap<>();
        this.knownCollectors = new ConcurrentHashMap<>();
      }
      this.containerTokenSecretManager = containerTokenSecretManager;
      this.nmTokenSecretManager = nmTokenSecretManager;
      this.dirsHandler = dirsHandler;
      this.aclsManager = aclsManager;
      this.nodeHealthStatus.setIsNodeHealthy(true);
      this.nodeHealthStatus.setHealthReport("Healthy");
      this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
      this.stateStore = stateStore;
      this.logAggregationReportForApps = new ConcurrentLinkedQueue<
          LogAggregationReport>();
      this.isDistSchedulingEnabled = isDistSchedulingEnabled;
      this.conf = conf;
    }

    /**
     * Usable only after ContainerManager is started.
     */
    @Override
    public NodeId getNodeId() {
      return this.nodeId;
    }

    @Override
    public int getHttpPort() {
      return this.webServer.getPort();
    }

    @Override
    public ConcurrentMap<ApplicationId, Application> getApplications() {
      return this.applications;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      return this.containers;
    }

    @Override
    public ConcurrentMap<ContainerId, org.apache.hadoop.yarn.api.records.Container>
        getIncreasedContainers() {
      return this.increasedContainers;
    }

    @Override
    public NMContainerTokenSecretManager getContainerTokenSecretManager() {
      return this.containerTokenSecretManager;
    }
    
    @Override
    public NMTokenSecretManagerInNM getNMTokenSecretManager() {
      return this.nmTokenSecretManager;
    }
    
    @Override
    public NodeHealthStatus getNodeHealthStatus() {
      return this.nodeHealthStatus;
    }

    @Override
    public NodeResourceMonitor getNodeResourceMonitor() {
      return this.nodeResourceMonitor;
    }

    public void setNodeResourceMonitor(NodeResourceMonitor nodeResourceMonitor) {
      this.nodeResourceMonitor = nodeResourceMonitor;
    }

    @Override
    public ContainerManager getContainerManager() {
      return this.containerManager;
    }

    public void setContainerManager(ContainerManager containerManager) {
      this.containerManager = containerManager;
    }

    public void setWebServer(WebServer webServer) {
      this.webServer = webServer;
    }

    public void setNodeId(NodeId nodeId) {
      this.nodeId = nodeId;
    }

    @Override
    public LocalDirsHandlerService getLocalDirsHandler() {
      return dirsHandler;
    }
    
    @Override
    public ApplicationACLsManager getApplicationACLsManager() {
      return aclsManager;
    }

    @Override
    public NMStateStoreService getNMStateStore() {
      return stateStore;
    }

    @Override
    public boolean getDecommissioned() {
      return isDecommissioned;
    }

    @Override
    public void setDecommissioned(boolean isDecommissioned) {
      this.isDecommissioned = isDecommissioned;
    }

    @Override
    public Map<ApplicationId, Credentials> getSystemCredentialsForApps() {
      return systemCredentials;
    }

    public void setSystemCrendentialsForApps(
        Map<ApplicationId, Credentials> systemCredentials) {
      this.systemCredentials = systemCredentials;
    }

    @Override
    public ConcurrentLinkedQueue<LogAggregationReport>
        getLogAggregationStatusForApps() {
      return this.logAggregationReportForApps;
    }

    public NodeStatusUpdater getNodeStatusUpdater() {
      return this.nodeStatusUpdater;
    }

    public void setNodeStatusUpdater(NodeStatusUpdater nodeStatusUpdater) {
      this.nodeStatusUpdater = nodeStatusUpdater;
    }

    public boolean isDistributedSchedulingEnabled() {
      return isDistSchedulingEnabled;
    }

    public void setQueueableContainerAllocator(
        OpportunisticContainerAllocator containerAllocator) {
      this.containerAllocator = containerAllocator;
    }

    @Override
    public OpportunisticContainerAllocator getContainerAllocator() {
      return containerAllocator;
    }

    @Override
    public ConcurrentMap<ApplicationId, AppCollectorData>
        getRegisteringCollectors() {
      return this.registeringCollectors;
    }

    @Override
    public ConcurrentMap<ApplicationId, AppCollectorData> getKnownCollectors() {
      return this.knownCollectors;
    }

    @Override
    public void setNMTimelinePublisher(NMTimelinePublisher nmMetricsPublisher) {
      this.nmTimelinePublisher = nmMetricsPublisher;
    }

    @Override
    public NMTimelinePublisher getNMTimelinePublisher() {
      return nmTimelinePublisher;
    }

    public ContainerExecutor getContainerExecutor() {
      return this.executor;
    }

    public void setContainerExecutor(ContainerExecutor executor) {
      this.executor = executor;
    }

    @Override
    public ContainerStateTransitionListener
        getContainerStateTransitionListener() {
      return this.containerStateTransitionListener;
    }

    public void setContainerStateTransitionListener(
        ContainerStateTransitionListener transitionListener) {
      this.containerStateTransitionListener = transitionListener;
    }

    public ResourcePluginManager getResourcePluginManager() {
      return resourcePluginManager;
    }

    /**
     * Returns the {@link NodeManagerMetrics} instance of this node.
     * This might return a null if the instance was not set to the context.
     * @return node manager metrics.
     */
    @Override
    public NodeManagerMetrics getNodeManagerMetrics() {
      return metrics;
    }

    public void setNodeManagerMetrics(NodeManagerMetrics nmMetrics) {
      this.metrics = nmMetrics;
    }

    public void setResourcePluginManager(
        ResourcePluginManager resourcePluginManager) {
      this.resourcePluginManager = resourcePluginManager;
    }

    /**
     * Return the NM's {@link DeletionService}.
     *
     * @return the NM's {@link DeletionService}.
     */
    public DeletionService getDeletionService() {
      return this.deletionService;
    }

    /**
     * Set the NM's {@link DeletionService}.
     *
     * @param deletionService the {@link DeletionService} to add to the Context.
     */
    public void setDeletionService(DeletionService deletionService) {
      this.deletionService = deletionService;
    }

    public void setNMLogAggregationStatusTracker(
        NMLogAggregationStatusTracker nmLogAggregationStatusTracker) {
      this.nmLogAggregationStatusTracker = nmLogAggregationStatusTracker;
    }
    @Override
    public NMLogAggregationStatusTracker getNMLogAggregationStatusTracker() {
      return nmLogAggregationStatusTracker;
    }
  }

  /**
   * @return the node health checker
   */
  public NodeHealthCheckerService getNodeHealthChecker() {
    return nodeHealthChecker;
  }

  private void initAndStartNodeManager(Configuration conf, boolean hasToReboot) {
    try {
      // Failed to start if we're a Unix based system but we don't have bash.
      // Bash is necessary to launch containers under Unix-based systems.
      if (!Shell.WINDOWS) {
        if (!Shell.checkIsBashSupported()) {
          String message =
              "Failing NodeManager start since we're on a "
                  + "Unix-based system but bash doesn't seem to be available.";
          LOG.error(message);
          throw new YarnRuntimeException(message);
        }
      }

      // Remove the old hook if we are rebooting.
      if (hasToReboot && null != nodeManagerShutdownHook) {
        ShutdownHookManager.get().removeShutdownHook(nodeManagerShutdownHook);
      }

      nodeManagerShutdownHook = new CompositeServiceShutdownHook(this);
      ShutdownHookManager.get().addShutdownHook(nodeManagerShutdownHook,
                                                SHUTDOWN_HOOK_PRIORITY);
      // System exit should be called only when NodeManager is instantiated from
      // main() funtion
      this.shouldExitOnShutdownEvent = true;
      this.init(conf);
      this.start();
    } catch (Throwable t) {
      LOG.error("Error starting NodeManager", t);
      System.exit(-1);
    }
  }

  @Override
  public void handle(NodeManagerEvent event) {
    switch (event.getType()) {
    case SHUTDOWN:
      shutDown(NodeManagerStatus.NO_ERROR.getExitCode());
      break;
    case RESYNC:
      resyncWithRM();
      break;
    default:
      LOG.warn("Invalid shutdown event " + event.getType() + ". Ignoring.");
    }
  }
  
  // For testing
  NodeManager createNewNodeManager() {
    return new NodeManager();
  }
  
  // For testing
  ContainerManagerImpl getContainerManager() {
    return containerManager;
  }
  
  //For testing
  Dispatcher getNMDispatcher(){
    return dispatcher;
  }

  @VisibleForTesting
  public Context getNMContext() {
    return this.context;
  }

  /**
   * Returns the NM collector service. It should be used only for testing
   * purposes.
   *
   * @return the NM collector service, or null if the timeline service v.2 is
   * not enabled
   */
  @VisibleForTesting
  NMCollectorService getNMCollectorService() {
    return this.nmCollectorService;
  }

  public static void main(String[] args) throws IOException {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(NodeManager.class, args, LOG);
    @SuppressWarnings("resource")
    NodeManager nodeManager = new NodeManager();
    Configuration conf = new YarnConfiguration();
    new GenericOptionsParser(conf, args);
    nodeManager.initAndStartNodeManager(conf, false);
  }

  @VisibleForTesting
  @Private
  public NodeStatusUpdater getNodeStatusUpdater() {
    return nodeStatusUpdater;
  }

  private NMLogAggregationStatusTracker createNMLogAggregationStatusTracker(
      Context ctxt) {
    return new NMLogAggregationStatusTracker(ctxt);
  }
}
