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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMLeveldbStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

import com.google.common.annotations.VisibleForTesting;

public class NodeManager extends CompositeService 
    implements EventHandler<NodeManagerEvent> {

  /**
   * Priority of the NodeManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(NodeManager.class);
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();
  private ApplicationACLsManager aclsManager;
  private NodeHealthCheckerService nodeHealthChecker;
  private LocalDirsHandlerService dirsHandler;
  private Context context;
  private AsyncDispatcher dispatcher;
  private ContainerManagerImpl containerManager;
  private NodeStatusUpdater nodeStatusUpdater;
  private static CompositeServiceShutdownHook nodeManagerShutdownHook; 
  private NMStateStoreService nmStore = null;
  
  private AtomicBoolean isStopping = new AtomicBoolean(false);
  private boolean rmWorkPreservingRestartEnabled;
  private boolean shouldExitOnShutdownEvent = false;

  public NodeManager() {
    super(NodeManager.class.getName());
  }

  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
      metrics);
  }

  protected NodeResourceMonitor createNodeResourceMonitor() {
    return new NodeResourceMonitorImpl();
  }

  protected ContainerManagerImpl createContainerManager(Context context,
      ContainerExecutor exec, DeletionService del,
      NodeStatusUpdater nodeStatusUpdater, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
      metrics, aclsManager, dirsHandler);
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
      NMStateStoreService stateStore) {
    return new NMContext(containerTokenSecretManager, nmTokenSecretManager,
        dirsHandler, aclsManager, stateStore);
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

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    rmWorkPreservingRestartEnabled = conf.getBoolean(YarnConfiguration
            .RM_WORK_PRESERVING_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);

    initAndStartRecoveryStore(conf);

    NMContainerTokenSecretManager containerTokenSecretManager =
        new NMContainerTokenSecretManager(conf, nmStore);

    NMTokenSecretManagerInNM nmTokenSecretManager =
        new NMTokenSecretManagerInNM(nmStore);

    recoverTokens(nmTokenSecretManager, containerTokenSecretManager);
    
    this.aclsManager = new ApplicationACLsManager(conf);

    ContainerExecutor exec = ReflectionUtils.newInstance(
        conf.getClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
          DefaultContainerExecutor.class, ContainerExecutor.class), conf);
    try {
      exec.init();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to initialize container executor", e);
    }    
    DeletionService del = createDeletionService(exec);
    addService(del);

    // NodeManager level dispatcher
    this.dispatcher = new AsyncDispatcher();

    nodeHealthChecker = new NodeHealthCheckerService();
    addService(nodeHealthChecker);
    dirsHandler = nodeHealthChecker.getDiskHandler();

    this.context = createNMContext(containerTokenSecretManager,
        nmTokenSecretManager, nmStore);
    
    nodeStatusUpdater =
        createNodeStatusUpdater(context, dispatcher, nodeHealthChecker);

    NodeResourceMonitor nodeResourceMonitor = createNodeResourceMonitor();
    addService(nodeResourceMonitor);

    containerManager =
        createContainerManager(context, exec, del, nodeStatusUpdater,
        this.aclsManager, dirsHandler);
    addService(containerManager);
    ((NMContext) context).setContainerManager(containerManager);

    WebServer webServer = createWebServer(context, containerManager
        .getContainersMonitor(), this.aclsManager, dirsHandler);
    addService(webServer);
    ((NMContext) context).setWebServer(webServer);

    dispatcher.register(ContainerManagerEventType.class, containerManager);
    dispatcher.register(NodeManagerEventType.class, this);
    addService(dispatcher);
    
    DefaultMetricsSystem.initialize("NodeManager");

    // StatusUpdater should be added last so that it get started last 
    // so that we make sure everything is up before registering with RM. 
    addService(nodeStatusUpdater);
    ((NMContext) context).setNodeStatusUpdater(nodeStatusUpdater);

    super.serviceInit(conf);
    // TODO add local dirs to del
  }

  @Override
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed NodeManager login", e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (isStopping.getAndSet(true)) {
      return;
    }
    try {
      super.serviceStop();
      DefaultMetricsSystem.shutdown();
    } finally {
      // YARN-3641: NM's services stop get failed shouldn't block the
      // release of NMLevelDBStore.
      stopRecoveryStore();
    }
  }

  public String getName() {
    return "NodeManager";
  }

  protected void shutDown() {
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
            ExitUtil.terminate(-1);
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
          LOG.info("Notifying ContainerManager to block new container-requests");
          containerManager.setBlockNewContainerRequests(true);
          if (!rmWorkPreservingRestartEnabled) {
            LOG.info("Cleaning up running containers on resync");
            containerManager.cleanupContainersOnNMResync();
          } else {
            LOG.info("Preserving containers on resync");
          }
          ((NodeStatusUpdaterImpl) nodeStatusUpdater)
            .rebootNodeStatusUpdaterAndRegisterWithRM();
        } catch (YarnRuntimeException e) {
          LOG.fatal("Error while rebooting NodeStatusUpdater.", e);
          shutDown();
        }
      }
    }.start();
  }

  public static class NMContext implements Context {

    private NodeId nodeId = null;
    protected final ConcurrentMap<ApplicationId, Application> applications =
        new ConcurrentHashMap<ApplicationId, Application>();

    private volatile Map<ApplicationId, Credentials> systemCredentials =
        new HashMap<ApplicationId, Credentials>();

    protected final ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentSkipListMap<ContainerId, Container>();

    private final NMContainerTokenSecretManager containerTokenSecretManager;
    private final NMTokenSecretManagerInNM nmTokenSecretManager;
    private ContainerManagementProtocol containerManager;
    private final LocalDirsHandlerService dirsHandler;
    private final ApplicationACLsManager aclsManager;
    private WebServer webServer;
    private final NodeHealthStatus nodeHealthStatus = RecordFactoryProvider
        .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);
    private final NMStateStoreService stateStore;
    private boolean isDecommissioned = false;
    private NodeStatusUpdater nodeStatusUpdater;

    public NMContext(NMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInNM nmTokenSecretManager,
        LocalDirsHandlerService dirsHandler, ApplicationACLsManager aclsManager,
        NMStateStoreService stateStore) {
      this.containerTokenSecretManager = containerTokenSecretManager;
      this.nmTokenSecretManager = nmTokenSecretManager;
      this.dirsHandler = dirsHandler;
      this.aclsManager = aclsManager;
      this.nodeHealthStatus.setIsNodeHealthy(true);
      this.nodeHealthStatus.setHealthReport("Healthy");
      this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
      this.stateStore = stateStore;
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
    public ConcurrentMap<ContainerId, Container> getContainers() {
      return this.containers;
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
    public ContainerManagementProtocol getContainerManager() {
      return this.containerManager;
    }

    public void setContainerManager(ContainerManagementProtocol containerManager) {
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

    public NodeStatusUpdater getNodeStatusUpdater() {
      return this.nodeStatusUpdater;
    }

    public void setNodeStatusUpdater(NodeStatusUpdater nodeStatusUpdater) {
      this.nodeStatusUpdater = nodeStatusUpdater;
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
      LOG.fatal("Error starting NodeManager", t);
      System.exit(-1);
    }
  }

  @Override
  public void handle(NodeManagerEvent event) {
    switch (event.getType()) {
    case SHUTDOWN:
      shutDown();
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

  public static void main(String[] args) throws IOException {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(NodeManager.class, args, LOG);
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
}
