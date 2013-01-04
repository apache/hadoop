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

package org.apache.hadoop.yarn.server.resourcemanager;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;

import com.google.common.annotations.VisibleForTesting;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources are belong to us..."
 *
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService implements Recoverable {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  public static final long clusterTimeStamp = System.currentTimeMillis();

  protected ClientToAMTokenSecretManagerInRM clientToAMSecretManager =
      new ClientToAMTokenSecretManagerInRM();
  
  protected RMContainerTokenSecretManager containerTokenSecretManager;

  protected ApplicationTokenSecretManager appTokenSecretManager;

  private Dispatcher rmDispatcher;

  protected ResourceScheduler scheduler;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  private ApplicationMasterLauncher applicationMasterLauncher;
  private AdminService adminService;
  private ContainerAllocationExpirer containerAllocationExpirer;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;
  private EventHandler<SchedulerEvent> schedulerDispatcher;
  protected RMAppManager rmAppManager;
  protected ApplicationACLsManager applicationACLsManager;
  protected RMDelegationTokenSecretManager rmDTSecretManager;
  private WebApp webApp;
  protected RMContext rmContext;
  protected ResourceTrackerService resourceTracker;
  private boolean recoveryEnabled;

  private Configuration conf;
  
  public ResourceManager() {
    super("ResourceManager");
  }
  
  public RMContext getRMContext() {
    return this.rmContext;
  }
  
  @Override
  public synchronized void init(Configuration conf) {

    this.conf = conf;

    this.conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    this.rmDispatcher = createDispatcher();
    addIfService(this.rmDispatcher);

    this.appTokenSecretManager = createApplicationTokenSecretManager(conf);

    this.containerAllocationExpirer = new ContainerAllocationExpirer(
        this.rmDispatcher);
    addService(this.containerAllocationExpirer);

    AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
    addService(amLivelinessMonitor);

    AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
    addService(amFinishingMonitor);

    DelegationTokenRenewer tokenRenewer = createDelegationTokenRenewer();
    addService(tokenRenewer);

    this.containerTokenSecretManager = createContainerTokenSecretManager(conf);
    
    boolean isRecoveryEnabled = conf.getBoolean(
        YarnConfiguration.RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);
    
    RMStateStore rmStore = null;
    if(isRecoveryEnabled) {
      recoveryEnabled = true;
      rmStore =  RMStateStoreFactory.getStore(conf);
    } else {
      recoveryEnabled = false;
      rmStore = new NullRMStateStore();
    }
    try {
      rmStore.init(conf);
      rmStore.setDispatcher(rmDispatcher);
    } catch (Exception e) {
      // the Exception from stateStore.init() needs to be handled for 
      // HA and we need to give up master status if we got fenced
      LOG.error("Failed to init state store", e);
      ExitUtil.terminate(1, e);
    }
    
    this.rmContext =
        new RMContextImpl(this.rmDispatcher, rmStore,
          this.containerAllocationExpirer, amLivelinessMonitor,
          amFinishingMonitor, tokenRenewer, this.appTokenSecretManager,
          this.containerTokenSecretManager, this.clientToAMSecretManager);
    
    // Register event handler for NodesListManager
    this.nodesListManager = new NodesListManager(this.rmContext);
    this.rmDispatcher.register(NodesListManagerEventType.class, 
        this.nodesListManager);
    addService(nodesListManager);

    // Initialize the scheduler
    this.scheduler = createScheduler();
    this.schedulerDispatcher = createSchedulerEventDispatcher();
    addIfService(this.schedulerDispatcher);
    this.rmDispatcher.register(SchedulerEventType.class,
        this.schedulerDispatcher);

    // Register event handler for RmAppEvents
    this.rmDispatcher.register(RMAppEventType.class,
        new ApplicationEventDispatcher(this.rmContext));

    // Register event handler for RmAppAttemptEvents
    this.rmDispatcher.register(RMAppAttemptEventType.class,
        new ApplicationAttemptEventDispatcher(this.rmContext));

    // Register event handler for RmNodes
    this.rmDispatcher.register(RMNodeEventType.class,
        new NodeEventDispatcher(this.rmContext));    

    this.nmLivelinessMonitor = createNMLivelinessMonitor();
    addService(this.nmLivelinessMonitor);

    this.resourceTracker = createResourceTrackerService();
    addService(resourceTracker);
  
    try {
      this.scheduler.reinitialize(conf, this.rmContext);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to initialize scheduler", ioe);
    }

    masterService = createApplicationMasterService();
    addService(masterService) ;

    this.applicationACLsManager = new ApplicationACLsManager(conf);

    this.rmAppManager = createRMAppManager();
    // Register event handler for RMAppManagerEvents
    this.rmDispatcher.register(RMAppManagerEventType.class,
        this.rmAppManager);
    this.rmDTSecretManager = createRMDelegationTokenSecretManager();
    clientRM = createClientRMService();
    addService(clientRM);
    
    adminService = createAdminService(clientRM, masterService, resourceTracker);
    addService(adminService);

    this.applicationMasterLauncher = createAMLauncher();
    this.rmDispatcher.register(AMLauncherEventType.class, 
        this.applicationMasterLauncher);

    addService(applicationMasterLauncher);

    new RMNMInfo(this.rmContext, this.scheduler);
    
    super.init(conf);
  }
  
  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setDispatcher(rmDispatcher);
    ((RMContextImpl) rmContext).setStateStore(rmStore);
  }

  protected RMContainerTokenSecretManager createContainerTokenSecretManager(
      Configuration conf) {
    return new RMContainerTokenSecretManager(conf);
  }

  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new SchedulerEventDispatcher(this.scheduler);
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  protected ApplicationTokenSecretManager createApplicationTokenSecretManager(
      Configuration conf) {
    return new ApplicationTokenSecretManager(conf);
  }

  protected ResourceScheduler createScheduler() {
    String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
        YarnConfiguration.DEFAULT_RM_SCHEDULER);
    LOG.info("Using Scheduler: " + schedulerClassName);
    try {
      Class<?> schedulerClazz = Class.forName(schedulerClassName);
      if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
        return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
            this.conf);
      } else {
        throw new YarnException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
  }
  
  protected DelegationTokenRenewer createDelegationTokenRenewer() {
    return new DelegationTokenRenewer();
  }

  protected RMAppManager createRMAppManager() {
    return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
      this.applicationACLsManager, this.conf);
  }

  @Private
  public static class SchedulerEventDispatcher extends AbstractService
      implements EventHandler<SchedulerEvent> {

    private final ResourceScheduler scheduler;
    private final BlockingQueue<SchedulerEvent> eventQueue =
      new LinkedBlockingQueue<SchedulerEvent>();
    private final Thread eventProcessor;
    private volatile boolean stopped = false;
    private boolean shouldExitOnError = false;

    public SchedulerEventDispatcher(ResourceScheduler scheduler) {
      super(SchedulerEventDispatcher.class.getName());
      this.scheduler = scheduler;
      this.eventProcessor = new Thread(new EventProcessor());
      this.eventProcessor.setName("ResourceManager Event Processor");
    }

    @Override
    public synchronized void init(Configuration conf) {
      this.shouldExitOnError =
          conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
      super.init(conf);
    }

    @Override
    public synchronized void start() {
      this.eventProcessor.start();
      super.start();
    }

    private final class EventProcessor implements Runnable {
      @Override
      public void run() {

        SchedulerEvent event;

        while (!stopped && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            LOG.error("Returning, interrupted : " + e);
            return; // TODO: Kill RM.
          }

          try {
            scheduler.handle(event);
          } catch (Throwable t) {
            // An error occurred, but we are shutting down anyway.
            // If it was an InterruptedException, the very act of 
            // shutdown could have caused it and is probably harmless.
            if (stopped) {
              LOG.warn("Exception during shutdown: ", t);
              break;
            }
            LOG.fatal("Error in handling event type " + event.getType()
                + " to the scheduler", t);
            if (shouldExitOnError
                && !ShutdownHookManager.get().isShutdownInProgress()) {
              LOG.info("Exiting, bbye..");
              System.exit(-1);
            }
          }
        }
      }
    }

    @Override
    public synchronized void stop() {
      this.stopped = true;
      this.eventProcessor.interrupt();
      try {
        this.eventProcessor.join();
      } catch (InterruptedException e) {
        throw new YarnException(e);
      }
      super.stop();
    }

    @Override
    public void handle(SchedulerEvent event) {
      try {
        int qSize = eventQueue.size();
        if (qSize !=0 && qSize %1000 == 0) {
          LOG.info("Size of scheduler event-queue is " + qSize);
        }
        int remCapacity = eventQueue.remainingCapacity();
        if (remCapacity < 1000) {
          LOG.info("Very low remaining capacity on scheduler event queue: "
              + remCapacity);
        }
        this.eventQueue.put(event);
      } catch (InterruptedException e) {
        throw new YarnException(e);
      }
    }
  }

  @Private
  public static final class ApplicationEventDispatcher implements
      EventHandler<RMAppEvent> {

    private final RMContext rmContext;

    public ApplicationEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppEvent event) {
      ApplicationId appID = event.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appID);
      if (rmApp != null) {
        try {
          rmApp.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for application " + appID, t);
        }
      }
    }
  }

  @Private
  public static final class ApplicationAttemptEventDispatcher implements
      EventHandler<RMAppAttemptEvent> {

    private final RMContext rmContext;

    public ApplicationAttemptEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMAppAttemptEvent event) {
      ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
      ApplicationId appAttemptId = appAttemptID.getApplicationId();
      RMApp rmApp = this.rmContext.getRMApps().get(appAttemptId);
      if (rmApp != null) {
        RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptID);
        if (rmAppAttempt != null) {
          try {
            rmAppAttempt.handle(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " for applicationAttempt " + appAttemptId, t);
          }
        }
      }
    }
  }

  @Private
  public static final class NodeEventDispatcher implements
      EventHandler<RMNodeEvent> {

    private final RMContext rmContext;

    public NodeEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMNodeEvent event) {
      NodeId nodeId = event.getNodeId();
      RMNode node = this.rmContext.getRMNodes().get(nodeId);
      if (node != null) {
        try {
          ((EventHandler<RMNodeEvent>) node).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for node " + nodeId, t);
        }
      }
    }
  }
  
  protected void startWepApp() {
    Builder<ApplicationMasterService> builder = 
      WebApps.$for("cluster", ApplicationMasterService.class, masterService, "ws").at(
          this.conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS)); 
    String proxyHostAndPort = YarnConfiguration.getProxyHostAndPort(conf);
    if(YarnConfiguration.getRMWebAppHostAndPort(conf).
        equals(proxyHostAndPort)) {
      AppReportFetcher fetcher = new AppReportFetcher(conf, getClientRMService());
      builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME, 
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
      String[] proxyParts = proxyHostAndPort.split(":");
      builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);

    }
    webApp = builder.start(new RMWebApp(this));
  }

  @Override
  public void start() {
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnException("Failed to login", ie);
    }

    this.appTokenSecretManager.start();
    this.containerTokenSecretManager.start();

    if(recoveryEnabled) {
      try {
        RMStateStore rmStore = rmContext.getStateStore();
        RMState state = rmStore.loadState();
        recover(state);
      } catch (Exception e) {
        // the Exception from loadState() needs to be handled for 
        // HA and we need to give up master status if we got fenced
        LOG.error("Failed to load/recover state", e);
        ExitUtil.terminate(1, e);
      }
    }

    startWepApp();
    DefaultMetricsSystem.initialize("ResourceManager");
    JvmMetrics.initSingleton("ResourceManager", null);
    try {
      rmDTSecretManager.startThreads();
    } catch(IOException ie) {
      throw new YarnException("Failed to start secret manager threads", ie);
    }

    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      String hostname = getConfig().get(YarnConfiguration.RM_WEBAPP_ADDRESS,
                                        YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
      hostname = (hostname.contains(":")) ? hostname.substring(0, hostname.indexOf(":")) : hostname;
      int port = webApp.port();
      String resolvedAddress = hostname + ":" + port;
      conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, resolvedAddress);
    }
    
    super.start();

    /*synchronized(shutdown) {
      try {
        while(!shutdown.get()) {
          shutdown.wait();
        }
      } catch(InterruptedException ie) {
        LOG.info("Interrupted while waiting", ie);
      }
    }*/
  }
  
  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
        YarnConfiguration.RM_PRINCIPAL);
  }

  @Override
  public void stop() {
    if (webApp != null) {
      webApp.stop();
    }
    rmDTSecretManager.stopThreads();

    this.appTokenSecretManager.stop();
    this.containerTokenSecretManager.stop();

    /*synchronized(shutdown) {
      shutdown.set(true);
      shutdown.notifyAll();
    }*/

    DefaultMetricsSystem.shutdown();

    RMStateStore store = rmContext.getStateStore();
    try {
      store.close();
    } catch (Exception e) {
      LOG.error("Error closing store.", e);
    }
      
    super.stop();
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor, this.containerTokenSecretManager);
  }

  protected RMDelegationTokenSecretManager
               createRMDelegationTokenSecretManager() {
    long secretKeyInterval = 
        conf.getLong(YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_KEY, 
            YarnConfiguration.DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_KEY,
            YarnConfiguration.DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
        conf.getLong(YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_KEY, 
            YarnConfiguration.DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);

    return new RMDelegationTokenSecretManager(secretKeyInterval, 
        tokenMaxLifetime, tokenRenewInterval, 3600000);
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.rmDTSecretManager);
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(this.rmContext, scheduler);
  }
  

  protected AdminService createAdminService(
      ClientRMService clientRMService, 
      ApplicationMasterService applicationMasterService,
      ResourceTrackerService resourceTrackerService) {
    return new AdminService(this.conf, scheduler, rmContext,
        this.nodesListManager, clientRMService, applicationMasterService,
        resourceTrackerService);
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }
  
  /**
   * return the scheduler.
   * @return the scheduler for the Resource Manager.
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return the resource tracking component.
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Private
  public ApplicationACLsManager getApplicationACLsManager() {
    return this.applicationACLsManager;
  }

  @Private
  public RMContainerTokenSecretManager getRMContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }

  @Private
  public ApplicationTokenSecretManager getApplicationTokenSecretManager(){
    return this.appTokenSecretManager;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover applications
    rmAppManager.recover(state);
  }
  
  public static void main(String argv[]) {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
    try {
      Configuration conf = new YarnConfiguration();
      ResourceManager resourceManager = new ResourceManager();
      ShutdownHookManager.get().addShutdownHook(
        new CompositeServiceShutdownHook(resourceManager),
        SHUTDOWN_HOOK_PRIORITY);
      resourceManager.init(conf);
      resourceManager.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting ResourceManager", t);
      System.exit(-1);
    }
  }
}
