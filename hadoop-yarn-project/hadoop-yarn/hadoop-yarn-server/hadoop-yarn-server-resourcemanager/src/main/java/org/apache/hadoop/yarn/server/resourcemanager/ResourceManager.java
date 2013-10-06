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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerPreemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
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
  protected NMTokenSecretManagerInRM nmTokenSecretManager;

  protected AMRMTokenSecretManager amRmTokenSecretManager;

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
  protected QueueACLsManager queueACLsManager;
  protected RMDelegationTokenSecretManager rmDTSecretManager;
  private DelegationTokenRenewer delegationTokenRenewer;
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
  protected void serviceInit(Configuration conf) throws Exception {

    validateConfigs(conf);

    this.conf = conf;

    this.conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    this.rmDispatcher = createDispatcher();
    addIfService(this.rmDispatcher);

    this.amRmTokenSecretManager = createAMRMTokenSecretManager(conf);

    this.containerAllocationExpirer = new ContainerAllocationExpirer(
        this.rmDispatcher);
    addService(this.containerAllocationExpirer);

    AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
    addService(amLivelinessMonitor);

    AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
    addService(amFinishingMonitor);

    this.containerTokenSecretManager = createContainerTokenSecretManager(conf);
    this.nmTokenSecretManager = createNMTokenSecretManager(conf);
    
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
      rmStore.setRMDispatcher(rmDispatcher);
    } catch (Exception e) {
      // the Exception from stateStore.init() needs to be handled for 
      // HA and we need to give up master status if we got fenced
      LOG.error("Failed to init state store", e);
      ExitUtil.terminate(1, e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      this.delegationTokenRenewer = createDelegationTokenRenewer();
    }

    this.rmContext =
        new RMContextImpl(this.rmDispatcher, rmStore,
          this.containerAllocationExpirer, amLivelinessMonitor,
          amFinishingMonitor, delegationTokenRenewer, this.amRmTokenSecretManager,
          this.containerTokenSecretManager, this.nmTokenSecretManager,
          this.clientToAMSecretManager);
    
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

    DefaultMetricsSystem.initialize("ResourceManager");
    JvmMetrics.initSingleton("ResourceManager", null);

    try {
      this.scheduler.reinitialize(conf, this.rmContext);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to initialize scheduler", ioe);
    }

    // creating monitors that handle preemption
    createPolicyMonitors();

    masterService = createApplicationMasterService();
    addService(masterService) ;

    this.applicationACLsManager = new ApplicationACLsManager(conf);

    queueACLsManager = createQueueACLsManager(scheduler, conf);

    this.rmAppManager = createRMAppManager();
    // Register event handler for RMAppManagerEvents
    this.rmDispatcher.register(RMAppManagerEventType.class,
        this.rmAppManager);
    this.rmDTSecretManager = createRMDelegationTokenSecretManager(this.rmContext);
    rmContext.setRMDelegationTokenSecretManager(this.rmDTSecretManager);
    clientRM = createClientRMService();
    rmContext.setClientRMService(clientRM);
    addService(clientRM);
    
    adminService = createAdminService(clientRM, masterService, resourceTracker);
    addService(adminService);

    this.applicationMasterLauncher = createAMLauncher();
    this.rmDispatcher.register(AMLauncherEventType.class, 
        this.applicationMasterLauncher);

    addService(applicationMasterLauncher);
    if (UserGroupInformation.isSecurityEnabled()) {
      addService(delegationTokenRenewer);
      delegationTokenRenewer.setRMContext(rmContext);
    }
    new RMNMInfo(this.rmContext, this.scheduler);
    
    super.serviceInit(conf);
  }
  
  protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler,
      Configuration conf) {
    return new QueueACLsManager(scheduler, conf);
  }

  @VisibleForTesting
  protected void setRMStateStore(RMStateStore rmStore) {
    rmStore.setRMDispatcher(rmDispatcher);
    ((RMContextImpl) rmContext).setStateStore(rmStore);
  }

  protected RMContainerTokenSecretManager createContainerTokenSecretManager(
      Configuration conf) {
    return new RMContainerTokenSecretManager(conf);
  }

  protected NMTokenSecretManagerInRM createNMTokenSecretManager(
      Configuration conf) {
    return new NMTokenSecretManagerInRM(conf);
  }
  
  protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
    return new SchedulerEventDispatcher(this.scheduler);
  }

  protected Dispatcher createDispatcher() {
    return new AsyncDispatcher();
  }

  protected AMRMTokenSecretManager createAMRMTokenSecretManager(
      Configuration conf) {
    return new AMRMTokenSecretManager(conf);
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
        throw new YarnRuntimeException("Class: " + schedulerClassName
            + " not instance of " + ResourceScheduler.class.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate Scheduler: "
          + schedulerClassName, e);
    }
  }

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

  // sanity check for configurations
  protected static void validateConfigs(Configuration conf) {
    // validate max-attempts
    int globalMaxAppAttempts =
        conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    if (globalMaxAppAttempts <= 0) {
      throw new YarnRuntimeException("Invalid global max attempts configuration"
          + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
          + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
    }

    // validate expireIntvl >= heartbeatIntvl
    long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    long heartbeatIntvl =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    if (expireIntvl < heartbeatIntvl) {
      throw new YarnRuntimeException("Nodemanager expiry interval should be no"
          + " less than heartbeat interval, "
          + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
          + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
          + heartbeatIntvl);
    }
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
    protected void serviceInit(Configuration conf) throws Exception {
      this.shouldExitOnError =
          conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
      super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
      this.eventProcessor.start();
      super.serviceStart();
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
    protected void serviceStop() throws Exception {
      this.stopped = true;
      this.eventProcessor.interrupt();
      try {
        this.eventProcessor.join();
      } catch (InterruptedException e) {
        throw new YarnRuntimeException(e);
      }
      super.serviceStop();
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
        throw new YarnRuntimeException(e);
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
  public static final class
    RMContainerPreemptEventDispatcher
      implements EventHandler<ContainerPreemptEvent> {

    private final PreemptableResourceScheduler scheduler;

    public RMContainerPreemptEventDispatcher(
        PreemptableResourceScheduler scheduler) {
      this.scheduler = scheduler;
    }

    @Override
    public void handle(ContainerPreemptEvent event) {
      ApplicationAttemptId aid = event.getAppId();
      RMContainer container = event.getContainer();
      switch (event.getType()) {
      case DROP_RESERVATION:
        scheduler.dropContainerReservation(container);
        break;
      case PREEMPT_CONTAINER:
        scheduler.preemptContainer(aid, container);
        break;
      case KILL_CONTAINER:
        scheduler.killContainer(container);
        break;
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
        WebApps
            .$for("cluster", ApplicationMasterService.class, masterService,
                "ws")
            .with(conf)
            .withHttpSpnegoPrincipalKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
            .withHttpSpnegoKeytabKey(
                YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
            .at(WebAppUtils.getRMWebAppURLWithoutScheme(conf)); 
    String proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
    if(WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
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
  protected void serviceStart() throws Exception {
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to login", ie);
    }

    this.amRmTokenSecretManager.start();
    this.containerTokenSecretManager.start();
    this.nmTokenSecretManager.start();

    RMStateStore rmStore = rmContext.getStateStore();
    // The state store needs to start irrespective of recoveryEnabled as apps
    // need events to move to further states.
    rmStore.start();

    if(recoveryEnabled) {
      try {
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
    try {
      rmDTSecretManager.startThreads();
    } catch(IOException ie) {
      throw new YarnRuntimeException("Failed to start secret manager threads", ie);
    }

    if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      String hostname = getConfig().get(YarnConfiguration.RM_WEBAPP_ADDRESS,
                                        YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
      hostname = (hostname.contains(":")) ? hostname.substring(0, hostname.indexOf(":")) : hostname;
      int port = webApp.port();
      String resolvedAddress = hostname + ":" + port;
      conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, resolvedAddress);
    }
    
    super.serviceStart();

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
  protected void serviceStop() throws Exception {
    if (webApp != null) {
      webApp.stop();
    }
    if (rmDTSecretManager != null) {
      rmDTSecretManager.stopThreads();
    }

    if (amRmTokenSecretManager != null) {
      this.amRmTokenSecretManager.stop();
    }
    if (containerTokenSecretManager != null) {
      this.containerTokenSecretManager.stop();
    }
    if(nmTokenSecretManager != null) {
      nmTokenSecretManager.stop();
    }

    /*synchronized(shutdown) {
      shutdown.set(true);
      shutdown.notifyAll();
    }*/

    DefaultMetricsSystem.shutdown();

    if (rmContext != null) {
      RMStateStore store = rmContext.getStateStore();
      try {
        store.close();
      } catch (Exception e) {
        LOG.error("Error closing store.", e);
      }
    }

    super.serviceStop();
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor, this.containerTokenSecretManager,
        this.nmTokenSecretManager);
  }

  protected RMDelegationTokenSecretManager
               createRMDelegationTokenSecretManager(RMContext rmContext) {
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
        tokenMaxLifetime, tokenRenewInterval, 3600000, rmContext);
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
        this.applicationACLsManager, this.queueACLsManager,
        this.rmDTSecretManager);
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(this.rmContext, scheduler);
  }

  protected void createPolicyMonitors() {
    if (scheduler instanceof PreemptableResourceScheduler
        && conf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS)) {
      LOG.info("Loading policy monitors");
      List<SchedulingEditPolicy> policies = conf.getInstances(
              YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES,
              SchedulingEditPolicy.class);
      if (policies.size() > 0) {
        this.rmDispatcher.register(ContainerPreemptEventType.class,
          new RMContainerPreemptEventDispatcher(
            (PreemptableResourceScheduler) scheduler));
        for (SchedulingEditPolicy policy : policies) {
          LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
          policy.init(conf, this.rmContext.getDispatcher().getEventHandler(),
              (PreemptableResourceScheduler) scheduler);
          // periodically check whether we need to take action to guarantee
          // constraints
          SchedulingMonitor mon = new SchedulingMonitor(policy);
          addService(mon);

        }
      } else {
        LOG.warn("Policy monitors configured (" +
            YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS +
            ") but none specified (" +
            YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + ")");
      }
    }
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
  public QueueACLsManager getQueueACLsManager() {
    return this.queueACLsManager;
  }

  @Private
  public RMContainerTokenSecretManager getRMContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }

  @Private
  public NMTokenSecretManagerInRM getRMNMTokenSecretManager() {
    return this.nmTokenSecretManager;
  }
  
  @Private
  public AMRMTokenSecretManager getAMRMTokenSecretManager(){
    return this.amRmTokenSecretManager;
  }

  @Override
  public void recover(RMState state) throws Exception {
    // recover RMdelegationTokenSecretManager
    rmDTSecretManager.recover(state);

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
      setHttpPolicy(conf);
      resourceManager.init(conf);
      resourceManager.start();
    } catch (Throwable t) {
      LOG.fatal("Error starting ResourceManager", t);
      System.exit(-1);
    }
  }
  
  private static void setHttpPolicy(Configuration conf) {
    HttpConfig.setPolicy(Policy.fromString(conf.get(
      YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT)));
  }
}
