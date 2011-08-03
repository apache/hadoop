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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

/**
 * The ResourceManager is the main class that is a set of components.
 *
 */
public class ResourceManager extends CompositeService implements Recoverable {
  private static final Log LOG = LogFactory.getLog(ResourceManager.class);
  public static final long clusterTimeStamp = System.currentTimeMillis();
  private YarnConfiguration conf;

  protected ClientToAMSecretManager clientToAMSecretManager =
      new ClientToAMSecretManager();
  
  protected ContainerTokenSecretManager containerTokenSecretManager =
      new ContainerTokenSecretManager();

  protected ApplicationTokenSecretManager appTokenSecretManager =
      new ApplicationTokenSecretManager();

  private Dispatcher rmDispatcher;

  protected ResourceScheduler scheduler;
  private ClientRMService clientRM;
  protected ApplicationMasterService masterService;
  private ApplicationMasterLauncher applicationMasterLauncher;
  private AdminService adminService;
  private ContainerAllocationExpirer containerAllocationExpirer;
  protected NMLivelinessMonitor nmLivelinessMonitor;
  protected NodesListManager nodesListManager;

  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private WebApp webApp;
  private RMContext rmContext;
  private final Store store;
  protected ResourceTrackerService resourceTracker;
  
  public ResourceManager(Store store) {
    super("ResourceManager");
    this.store = store;
    this.nodesListManager = new NodesListManager();
  }
  
  public RMContext getRMContext() {
    return this.rmContext;
  }
  
  @Override
  public synchronized void init(Configuration conf) {

    this.rmDispatcher = new AsyncDispatcher();
    addIfService(this.rmDispatcher);

    this.containerAllocationExpirer = new ContainerAllocationExpirer(
        this.rmDispatcher);
    addService(this.containerAllocationExpirer);

    AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
    addService(amLivelinessMonitor);

    this.rmContext = new RMContextImpl(this.store, this.rmDispatcher,
        this.containerAllocationExpirer, amLivelinessMonitor);

    addService(nodesListManager);

    // Initialize the config
    this.conf = new YarnConfiguration(conf);
    // Initialize the scheduler
    this.scheduler = createScheduler();
    this.rmDispatcher.register(SchedulerEventType.class, scheduler);

    // Register event handler for RmAppEvents
    this.rmDispatcher.register(RMAppEventType.class,
        new ApplicationEventDispatcher(this.rmContext));

    // Register event handler for RmAppAttemptEvents
    this.rmDispatcher.register(RMAppAttemptEventType.class,
        new ApplicationAttemptEventDispatcher(this.rmContext));

    // Register event handler for RmNodes
    this.rmDispatcher.register(RMNodeEventType.class,
        new NodeEventDispatcher(this.rmContext));

    // Register event handler for RMContainer
    this.rmDispatcher.register(RMContainerEventType.class,
        new ContainerEventDispatcher(this.rmContext));

    //TODO change this to be random
    this.appTokenSecretManager.setMasterKey(ApplicationTokenSecretManager
        .createSecretKey("Dummy".getBytes()));

    this.nmLivelinessMonitor = createNMLivelinessMonitor();
    addService(this.nmLivelinessMonitor);

    this.resourceTracker = createResourceTrackerService();
    addService(resourceTracker);
  
    try {
      this.scheduler.reinitialize(this.conf,
          this.containerTokenSecretManager, this.rmContext);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to initialize scheduler", ioe);
    }

    masterService = createApplicationMasterService();
    addService(masterService) ;

    clientRM = createClientRMService();
    addService(clientRM);
    
    adminService = createAdminService();
    addService(adminService);

    this.applicationMasterLauncher = createAMLauncher();
    addService(applicationMasterLauncher);

    super.init(conf);
  }

  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  protected ResourceScheduler createScheduler() {
    return 
    ReflectionUtils.newInstance(
        conf.getClass(RMConfig.RESOURCE_SCHEDULER, 
            FifoScheduler.class, ResourceScheduler.class), 
        this.conf);
  }

  protected ApplicationMasterLauncher createAMLauncher() {
    return new ApplicationMasterLauncher(
        this.appTokenSecretManager, this.clientToAMSecretManager,
        this.rmContext);
  }

  private NMLivelinessMonitor createNMLivelinessMonitor() {
    return new NMLivelinessMonitor(this.rmContext
        .getDispatcher());
  }

  protected AMLivelinessMonitor createAMLivelinessMonitor() {
    return new AMLivelinessMonitor(this.rmDispatcher);
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

  private static final class ContainerEventDispatcher implements
      EventHandler<RMContainerEvent> {

    private final RMContext rmContext;

    public ContainerEventDispatcher(RMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(RMContainerEvent event) {
      ContainerId containerId = event.getContainerId();
      RMContainer container = this.rmContext.getRMContainers().get(containerId);
      if (container != null) {
        try {
          ((EventHandler<RMContainerEvent>) container).handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType()
              + " for container " + containerId, t);
        }
      }
    }
  }

  protected void startWepApp() {
    webApp = WebApps.$for("yarn", masterService).at(
        conf.get(YarnConfiguration.RM_WEBAPP_BIND_ADDRESS,
        YarnConfiguration.DEFAULT_RM_WEBAPP_BIND_ADDRESS)).
      start(new RMWebApp(this));

  }

  @Override
  public void start() {
    try {
      doSecureLogin();
    } catch(IOException ie) {
      throw new YarnException("Failed to login", ie);
    }

    startWepApp();
    DefaultMetricsSystem.initialize("ResourceManager");

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
    SecurityUtil.login(conf, RMConfig.RM_KEYTAB,
        YarnConfiguration.RM_SERVER_PRINCIPAL_KEY);
  }

  @Override
  public void stop() {
    if (webApp != null) {
      webApp.stop();
    }

    /*synchronized(shutdown) {
      shutdown.set(true);
      shutdown.notifyAll();
    }*/

    DefaultMetricsSystem.shutdown();

    super.stop();
  }
  
  protected ResourceTrackerService createResourceTrackerService() {
    return new ResourceTrackerService(this.rmContext, this.nodesListManager,
        this.nmLivelinessMonitor, this.containerTokenSecretManager);
  }

  protected ClientRMService createClientRMService() {
    return new ClientRMService(this.rmContext, this.clientToAMSecretManager,
        scheduler, masterService);
  }

  protected ApplicationMasterService createApplicationMasterService() {
    return new ApplicationMasterService(this.rmContext,
        this.appTokenSecretManager, scheduler);
  }
  

  protected AdminService createAdminService() {
    return new AdminService(conf, scheduler, rmContext, this.nodesListManager);
  }

  @Private
  public ClientRMService getClientRMService() {
    return this.clientRM;
  }
  
  /**
   * return the scheduler.
   * @return
   */
  @Private
  public ResourceScheduler getResourceScheduler() {
    return this.scheduler;
  }

  /**
   * return the resource tracking component.
   * @return
   */
  @Private
  public ResourceTrackerService getResourceTrackerService() {
    return this.resourceTracker;
  }

  @Private
  public ApplicationMasterService getApplicationMasterService() {
    return this.masterService;
  }

  @Override
  public void recover(RMState state) throws Exception {
    resourceTracker.recover(state);
    scheduler.recover(state);
  }
  
  public static void main(String argv[]) {
    ResourceManager resourceManager = null;
    try {
      Configuration conf = new YarnConfiguration();
      Store store =  StoreFactory.getStore(conf);
      resourceManager = new ResourceManager(store);
      resourceManager.init(conf);
      //resourceManager.recover(store.restore());
      //store.doneWithRecovery();
      resourceManager.start();
    } catch (Throwable e) {
      LOG.error("Error starting RM", e);
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }
}
