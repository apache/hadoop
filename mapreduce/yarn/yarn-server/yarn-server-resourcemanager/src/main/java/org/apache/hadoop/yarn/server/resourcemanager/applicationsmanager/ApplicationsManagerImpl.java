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

package org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationsManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.SNEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;


/**
 * This is the main class for the applications manager. This keeps track
 * of the application masters running in the system and is responsible for 
 * getting a container for AM and launching it.
 * {@link ApplicationsManager} is the interface that clients use to talk to 
 * ASM via the RPC servers. {@link ApplicationMasterHandler} is the interface that 
 * AM's use to talk to the ASM via the RPC.
 */
public class ApplicationsManagerImpl extends CompositeService
  implements ApplicationsManager  {
  private static final Log LOG = LogFactory.getLog(ApplicationsManagerImpl.class);

  final private YarnScheduler scheduler;
  private ClientToAMSecretManager clientToAMSecretManager =
    new ClientToAMSecretManager();
  private final EventHandler eventHandler;
  private final ApplicationTokenSecretManager applicationTokenSecretManager;
  private AMLivelinessMonitor amLivelinessMonitor;
  private final RMContext rmContext; 
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);
  
  public ApplicationsManagerImpl(ApplicationTokenSecretManager 
      applicationTokenSecretManager, YarnScheduler scheduler, RMContext rmContext) {
    super("ApplicationsManager");
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.eventHandler = this.rmContext.getDispatcher().getEventHandler();
    this.applicationTokenSecretManager = applicationTokenSecretManager;
    this.amLivelinessMonitor = new AMLivelinessMonitor(this.eventHandler);
   }

  @Override
  public AMLivelinessMonitor getAmLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }

  @Override
  public ClientToAMSecretManager getClientToAMSecretManager() {
    return this.clientToAMSecretManager;
  }

  /**
   * Create a new scheduler negotiator.
   * @param scheduler the scheduler 
   * @return scheduler negotiator that talks to the scheduler.
   */
  protected EventHandler<ASMEvent<SNEventType>> createNewSchedulerNegotiator(YarnScheduler scheduler) {
    return new SchedulerNegotiator(this.rmContext, scheduler);
  }

  /**
   * create a new application master launcher.
   * @param tokenSecretManager the token manager for applications.
   * @return {@link ApplicationMasterLauncher} responsible for launching
   * application masters.
   */
  protected EventHandler<ASMEvent<AMLauncherEventType>> createNewApplicationMasterLauncher(
      ApplicationTokenSecretManager tokenSecretManager) {
    return  new ApplicationMasterLauncher(tokenSecretManager,
        this.clientToAMSecretManager, this.rmContext);
  }

  /**
   * Add to service if a service object.
   * @param object
   */
  protected void addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
    }
  }

  @Override
  public  void init(Configuration conf) {
    addIfService(createNewApplicationMasterLauncher(applicationTokenSecretManager));
    addIfService(createNewSchedulerNegotiator(scheduler));
    addService(this.amLivelinessMonitor);
    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  /* As of now we dont remove applications from the RM */
  /* TODO we need to decide on a strategy for expiring done applications */
  public void remove(ApplicationId applicationId) {
    synchronized (rmContext.getApplications()) {
      //applications.remove(applicationId);
      this.amLivelinessMonitor.unRegister(applicationId);
    }
  }

  @Override
  public void recover(RMState state) {

    for (Map.Entry<ApplicationId, ApplicationInfo> entry : state
        .getStoredApplications().entrySet()) {
      ApplicationId appId = entry.getKey();
      ApplicationInfo appInfo = entry.getValue();
      Application application = null;
      try {
        application = new ApplicationImpl(this.rmContext, getConfig(), appInfo
            .getApplicationSubmissionContext().getUser(), appInfo
            .getApplicationSubmissionContext(), appInfo
            .getApplicationMaster().getClientToken(), this.rmContext
            .getApplicationsStore().createApplicationStore(appId,
                appInfo.getApplicationSubmissionContext()),
            this.amLivelinessMonitor);
      } catch (IOException ie) {
        // ignore
      }
      ApplicationMaster master = application.getMaster();
      ApplicationMaster storedAppMaster = appInfo.getApplicationMaster();
      master.setAMFailCount(storedAppMaster.getAMFailCount());
      master.setApplicationId(storedAppMaster.getApplicationId());
      master.setClientToken(storedAppMaster.getClientToken());
      master.setContainerCount(storedAppMaster.getContainerCount());
      master.setTrackingUrl(storedAppMaster.getTrackingUrl());
      master.setDiagnostics(storedAppMaster.getDiagnostics());
      master.setHost(storedAppMaster.getHost());
      master.setRpcPort(storedAppMaster.getRpcPort());
      master.setStatus(storedAppMaster.getStatus());
      master.setState(storedAppMaster.getState());
      // TODO: Synchro? PutIfAbset?
      this.rmContext.getApplications().put(appId, application);
      this.rmContext.getDispatcher().getEventHandler().handle(
          new ApplicationEvent(ApplicationEventType.RECOVER, appId));
    }

  }
}
