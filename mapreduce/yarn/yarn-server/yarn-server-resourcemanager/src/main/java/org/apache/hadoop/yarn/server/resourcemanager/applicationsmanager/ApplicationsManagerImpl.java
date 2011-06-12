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
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.Application;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.ApplicationTokenIdentifier;
import org.apache.hadoop.yarn.security.ApplicationTokenSecretManager;
import org.apache.hadoop.yarn.security.client.ClientToAMSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.application.ApplicationACL;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.application.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.SNEventType;
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
  implements ApplicationsManager, ApplicationMasterHandler  {
  private static final Log LOG = LogFactory.getLog(ApplicationsManagerImpl.class);

  final private AtomicInteger applicationCounter = new AtomicInteger(0);
  final private YarnScheduler scheduler;
  private AMTracker amTracker;
  private ClientToAMSecretManager clientToAMSecretManager =
    new ClientToAMSecretManager();
  private final EventHandler eventHandler;
  private final ApplicationTokenSecretManager applicationTokenSecretManager;
  private final RMContext rmContext; 
  private  ApplicationACLsManager aclsManager;
  private Map<ApplicationACL, AccessControlList> applicationACLs;
  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);
  
  public ApplicationsManagerImpl(ApplicationTokenSecretManager 
      applicationTokenSecretManager, YarnScheduler scheduler, RMContext rmContext) {
    super("ApplicationsManager");
    this.scheduler = scheduler;
    this.rmContext = rmContext;
    this.eventHandler = this.rmContext.getDispatcher().getEventHandler();
    this.applicationTokenSecretManager = applicationTokenSecretManager;
   }
  

  /**
   * create a new am heart beat handler.
   * @return create a new am heart beat handler.
   */
  protected AMTracker createNewAMTracker() {
    return new AMTracker(this.rmContext);
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
    this.amTracker = createNewAMTracker();
    addIfService(amTracker);
    this.aclsManager = new ApplicationACLsManager(conf);
    this.applicationACLs = aclsManager.constructApplicationACLs(conf);
    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public ApplicationMaster getApplicationMaster(ApplicationId applicationId) {
    ApplicationMaster appMaster =
      amTracker.get(applicationId).getMaster();
    //TODO NPE (When the RM is restarted - it doesn't know about previous AMs)
    return appMaster;
  }
  
  @Override
  public ApplicationId getNewApplicationID() {
    ApplicationId applicationId =
      org.apache.hadoop.yarn.util.BuilderUtils.newApplicationId(recordFactory,
          ResourceManager.clusterTimeStamp, applicationCounter.incrementAndGet());
    LOG.info("Allocated new applicationId: " + applicationId.getId());
    return applicationId;
  }

  @Override
  public  void submitApplication(ApplicationSubmissionContext context)
  throws IOException {
    String user;
    ApplicationId applicationId = context.getApplicationId();
    String clientTokenStr = null;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
      if (UserGroupInformation.isSecurityEnabled()) {
        Token<ApplicationTokenIdentifier> clientToken =
          new Token<ApplicationTokenIdentifier>(
              new ApplicationTokenIdentifier(applicationId),
              this.clientToAMSecretManager);
        clientTokenStr = clientToken.encodeToUrlString();
        LOG.debug("Sending client token as " + clientTokenStr);
      }
    } catch (IOException e) {
      LOG.info("Error in submitting application", e);
      throw e;
    } 

    context.setQueue(context.getQueue() == null ? "default" : context.getQueue());
    context.setApplicationName(context.getApplicationName() == null ? "N/A" : context.getApplicationName());
    amTracker.addMaster(user, context, clientTokenStr);
    ApplicationMasterInfo masterInfo = amTracker.get(applicationId);
    /** this can throw so we need to call it synchronously to let the client
     * know as soon as it submits. For backwards compatibility we cannot make 
     * it asynchronous
     */
    try {
      scheduler.addApplication(applicationId, masterInfo.getMaster(), user, masterInfo.getQueue(),
          context.getPriority(), masterInfo.getStore());
    } catch(IOException io) {
      LOG.info("Failed to submit application " + applicationId, io);
      amTracker.finishNonRunnableApplication(applicationId);
      throw io;
    }
    amTracker.runApplication(applicationId);
    // TODO this should happen via dispatcher. should move it out to scheudler
    // negotiator.
    LOG.info("Application with id " + applicationId.getId() + " submitted by user " + 
        user + " with " + context);
  }

  @Override
  public void finishApplicationMaster(ApplicationMaster applicationMaster)
  throws IOException {
    amTracker.finish(applicationMaster);
  }

  /**
   * check if the calling user has the access to application information.
   * @param applicationId
   * @param callerUGI
   * @param owner
   * @param appACL
   * @return
   */
  private boolean checkAccess(UserGroupInformation callerUGI, String owner, ApplicationACL appACL) {
      if (!UserGroupInformation.isSecurityEnabled()) {
        return true;
      }
      AccessControlList applicationACL = applicationACLs.get(appACL);
      return aclsManager.checkAccess(callerUGI, appACL, owner, applicationACL);
  }
  
  @Override
  public synchronized void finishApplication(ApplicationId applicationId,
      UserGroupInformation callerUGI) 
  throws IOException {
    ApplicationMasterInfo masterInfo = amTracker.get(applicationId);
    if (!checkAccess(callerUGI, masterInfo.getUser(),  ApplicationACL.MODIFY_APP)) {
      RPCUtil.getRemoteException(new AccessControlException("User "
          + callerUGI.getShortUserName() + " cannot perform operation "
          + ApplicationACL.MODIFY_APP.name() + " on " + applicationId));
    }
    amTracker.kill(applicationId);
  }

  @Override
  public  void applicationHeartbeat(ApplicationStatus status) 
  throws IOException {
    amTracker.heartBeat(status);
  }

  @Override
  public  void registerApplicationMaster(ApplicationMaster applicationMaster)
  throws IOException {
    amTracker.registerMaster(applicationMaster);
 }

  @Override
  public  List<AppContext> getAllApplications() {
    return amTracker.getAllApplications();
  }

  public  ApplicationMasterInfo getApplicationMasterInfo(ApplicationId
      applicationId) {
    return amTracker.get(applicationId);
  }
  
  private Application createApplication(ApplicationMaster am, String user,
      String queue, String name, Container masterContainer) {
    Application application = 
      recordFactory.newRecordInstance(Application.class);
    application.setApplicationId(am.getApplicationId());
    application.setMasterContainer(masterContainer);
    application.setTrackingUrl(am.getTrackingUrl());
    application.setDiagnostics(am.getDiagnostics());
    application.setName(name);
    application.setQueue(queue);
    application.setState(am.getState());
    application.setStatus(am.getStatus());
    application.setUser(user);
    return application;
  }
  
  @Override
  public List<Application> getApplications() {
    List<Application> apps = new ArrayList<Application>();
    for (AppContext am: getAllApplications()) {
      apps.add(createApplication(am.getMaster(), 
          am.getUser(), am.getQueue(), am.getName(), am.getMasterContainer()));
    }
    return apps;
  }

  @Override
  public Application getApplication(ApplicationId appID) {
    ApplicationMasterInfo master = amTracker.get(appID);
    return (master == null) ? null : createApplication(master.getMaster(),
        master.getUser(), master.getQueue(), master.getName(),
        master.getMasterContainer());
  }


  @Override
  public void recover(RMState state) {
    amTracker.recover(state);
  }
}
