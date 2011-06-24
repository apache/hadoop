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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.service.CompositeService;

/**
 * This class tracks the application masters that are running. It tracks
 * heartbeats from application master to see if it needs to expire some application
 * master.
 */
@Evolving
@Private
public class AMTracker extends CompositeService implements Recoverable {
  private static final Log LOG = LogFactory.getLog(AMTracker.class);
  private AMLivelinessMonitor amLivelinessMonitor;
  @SuppressWarnings("rawtypes")
  private EventHandler handler;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  private final RMContext rmContext;

  private final Map<ApplicationId, ApplicationMasterInfo> applications = 
    new ConcurrentHashMap<ApplicationId, ApplicationMasterInfo>();

  private final ApplicationsStore appsStore;

  public AMTracker(RMContext rmContext) {
    super(AMTracker.class.getName());
    this.rmContext = rmContext;
    this.appsStore = rmContext.getApplicationsStore();
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.amLivelinessMonitor = new AMLivelinessMonitor(this.handler);
    addService(this.amLivelinessMonitor);
  }

  @Override
  public void init(Configuration conf) {
    this.rmContext.getDispatcher().register(ApplicationEventType.class,
        new ApplicationEventDispatcher());
    super.init(conf);
  }

  private final class ApplicationEventDispatcher implements
      EventHandler<ApplicationMasterInfoEvent> {

    public ApplicationEventDispatcher() {
    }

    @Override
    public void handle(ApplicationMasterInfoEvent event) {
      ApplicationId appID = event.getApplicationId();
      ApplicationMasterInfo masterInfo = null;
      synchronized (applications) {
        masterInfo = applications.get(appID);
      }
      try {
        masterInfo.handle(event);
      } catch (Throwable t) {
        LOG.error("Error in handling event type " + event.getType()
            + " for application " + event.getApplicationId());
      }
    }
  }

  public void addMaster(String user,  ApplicationSubmissionContext 
      submissionContext, String clientToken) throws IOException {
    
    ApplicationStore appStore = appsStore.createApplicationStore(submissionContext.getApplicationId(),
        submissionContext);
    ApplicationMasterInfo applicationMaster = new ApplicationMasterInfo(
        rmContext, getConfig(), user, submissionContext, clientToken,
        appStore, this.amLivelinessMonitor);
    synchronized(applications) {
      applications.put(applicationMaster.getApplicationID(), applicationMaster);
    }
    
  }
  
  public void runApplication(ApplicationId applicationId) {
    rmContext.getDispatcher().getSyncHandler().handle(
        new ApplicationMasterInfoEvent(ApplicationEventType.ALLOCATE,
            applicationId));
  }
  
  public void finishNonRunnableApplication(ApplicationId applicationId) {
    rmContext.getDispatcher().getSyncHandler().handle(
        new ApplicationMasterInfoEvent(ApplicationEventType.FAILED,
            applicationId));
   }

  public void finish(ApplicationMaster remoteApplicationMaster) {
    ApplicationId applicationId = remoteApplicationMaster.getApplicationId() ;
    ApplicationMasterInfo masterInfo = null;
    synchronized(applications) {
      masterInfo = applications.get(applicationId);
    }
    if (masterInfo == null) {
      LOG.info("Cant find application to finish " + applicationId);
      return;
    }
    masterInfo.getMaster().setTrackingUrl(
        remoteApplicationMaster.getTrackingUrl());
    masterInfo.getMaster().setDiagnostics(
        remoteApplicationMaster.getDiagnostics());

    rmContext.getDispatcher().getEventHandler().handle(
        new ApplicationFinishEvent(applicationId, remoteApplicationMaster
            .getState()));
  }

  public ApplicationMasterInfo get(ApplicationId applicationId) {
    ApplicationMasterInfo masterInfo = null;
    synchronized (applications) {
      masterInfo = applications.get(applicationId);
    }
    return masterInfo;
  }

  /* As of now we dont remove applications from the RM */
  /* TODO we need to decide on a strategy for expiring done applications */
  public void remove(ApplicationId applicationId) {
    synchronized (applications) {
      //applications.remove(applicationId);
      this.amLivelinessMonitor.unRegister(applicationId);
    }
  }

  public synchronized List<AppContext> getAllApplications() {
    List<AppContext> allAMs = new ArrayList<AppContext>();
    synchronized (applications) {
      for ( ApplicationMasterInfo val: applications.values()) {
        allAMs.add(val);
      }
    }
    return allAMs;
  }

  public void kill(ApplicationId applicationID) {
    handler.handle(new ApplicationMasterInfoEvent(ApplicationEventType.KILL,
        applicationID));
  }

  public void heartBeat(ApplicationStatus status) {
    ApplicationMaster master = recordFactory.newRecordInstance(ApplicationMaster.class);
    master.setStatus(status);
    master.setApplicationId(status.getApplicationId());
    handler.handle(new ApplicationMasterStatusUpdateEvent(status));
  }

  public void registerMaster(ApplicationMaster applicationMaster) {
    ApplicationMasterInfo master = null;
    synchronized(applications) {
      master = applications.get(applicationMaster.getApplicationId());
    }
    LOG.info("AM registration " + master.getMaster());
    handler.handle(new ApplicationMasterRegistrationEvent(applicationMaster));
  }

  @Override
  public void recover(RMState state) {
    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
      ApplicationId appId = entry.getKey();
      ApplicationInfo appInfo = entry.getValue();
      ApplicationMasterInfo masterInfo = null;
      try {
        masterInfo = new ApplicationMasterInfo(this.rmContext, getConfig(),
            appInfo.getApplicationSubmissionContext().getUser(), appInfo
                .getApplicationSubmissionContext(), appInfo
                .getApplicationMaster().getClientToken(), this.appsStore
                .createApplicationStore(appId, appInfo
                    .getApplicationSubmissionContext()),
            this.amLivelinessMonitor);
      } catch(IOException ie) {
        //ignore
      }
      ApplicationMaster master = masterInfo.getMaster();
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
      applications.put(appId, masterInfo);
      handler.handle(new ApplicationMasterInfoEvent(
          ApplicationEventType.RECOVER, appId));
    }
  }
}
