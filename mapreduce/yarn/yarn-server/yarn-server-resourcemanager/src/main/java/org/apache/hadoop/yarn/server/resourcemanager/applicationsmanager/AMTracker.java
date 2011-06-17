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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationMaster;
import org.apache.hadoop.yarn.api.records.ApplicationState;
import org.apache.hadoop.yarn.api.records.ApplicationStatus;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMConfig;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ASMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.resourcemanager.applicationsmanager.events.ApplicationMasterEvents.ApplicationEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.ApplicationsStore.ApplicationStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.ApplicationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Store.RMState;
import org.apache.hadoop.yarn.service.AbstractService;

/**
 * This class tracks the application masters that are running. It tracks
 * heartbeats from application master to see if it needs to expire some application
 * master.
 */
@Evolving
@Private
public class AMTracker extends AbstractService  implements EventHandler<ASMEvent
<ApplicationEventType>>, Recoverable {
  private static final Log LOG = LogFactory.getLog(AMTracker.class);
  private AMLivelinessMonitor amLivelinessMonitor;
  private long amExpiryInterval; 
  @SuppressWarnings("rawtypes")
  private EventHandler handler;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private int amMaxRetries;

  private final RMContext rmContext;

  private final Map<ApplicationId, ApplicationMasterInfo> applications = 
    new ConcurrentHashMap<ApplicationId, ApplicationMasterInfo>();

  private final ApplicationsStore appsStore;
  
  private TreeSet<ApplicationStatus> amExpiryQueue =
    new TreeSet<ApplicationStatus>(
        new Comparator<ApplicationStatus>() {
          public int compare(ApplicationStatus p1, ApplicationStatus p2) {
            if (p1.getLastSeen() < p2.getLastSeen()) {
              return -1;
            } else if (p1.getLastSeen() > p2.getLastSeen()) {
              return 1;
            } else {
              return (p1.getApplicationId().getId() -
                  p2.getApplicationId().getId());
            }
          }
        }
    );

  public AMTracker(RMContext rmContext) {
    super(AMTracker.class.getName());
    this.amLivelinessMonitor = new AMLivelinessMonitor();
    this.rmContext = rmContext;
    this.appsStore = rmContext.getApplicationsStore();
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
    this.handler = rmContext.getDispatcher().getEventHandler();
    this.amExpiryInterval = conf.getLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 
        RMConfig.DEFAULT_AM_EXPIRY_INTERVAL);
    LOG.info("AM expiry interval: " + this.amExpiryInterval);
    this.amMaxRetries =  conf.getInt(RMConfig.AM_MAX_RETRIES, 
        RMConfig.DEFAULT_AM_MAX_RETRIES);
    LOG.info("AM max retries: " + this.amMaxRetries);
    this.amLivelinessMonitor.setMonitoringInterval(conf.getLong(
        RMConfig.AMLIVELINESS_MONITORING_INTERVAL,
        RMConfig.DEFAULT_AMLIVELINESS_MONITORING_INTERVAL));
    this.rmContext.getDispatcher().register(ApplicationEventType.class, this);
  }

  @Override
  public void start() {   
    super.start();
    amLivelinessMonitor.start();
  }

  /**
   * This class runs continuosly to track the application masters
   * that might be dead.
   */
  private class AMLivelinessMonitor extends Thread {
    private volatile boolean stop = false;
    private long monitoringInterval =
        RMConfig.DEFAULT_AMLIVELINESS_MONITORING_INTERVAL;

    public AMLivelinessMonitor() {
      super("ApplicationsManager:" + AMLivelinessMonitor.class.getName());
    }

    public void setMonitoringInterval(long interval) {
      this.monitoringInterval = interval;
    }

    @Override
    public void run() {

      /* the expiry queue does not need to be in sync with applications,
       * if an applications in the expiry queue cannot be found in applications
       * its alright. We do not want to hold a lock on applications while going
       * through the expiry queue.
       */
      List<ApplicationId> expired = new ArrayList<ApplicationId>();
      while (!stop) {
        ApplicationStatus leastRecent;
        long now = System.currentTimeMillis();
        expired.clear();
        synchronized(amExpiryQueue) {
          while ((amExpiryQueue.size() > 0) &&
              (leastRecent = amExpiryQueue.first()) != null &&
              ((now - leastRecent.getLastSeen()) > 
              amExpiryInterval)) {
            amExpiryQueue.remove(leastRecent);
            ApplicationMasterInfo info;
            synchronized(applications) {
              info = applications.get(leastRecent.getApplicationId());
            }
            if (info == null) {
              continue;
            }
            ApplicationStatus status = info.getStatus();
            if ((now - status.getLastSeen()) > amExpiryInterval) {
              expired.add(status.getApplicationId());
            } else {
              amExpiryQueue.add(status);
            }
          }
        }
        expireAMs(expired);
        try {
          Thread.sleep(this.monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }

    public void shutdown() {
      stop = true;
    }
  }

  private void expireAMs(List<ApplicationId> toExpire) {
    for (ApplicationId app: toExpire) {
      ApplicationMasterInfo am = null;
      synchronized (applications) {
        am = applications.get(app);
      }
      LOG.info("Expiring the Application " + app);
      handler.handle(new ASMEvent<ApplicationEventType>
      (ApplicationEventType.EXPIRE, am));
    }
  }

  @Override
  public void stop() {
    amLivelinessMonitor.interrupt();
    amLivelinessMonitor.shutdown();
    try {
      amLivelinessMonitor.join();
    } catch (InterruptedException ie) {
      LOG.info(amLivelinessMonitor.getName() + " interrupted during join ",
          ie);
    }
    super.stop();
  }

  public void addMaster(String user,  ApplicationSubmissionContext 
      submissionContext, String clientToken) throws IOException {
    
    ApplicationStore appStore = appsStore.createApplicationStore(submissionContext.getApplicationId(),
        submissionContext);
    ApplicationMasterInfo applicationMaster = new ApplicationMasterInfo(rmContext, 
        user, submissionContext, clientToken, appStore);
    synchronized(applications) {
      applications.put(applicationMaster.getApplicationID(), applicationMaster);
    }
    
  }
  
  public void runApplication(ApplicationId applicationId) {
    ApplicationMasterInfo masterInfo = null;
    synchronized (applications) {
      masterInfo = applications.get(applicationId);
    }
    rmContext.getDispatcher().getSyncHandler().handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.ALLOCATE, masterInfo));
    
  }
  
  public void finishNonRunnableApplication(ApplicationId applicationId) {
    ApplicationMasterInfo masterInfo = null;
    synchronized (applications) {
      masterInfo = applications.get(applicationId);
    }
    rmContext.getDispatcher().getSyncHandler().handle(new ASMEvent<ApplicationEventType>(
        ApplicationEventType.FAILED, masterInfo));
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
        new ApplicationFinishEvent(masterInfo, remoteApplicationMaster
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

  private void addForTracking(AppContext master) {
    LOG.info("Adding application master for tracking " + master.getMaster());
    synchronized (amExpiryQueue) {
      amExpiryQueue.add(master.getStatus());
    }
  }

  public void kill(ApplicationId applicationID) {
    ApplicationMasterInfo masterInfo = null;

    synchronized(applications) {
      masterInfo = applications.get(applicationID);
    }
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.KILL, 
        masterInfo));
  }

  /*
   * this class is used for passing status context to the application state
   * machine.
   */
  private static class TrackerAppContext implements AppContext {
    private final ApplicationId appID;
    private final ApplicationMaster master;
    private final UnsupportedOperationException notimplemented;

    public TrackerAppContext(
        ApplicationId appId, ApplicationMaster master) {
      this.appID = appId;
      this.master = master;
      this.notimplemented = new NotImplementedException();
    }

    @Override
    public ApplicationSubmissionContext getSubmissionContext() {
      throw notimplemented;
    }
    @Override
    public Resource getResource() {
      throw notimplemented;
    }
    @Override
    public ApplicationId getApplicationID() {
      return appID;
    }
    @Override
    public ApplicationStatus getStatus() {
      return master.getStatus();
    }
    @Override
    public ApplicationMaster getMaster() {
      return master;
    }
    @Override
    public Container getMasterContainer() {
      throw notimplemented;
    }
    @Override
    public String getUser() {   
      throw notimplemented;
    }
   
    @Override
    public String getName() {
      throw notimplemented;
    }
    @Override
    public String getQueue() {
      throw notimplemented;
    }

    @Override
    public int getFailedCount() {
      throw notimplemented;
    }

    @Override
    public ApplicationStore getStore() {
     throw notimplemented;
    }

    @Override
    public long getStartTime() {
      throw notimplemented;
    }

    @Override
    public long getFinishTime() {
      throw notimplemented;
    }
  }

  public void heartBeat(ApplicationStatus status) {
    ApplicationMaster master = recordFactory.newRecordInstance(ApplicationMaster.class);
    master.setStatus(status);
    master.setApplicationId(status.getApplicationId());
    TrackerAppContext context = new TrackerAppContext(status.getApplicationId(), master);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.STATUSUPDATE, 
        context));
  }

  public void registerMaster(ApplicationMaster applicationMaster) {
    applicationMaster.getStatus().setLastSeen(System.currentTimeMillis());
    ApplicationMasterInfo master = null;
    synchronized(applications) {
      master = applications.get(applicationMaster.getApplicationId());
    }
    LOG.info("AM registration " + master.getMaster());
    TrackerAppContext registrationContext = new TrackerAppContext(
        master.getApplicationID(), applicationMaster);
    handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.
        REGISTERED,  registrationContext));
  }

  @Override
  public void handle(ASMEvent<ApplicationEventType> event) {
    ApplicationId appID = event.getAppContext().getApplicationID();
    ApplicationMasterInfo masterInfo = null;
    synchronized(applications) {
      masterInfo = applications.get(appID);
    }
    try {
      masterInfo.handle(event);
    } catch(Throwable t) {
      LOG.error("Error in handling event type " + event.getType() + " for application " 
          + event.getAppContext().getApplicationID());
    }
    /* we need to launch the applicaiton master on allocated transition */
    if (masterInfo.getState() == ApplicationState.ALLOCATED) {
      handler.handle(new ASMEvent<ApplicationEventType>(
          ApplicationEventType.LAUNCH, masterInfo));
    }
    if (masterInfo.getState() == ApplicationState.LAUNCHED) {
      addForTracking(masterInfo);
    }

    /* check to see if the AM is an EXPIRED_PENDING state and start off the cycle again */
    if (masterInfo.getState() == ApplicationState.EXPIRED_PENDING) {
      /* check to see if the number of retries are reached or not */
      if (masterInfo.getFailedCount() < this.amMaxRetries) {
        handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.ALLOCATE,
            masterInfo));
      } else {
        handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.
            FAILED_MAX_RETRIES, masterInfo));
      }
    }
  }

  @Override
  public void recover(RMState state) {
    for (Map.Entry<ApplicationId, ApplicationInfo> entry: state.getStoredApplications().entrySet()) {
      ApplicationId appId = entry.getKey();
      ApplicationInfo appInfo = entry.getValue();
      ApplicationMasterInfo masterInfo = null;
      try {
        masterInfo = new ApplicationMasterInfo(this.rmContext,
      
          appInfo.getApplicationSubmissionContext().getUser(), appInfo.getApplicationSubmissionContext(), 
          appInfo.getApplicationMaster().getClientToken(), 
          this.appsStore.createApplicationStore(appId, appInfo.getApplicationSubmissionContext()));
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
      handler.handle(new ASMEvent<ApplicationEventType>(ApplicationEventType.RECOVER, masterInfo));
    }
  }
}
