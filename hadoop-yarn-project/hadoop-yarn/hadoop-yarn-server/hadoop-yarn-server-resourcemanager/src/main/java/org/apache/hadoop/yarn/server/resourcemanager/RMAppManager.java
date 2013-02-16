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
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * This class manages the list of applications for the resource manager. 
 */
public class RMAppManager implements EventHandler<RMAppManagerEvent>, 
                                        Recoverable {

  private static final Log LOG = LogFactory.getLog(RMAppManager.class);

  private int completedAppsMax = YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS;
  private LinkedList<ApplicationId> completedApps = new LinkedList<ApplicationId>();

  private final RMContext rmContext;
  private final ApplicationMasterService masterService;
  private final YarnScheduler scheduler;
  private final ApplicationACLsManager applicationACLsManager;
  private Configuration conf;

  public RMAppManager(RMContext context,
      YarnScheduler scheduler, ApplicationMasterService masterService,
      ApplicationACLsManager applicationACLsManager, Configuration conf) {
    this.rmContext = context;
    this.scheduler = scheduler;
    this.masterService = masterService;
    this.applicationACLsManager = applicationACLsManager;
    this.conf = conf;
    setCompletedAppsMax(conf.getInt(
        YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
        YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS));
  }

  /**
   *  This class is for logging the application summary.
   */
  static class ApplicationSummary {
    static final Log LOG = LogFactory.getLog(ApplicationSummary.class);

    // Escape sequences 
    static final char EQUALS = '=';
    static final char[] charsToEscape =
      {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

    static class SummaryBuilder {
      final StringBuilder buffer = new StringBuilder();

      // A little optimization for a very common case
      SummaryBuilder add(String key, long value) {
        return _add(key, Long.toString(value));
      }

      <T> SummaryBuilder add(String key, T value) {
        return _add(key, StringUtils.escapeString(String.valueOf(value),
                    StringUtils.ESCAPE_CHAR, charsToEscape));
      }

      SummaryBuilder add(SummaryBuilder summary) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(summary.buffer);
        return this;
      }

      SummaryBuilder _add(String key, String value) {
        if (buffer.length() > 0) buffer.append(StringUtils.COMMA);
        buffer.append(key).append(EQUALS).append(value);
        return this;
      }

      @Override public String toString() {
        return buffer.toString();
      }
    }

    /**
     * create a summary of the application's runtime.
     * 
     * @param app {@link RMApp} whose summary is to be created, cannot
     *            be <code>null</code>.
     */
    public static SummaryBuilder createAppSummary(RMApp app) {
      String trackingUrl = "N/A";
      String host = "N/A";
      RMAppAttempt attempt = app.getCurrentAppAttempt();
      if (attempt != null) {
        trackingUrl = attempt.getTrackingUrl();
        host = attempt.getHost();
      }
      SummaryBuilder summary = new SummaryBuilder()
          .add("appId", app.getApplicationId())
          .add("name", app.getName())
          .add("user", app.getUser())
          .add("queue", app.getQueue())
          .add("state", app.getState())
          .add("trackingUrl", trackingUrl)
          .add("appMasterHost", host)
          .add("startTime", app.getStartTime())
          .add("finishTime", app.getFinishTime());
      return summary;
    }

    /**
     * Log a summary of the application's runtime.
     * 
     * @param app {@link RMApp} whose summary is to be logged
     */
    public static void logAppSummary(RMApp app) {
      if (app != null) {
        LOG.info(createAppSummary(app));
      }
    }
  }

  protected synchronized void setCompletedAppsMax(int max) {
    this.completedAppsMax = max;
  }

  protected synchronized int getCompletedAppsListSize() {
    return this.completedApps.size(); 
  }

  protected synchronized void finishApplication(ApplicationId applicationId) {
    if (applicationId == null) {
      LOG.error("RMAppManager received completed appId of null, skipping");
    } else {
      // Inform the DelegationTokenRenewer
      if (UserGroupInformation.isSecurityEnabled()) {
        rmContext.getDelegationTokenRenewer().applicationFinished(applicationId);
      }
      
      completedApps.add(applicationId);  
      writeAuditLog(applicationId);
      
      // application completely done. Remove from state
      RMStateStore store = rmContext.getStateStore();
      store.removeApplication(rmContext.getRMApps().get(applicationId));
    }
  }

  protected void writeAuditLog(ApplicationId appId) {
    RMApp app = rmContext.getRMApps().get(appId);
    String operation = "UNKONWN";
    boolean success = false;
    switch (app.getState()) {
      case FAILED: 
        operation = AuditConstants.FINISH_FAILED_APP;
        break;
      case FINISHED:
        operation = AuditConstants.FINISH_SUCCESS_APP;
        success = true;
        break;
      case KILLED: 
        operation = AuditConstants.FINISH_KILLED_APP;
        success = true;
        break;
      default:
    }
    
    if (success) {
      RMAuditLogger.logSuccess(app.getUser(), operation,
          "RMAppManager", app.getApplicationId());
    } else {
      StringBuilder diag = app.getDiagnostics(); 
      String msg = diag == null ? null : diag.toString();
      RMAuditLogger.logFailure(app.getUser(), operation, msg, "RMAppManager",
          "App failed with state: " + app.getState(), appId);
    }
  }

  /*
   * check to see if hit the limit for max # completed apps kept
   */
  protected synchronized void checkAppNumCompletedLimit() {
    while (completedApps.size() > this.completedAppsMax) {
      ApplicationId removeId = completedApps.remove();  
      LOG.info("Application should be expired, max # apps"
          + " met. Removing app: " + removeId); 
      rmContext.getRMApps().remove(removeId);
      this.applicationACLsManager.removeApplication(removeId);
    }
  }

  @SuppressWarnings("unchecked")
  protected void submitApplication(
      ApplicationSubmissionContext submissionContext, long submitTime) {
    ApplicationId applicationId = submissionContext.getApplicationId();
    RMApp application = null;
    try {

      // Sanity checks
      if (submissionContext.getQueue() == null) {
        submissionContext.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
      }
      if (submissionContext.getApplicationName() == null) {
        submissionContext.setApplicationName(
            YarnConfiguration.DEFAULT_APPLICATION_NAME);
      }

      // Create RMApp
      application =
          new RMAppImpl(applicationId, rmContext, this.conf,
            submissionContext.getApplicationName(),
            submissionContext.getUser(), submissionContext.getQueue(),
            submissionContext, this.scheduler, this.masterService,
            submitTime);

      // Sanity check - duplicate?
      if (rmContext.getRMApps().putIfAbsent(applicationId, application) != 
          null) {
        String message = "Application with id " + applicationId
            + " is already present! Cannot add a duplicate!";
        LOG.info(message);
        throw RPCUtil.getRemoteException(message);
      } 

      // Inform the ACLs Manager
      this.applicationACLsManager.addApplication(applicationId,
          submissionContext.getAMContainerSpec().getApplicationACLs());

      // Setup tokens for renewal
      if (UserGroupInformation.isSecurityEnabled()) {
        this.rmContext.getDelegationTokenRenewer().addApplication(
            applicationId,parseCredentials(submissionContext),
            submissionContext.getCancelTokensWhenComplete()
            );
      }      
      
      // All done, start the RMApp
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppEvent(applicationId, RMAppEventType.START));
    } catch (IOException ie) {
        LOG.info("RMAppManager submit application exception", ie);
        if (application != null) {
          // Sending APP_REJECTED is fine, since we assume that the 
          // RMApp is in NEW state and thus we havne't yet informed the 
          // Scheduler about the existence of the application
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppRejectedEvent(applicationId, ie.getMessage()));
        }
    }
  }
  
  private Credentials parseCredentials(ApplicationSubmissionContext application) 
      throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = application.getAMContainerSpec().getContainerTokens();
    if (tokens != null) {
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }
    return credentials;
  }
  
  @Override
  public void recover(RMState state) throws Exception {
    RMStateStore store = rmContext.getStateStore();
    assert store != null;
    // recover applications
    Map<ApplicationId, ApplicationState> appStates = state.getApplicationState();
    LOG.info("Recovering " + appStates.size() + " applications");
    for(ApplicationState appState : appStates.values()) {
      // re-submit the application
      // this is going to send an app start event but since the async dispatcher 
      // has not started that event will be queued until we have completed re
      // populating the state
      if(appState.getApplicationSubmissionContext().getUnmanagedAM()) {
        // do not recover unmanaged applications since current recovery 
        // mechanism of restarting attempts does not work for them.
        // This will need to be changed in work preserving recovery in which 
        // RM will re-connect with the running AM's instead of restarting them
        LOG.info("Not recovering unmanaged application " + appState.getAppId());
        store.removeApplication(appState);
      } else {
        LOG.info("Recovering application " + appState.getAppId());
        submitApplication(appState.getApplicationSubmissionContext(), 
                          appState.getSubmitTime());
        // re-populate attempt information in application
        RMAppImpl appImpl = (RMAppImpl) rmContext.getRMApps().get(
                                                          appState.getAppId());
        appImpl.recover(state);
      }
    }
  }

  @Override
  public void handle(RMAppManagerEvent event) {
    ApplicationId applicationId = event.getApplicationId();
    LOG.debug("RMAppManager processing event for " 
        + applicationId + " of type " + event.getType());
    switch(event.getType()) {
      case APP_COMPLETED: 
      {
        finishApplication(applicationId);
        ApplicationSummary.logAppSummary(
            rmContext.getRMApps().get(applicationId));
        checkAppNumCompletedLimit(); 
      } 
      break;
      case APP_SUBMIT:
      {
        ApplicationSubmissionContext submissionContext = 
            ((RMAppManagerSubmitEvent)event).getSubmissionContext();
        long submitTime = ((RMAppManagerSubmitEvent)event).getSubmitTime();
        submitApplication(submissionContext, submitTime);
      }
      break;
      default:
        LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
      }
  }
}
