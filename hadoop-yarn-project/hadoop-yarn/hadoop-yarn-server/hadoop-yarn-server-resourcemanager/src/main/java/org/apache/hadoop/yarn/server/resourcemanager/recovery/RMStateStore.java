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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEventType;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.RMStateVersion;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppUpdateSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptNewSavedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUpdateSavedEvent;

@Private
@Unstable
/**
 * Base class to implement storage of ResourceManager state.
 * Takes care of asynchronous notifications and interfacing with YARN objects.
 * Real store implementations need to derive from it and implement blocking
 * store and load methods to actually store and load the state.
 */
public abstract class RMStateStore extends AbstractService {

  // constants for RM App state and RMDTSecretManagerState.
  protected static final String RM_APP_ROOT = "RMAppRoot";
  protected static final String RM_DT_SECRET_MANAGER_ROOT = "RMDTSecretManagerRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";
  protected static final String DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX =
      "RMDTSequenceNumber_";
  protected static final String VERSION_NODE = "RMVersionNode";

  public static final Log LOG = LogFactory.getLog(RMStateStore.class);

  public RMStateStore() {
    super(RMStateStore.class.getName());
  }

  /**
   * State of an application attempt
   */
  public static class ApplicationAttemptState {
    final ApplicationAttemptId attemptId;
    final Container masterContainer;
    final Credentials appAttemptCredentials;
    long startTime = 0;
    // fields set when attempt completes
    RMAppAttemptState state;
    String finalTrackingUrl = "N/A";
    String diagnostics;
    FinalApplicationStatus amUnregisteredFinalStatus;

    public ApplicationAttemptState(ApplicationAttemptId attemptId,
        Container masterContainer, Credentials appAttemptCredentials,
        long startTime) {
      this(attemptId, masterContainer, appAttemptCredentials, startTime, null,
        null, "", null);
    }

    public ApplicationAttemptState(ApplicationAttemptId attemptId,
        Container masterContainer, Credentials appAttemptCredentials,
        long startTime, RMAppAttemptState state, String finalTrackingUrl,
        String diagnostics, FinalApplicationStatus amUnregisteredFinalStatus) {
      this.attemptId = attemptId;
      this.masterContainer = masterContainer;
      this.appAttemptCredentials = appAttemptCredentials;
      this.startTime = startTime;
      this.state = state;
      this.finalTrackingUrl = finalTrackingUrl;
      this.diagnostics = diagnostics == null ? "" : diagnostics;
      this.amUnregisteredFinalStatus = amUnregisteredFinalStatus;
    }

    public Container getMasterContainer() {
      return masterContainer;
    }
    public ApplicationAttemptId getAttemptId() {
      return attemptId;
    }
    public Credentials getAppAttemptCredentials() {
      return appAttemptCredentials;
    }
    public RMAppAttemptState getState(){
      return state;
    }
    public String getFinalTrackingUrl() {
      return finalTrackingUrl;
    }
    public String getDiagnostics() {
      return diagnostics;
    }
    public long getStartTime() {
      return startTime;
    }
    public FinalApplicationStatus getFinalApplicationStatus() {
      return amUnregisteredFinalStatus;
    }
  }
  
  /**
   * State of an application application
   */
  public static class ApplicationState {
    final ApplicationSubmissionContext context;
    final long submitTime;
    final long startTime;
    final String user;
    Map<ApplicationAttemptId, ApplicationAttemptState> attempts =
                  new HashMap<ApplicationAttemptId, ApplicationAttemptState>();
    // fields set when application completes.
    RMAppState state;
    String diagnostics;
    long finishTime;

    public ApplicationState(long submitTime,
        long startTime, ApplicationSubmissionContext context, String user) {
      this(submitTime, startTime, context, user, null, "", 0);
    }

    public ApplicationState(long submitTime,
        long startTime,ApplicationSubmissionContext context,
        String user, RMAppState state, String diagnostics, long finishTime) {
      this.submitTime = submitTime;
      this.startTime = startTime;
      this.context = context;
      this.user = user;
      this.state = state;
      this.diagnostics = diagnostics == null ? "" : diagnostics;
      this.finishTime = finishTime;
    }

    public ApplicationId getAppId() {
      return context.getApplicationId();
    }
    public long getSubmitTime() {
      return submitTime;
    }
    public long getStartTime() {
      return startTime;
    }
    public int getAttemptCount() {
      return attempts.size();
    }
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      return context;
    }
    public ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId) {
      return attempts.get(attemptId);
    }
    public String getUser() {
      return user;
    }
    public RMAppState getState() {
      return state;
    }
    public String getDiagnostics() {
      return diagnostics;
    }
    public long getFinishTime() {
      return finishTime;
    }
  }

  public static class RMDTSecretManagerState {
    // DTIdentifier -> renewDate
    Map<RMDelegationTokenIdentifier, Long> delegationTokenState =
        new HashMap<RMDelegationTokenIdentifier, Long>();

    Set<DelegationKey> masterKeyState =
        new HashSet<DelegationKey>();

    int dtSequenceNumber = 0;

    public Map<RMDelegationTokenIdentifier, Long> getTokenState() {
      return delegationTokenState;
    }

    public Set<DelegationKey> getMasterKeyState() {
      return masterKeyState;
    }

    public int getDTSequenceNumber() {
      return dtSequenceNumber;
    }
  }

  /**
   * State of the ResourceManager
   */
  public static class RMState {
    Map<ApplicationId, ApplicationState> appState =
        new HashMap<ApplicationId, ApplicationState>();

    RMDTSecretManagerState rmSecretManagerState = new RMDTSecretManagerState();

    public Map<ApplicationId, ApplicationState> getApplicationState() {
      return appState;
    }

    public RMDTSecretManagerState getRMDTSecretManagerState() {
      return rmSecretManagerState;
    }
  }
    
  private Dispatcher rmDispatcher;

  /**
   * Dispatcher used to send state operation completion events to 
   * ResourceManager services
   */
  public void setRMDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }
  
  AsyncDispatcher dispatcher;

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.register(RMStateStoreEventType.class, 
                        new ForwardingEventHandler());
    dispatcher.setDrainEventsOnStop();
    initInternal(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dispatcher.start();
    startInternal();
  }

  /**
   * Derived classes initialize themselves using this method.
   */
  protected abstract void initInternal(Configuration conf) throws Exception;

  /**
   * Derived classes start themselves using this method.
   * The base class is started and the event dispatcher is ready to use at
   * this point
   */
  protected abstract void startInternal() throws Exception;

  @Override
  protected void serviceStop() throws Exception {
    closeInternal();
    dispatcher.stop();
  }

  /**
   * Derived classes close themselves using this method.
   * The base class will be closed and the event dispatcher will be shutdown 
   * after this
   */
  protected abstract void closeInternal() throws Exception;

  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of state-store is a major upgrade, and any
   *    compatible change of state-store is a minor upgrade.
   * 3) If theres's no version, treat it as 1.0.
   * 4) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 5) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade RM state.
   */
  public void checkVersion() throws Exception {
    RMStateVersion loadedVersion = loadVersion();
    LOG.info("Loaded RM state version info " + loadedVersion);
    if (loadedVersion != null && loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    // if there is no version info, treat it as 1.0;
    if (loadedVersion == null) {
      loadedVersion = RMStateVersion.newInstance(1, 0);
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing RM state version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new RMStateVersionIncompatibleException(
        "Expecting RM state version " + getCurrentVersion()
            + ", but loading version " + loadedVersion);
    }
  }

  /**
   * Derived class use this method to load the version information from state
   * store.
   */
  protected abstract RMStateVersion loadVersion() throws Exception;

  /**
   * Derived class use this method to store the version information.
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * Get the current version of the underlying state store.
   */
  protected abstract RMStateVersion getCurrentVersion();

  /**
   * Blocking API
   * The derived class must recover state from the store and return a new 
   * RMState object populated with that state
   * This must not be called on the dispatcher thread
   */
  public abstract RMState loadState() throws Exception;
  
  /**
   * Non-Blocking API
   * ResourceManager services use this to store the application's state
   * This does not block the dispatcher threads
   * RMAppStoredEvent will be sent on completion to notify the RMApp
   */
  @SuppressWarnings("unchecked")
  public synchronized void storeNewApplication(RMApp app) {
    ApplicationSubmissionContext context = app
                                            .getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationState appState =
        new ApplicationState(app.getSubmitTime(), app.getStartTime(), context,
          app.getUser());
    dispatcher.getEventHandler().handle(new RMStateStoreAppEvent(appState));
  }

  @SuppressWarnings("unchecked")
  public synchronized void updateApplicationState(ApplicationState appState) {
    dispatcher.getEventHandler().handle(new RMStateUpdateAppEvent(appState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application.
   */
  protected abstract void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception;

  protected abstract void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateDataPBImpl appStateData) throws Exception;
  
  @SuppressWarnings("unchecked")
  /**
   * Non-blocking API
   * ResourceManager services call this to store state on an application attempt
   * This does not block the dispatcher threads
   * RMAppAttemptStoredEvent will be sent on completion to notify the RMAppAttempt
   */
  public synchronized void storeNewApplicationAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = getCredentialsFromAppAttempt(appAttempt);

    ApplicationAttemptState attemptState =
        new ApplicationAttemptState(appAttempt.getAppAttemptId(),
          appAttempt.getMasterContainer(), credentials,
          appAttempt.getStartTime());

    dispatcher.getEventHandler().handle(
      new RMStateStoreAppAttemptEvent(attemptState));
  }

  @SuppressWarnings("unchecked")
  public synchronized void updateApplicationAttemptState(
      ApplicationAttemptState attemptState) {
    dispatcher.getEventHandler().handle(
      new RMStateUpdateAppAttemptEvent(attemptState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application attempt
   */
  protected abstract void storeApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception;

  protected abstract void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a delegation token
   * and sequence number
   */
  public synchronized void storeRMDelegationTokenAndSequenceNumber(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) {
    try {
      storeRMDelegationTokenAndSequenceNumberState(rmDTIdentifier, renewDate,
          latestSequenceNumber);
    } catch (Exception e) {
      notifyStoreOperationFailed(e);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void storeRMDelegationTokenAndSequenceNumberState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a delegation token
   */
  public synchronized void removeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, int sequenceNumber) {
    try {
      removeRMDelegationTokenState(rmDTIdentifier);
    } catch (Exception e) {
      notifyStoreOperationFailed(e);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of RMDelegationToken
   */
  protected abstract void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception;

  /**
   * RMDTSecretManager call this to update the state of a delegation token
   * and sequence number
   */
  public synchronized void updateRMDelegationTokenAndSequenceNumber(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) {
    try {
      updateRMDelegationTokenAndSequenceNumberInternal(rmDTIdentifier, renewDate,
          latestSequenceNumber);
    } catch (Exception e) {
      notifyStoreOperationFailed(e);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to update the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void updateRMDelegationTokenAndSequenceNumberInternal(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a master key
   */
  public synchronized void storeRMDTMasterKey(DelegationKey delegationKey) {
    try {
      storeRMDTMasterKeyState(delegationKey);
    } catch (Exception e) {
      notifyStoreOperationFailed(e);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * DelegationToken Master Key
   */
  protected abstract void storeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a master key
   */
  public synchronized void removeRMDTMasterKey(DelegationKey delegationKey) {
    try {
      removeRMDTMasterKeyState(delegationKey);
    } catch (Exception e) {
      notifyStoreOperationFailed(e);
    }
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * DelegationToken Master Key
   */
  protected abstract void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * Non-blocking API
   * ResourceManager services call this to remove an application from the state
   * store
   * This does not block the dispatcher threads
   * There is no notification of completion for this operation.
   */
  @SuppressWarnings("unchecked")
  public synchronized void removeApplication(RMApp app) {
    ApplicationState appState = new ApplicationState(
            app.getSubmitTime(), app.getStartTime(),
            app.getApplicationSubmissionContext(), app.getUser());
    for(RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      Credentials credentials = getCredentialsFromAppAttempt(appAttempt);
      ApplicationAttemptState attemptState =
          new ApplicationAttemptState(appAttempt.getAppAttemptId(),
            appAttempt.getMasterContainer(), credentials,
            appAttempt.getStartTime());
      appState.attempts.put(attemptState.getAttemptId(), attemptState);
    }
    
    dispatcher.getEventHandler().handle(new RMStateStoreRemoveAppEvent(appState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of an 
   * application and its attempts
   */
  protected abstract void removeApplicationStateInternal(
      ApplicationState appState) throws Exception;

  // TODO: This should eventually become cluster-Id + "AM_RM_TOKEN_SERVICE". See
  // YARN-1779
  public static final Text AM_RM_TOKEN_SERVICE = new Text(
    "AM_RM_TOKEN_SERVICE");

  public static final Text AM_CLIENT_TOKEN_MASTER_KEY_NAME =
      new Text("YARN_CLIENT_TOKEN_MASTER_KEY");
  
  public Credentials getCredentialsFromAppAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = new Credentials();
    Token<AMRMTokenIdentifier> appToken = appAttempt.getAMRMToken();
    if(appToken != null){
      credentials.addToken(AM_RM_TOKEN_SERVICE, appToken);
    }
    SecretKey clientTokenMasterKey =
        appAttempt.getClientTokenMasterKey();
    if(clientTokenMasterKey != null){
      credentials.addSecretKey(AM_CLIENT_TOKEN_MASTER_KEY_NAME,
          clientTokenMasterKey.getEncoded());
    }
    return credentials;
  }

  // Dispatcher related code
  protected void handleStoreEvent(RMStateStoreEvent event) {
    if (event.getType().equals(RMStateStoreEventType.STORE_APP)
        || event.getType().equals(RMStateStoreEventType.UPDATE_APP)) {
      ApplicationState appState = null;
      if (event.getType().equals(RMStateStoreEventType.STORE_APP)) {
        appState = ((RMStateStoreAppEvent) event).getAppState();
      } else {
        assert event.getType().equals(RMStateStoreEventType.UPDATE_APP);
        appState = ((RMStateUpdateAppEvent) event).getAppState();
      }

      Exception storedException = null;
      ApplicationStateDataPBImpl appStateData =
          (ApplicationStateDataPBImpl) ApplicationStateDataPBImpl
            .newApplicationStateData(appState.getSubmitTime(),
              appState.getStartTime(), appState.getUser(),
              appState.getApplicationSubmissionContext(), appState.getState(),
              appState.getDiagnostics(), appState.getFinishTime());

      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();

      LOG.info("Storing info for app: " + appId);
      try {
        if (event.getType().equals(RMStateStoreEventType.STORE_APP)) {
          storeApplicationStateInternal(appId, appStateData);
          notifyDoneStoringApplication(appId, storedException);
        } else {
          assert event.getType().equals(RMStateStoreEventType.UPDATE_APP);
          updateApplicationStateInternal(appId, appStateData);
          notifyDoneUpdatingApplication(appId, storedException);
        }
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        notifyStoreOperationFailed(e);
      }
    } else if (event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT)
        || event.getType().equals(RMStateStoreEventType.UPDATE_APP_ATTEMPT)) {

      ApplicationAttemptState attemptState = null;
      if (event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT)) {
        attemptState =
            ((RMStateStoreAppAttemptEvent) event).getAppAttemptState();
      } else {
        assert event.getType().equals(RMStateStoreEventType.UPDATE_APP_ATTEMPT);
        attemptState =
            ((RMStateUpdateAppAttemptEvent) event).getAppAttemptState();
      }

      Exception storedException = null;
      Credentials credentials = attemptState.getAppAttemptCredentials();
      ByteBuffer appAttemptTokens = null;
      try {
        if (credentials != null) {
          DataOutputBuffer dob = new DataOutputBuffer();
          credentials.writeTokenStorageToStream(dob);
          appAttemptTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        }
        ApplicationAttemptStateDataPBImpl attemptStateData =
            (ApplicationAttemptStateDataPBImpl) ApplicationAttemptStateDataPBImpl
              .newApplicationAttemptStateData(attemptState.getAttemptId(),
                attemptState.getMasterContainer(), appAttemptTokens,
                attemptState.getStartTime(), attemptState.getState(),
                attemptState.getFinalTrackingUrl(),
                attemptState.getDiagnostics(),
                attemptState.getFinalApplicationStatus());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing info for attempt: " + attemptState.getAttemptId());
        }
        if (event.getType().equals(RMStateStoreEventType.STORE_APP_ATTEMPT)) {
          storeApplicationAttemptStateInternal(attemptState.getAttemptId(),
              attemptStateData);
          notifyDoneStoringApplicationAttempt(attemptState.getAttemptId(),
              storedException);
        } else {
          assert event.getType().equals(
            RMStateStoreEventType.UPDATE_APP_ATTEMPT);
          updateApplicationAttemptStateInternal(attemptState.getAttemptId(),
              attemptStateData);
          notifyDoneUpdatingApplicationAttempt(attemptState.getAttemptId(),
              storedException);
        }
      } catch (Exception e) {
        LOG.error(
            "Error storing appAttempt: " + attemptState.getAttemptId(), e);
        notifyStoreOperationFailed(e);
      }
    } else if (event.getType().equals(RMStateStoreEventType.REMOVE_APP)) {
      ApplicationState appState =
          ((RMStateStoreRemoveAppEvent) event).getAppState();
      ApplicationId appId = appState.getAppId();
      LOG.info("Removing info for app: " + appId);
      try {
        removeApplicationStateInternal(appState);
      } catch (Exception e) {
        LOG.error("Error removing app: " + appId, e);
        notifyStoreOperationFailed(e);
      }
    } else {
      LOG.error("Unknown RMStateStoreEvent type: " + event.getType());
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * This method is called to notify the ResourceManager that the store
   * operation has failed.
   * @param failureCause the exception due to which the operation failed
   */
  protected void notifyStoreOperationFailed(Exception failureCause) {
    RMFatalEventType type;
    if (failureCause instanceof StoreFencedException) {
      type = RMFatalEventType.STATE_STORE_FENCED;
    } else {
      type = RMFatalEventType.STATE_STORE_OP_FAILED;
    }
    rmDispatcher.getEventHandler().handle(new RMFatalEvent(type, failureCause));
  }

  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application that new application is stored in state store
   * @param appId id of the application that has been saved
   * @param storedException the exception that is thrown when storing the
   * application
   */
  private void notifyDoneStoringApplication(ApplicationId appId,
                                                  Exception storedException) {
    rmDispatcher.getEventHandler().handle(
        new RMAppNewSavedEvent(appId, storedException));
  }

  @SuppressWarnings("unchecked")
  private void notifyDoneUpdatingApplication(ApplicationId appId,
      Exception storedException) {
    rmDispatcher.getEventHandler().handle(
      new RMAppUpdateSavedEvent(appId, storedException));
  }

  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application attempt that new attempt is stored in state store
   * @param appAttempt attempt that has been saved
   */
  private void notifyDoneStoringApplicationAttempt(ApplicationAttemptId attemptId,
                                                  Exception storedException) {
    rmDispatcher.getEventHandler().handle(
        new RMAppAttemptNewSavedEvent(attemptId, storedException));
  }

  @SuppressWarnings("unchecked")
  private void notifyDoneUpdatingApplicationAttempt(ApplicationAttemptId attemptId,
      Exception updatedException) {
    rmDispatcher.getEventHandler().handle(
      new RMAppAttemptUpdateSavedEvent(attemptId, updatedException));
  }

  /**
   * EventHandler implementation which forward events to the FSRMStateStore
   * This hides the EventHandle methods of the store from its public interface 
   */
  private final class ForwardingEventHandler 
                                  implements EventHandler<RMStateStoreEvent> {
    
    @Override
    public void handle(RMStateStoreEvent event) {
      handleStoreEvent(event);
    }
  }
}
