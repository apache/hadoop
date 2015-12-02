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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMFatalEventType;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

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
  protected static final String AMRMTOKEN_SECRET_MANAGER_ROOT =
      "AMRMTokenSecretManagerRoot";
  protected static final String VERSION_NODE = "RMVersionNode";
  protected static final String EPOCH_NODE = "EpochNode";
  private ResourceManager resourceManager;
  private final ReadLock readLock;
  private final WriteLock writeLock;

  public static final Log LOG = LogFactory.getLog(RMStateStore.class);

  /**
   * The enum defines state of RMStateStore.
   */
  public enum RMStateStoreState {
    ACTIVE,
    FENCED
  };

  private static final StateMachineFactory<RMStateStore,
                                           RMStateStoreState,
                                           RMStateStoreEventType, 
                                           RMStateStoreEvent>
      stateMachineFactory = new StateMachineFactory<RMStateStore,
                                                    RMStateStoreState,
                                                    RMStateStoreEventType,
                                                    RMStateStoreEvent>(
      RMStateStoreState.ACTIVE)
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_APP, new StoreAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_APP, new UpdateAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_APP, new RemoveAppTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_APP_ATTEMPT,
          new StoreAppAttemptTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_APP_ATTEMPT,
          new UpdateAppAttemptTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_MASTERKEY,
          new StoreRMDTMasterKeyTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_MASTERKEY,
          new RemoveRMDTMasterKeyTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_DELEGATION_TOKEN,
          new StoreRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_DELEGATION_TOKEN,
          new RemoveRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_DELEGATION_TOKEN,
          new UpdateRMDTTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.UPDATE_AMRM_TOKEN,
          new StoreOrUpdateAMRMTokenTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED,
          RMStateStoreEventType.FENCED)
      .addTransition(RMStateStoreState.FENCED, RMStateStoreState.FENCED,
          EnumSet.of(
          RMStateStoreEventType.STORE_APP,
          RMStateStoreEventType.UPDATE_APP,
          RMStateStoreEventType.REMOVE_APP,
          RMStateStoreEventType.STORE_APP_ATTEMPT,
          RMStateStoreEventType.UPDATE_APP_ATTEMPT,
          RMStateStoreEventType.FENCED,
          RMStateStoreEventType.STORE_MASTERKEY,
          RMStateStoreEventType.REMOVE_MASTERKEY,
          RMStateStoreEventType.STORE_DELEGATION_TOKEN,
          RMStateStoreEventType.REMOVE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_DELEGATION_TOKEN,
          RMStateStoreEventType.UPDATE_AMRM_TOKEN));

  private final StateMachine<RMStateStoreState,
                             RMStateStoreEventType,
                             RMStateStoreEvent> stateMachine;

  private static class StoreAppTransition
      implements MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreAppEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateStoreAppEvent) event).getAppState();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Storing info for app: " + appId);
      try {
        store.storeApplicationStateInternal(appId, appState);
        store.notifyApplication(new RMAppEvent(appId,
               RMAppEventType.APP_NEW_SAVED));
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    };
  }

  private static class UpdateAppTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateUpdateAppEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateUpdateAppEvent) event).getAppState();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Updating info for app: " + appId);
      try {
        store.updateApplicationStateInternal(appId, appState);
        store.notifyApplication(new RMAppEvent(appId,
            RMAppEventType.APP_UPDATE_SAVED));
      } catch (Exception e) {
        LOG.error("Error updating app: " + appId, e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    };
  }

  private static class RemoveAppTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRemoveAppEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationStateData appState =
          ((RMStateStoreRemoveAppEvent) event).getAppState();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Removing info for app: " + appId);
      try {
        store.removeApplicationStateInternal(appState);
      } catch (Exception e) {
        LOG.error("Error removing app: " + appId, e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    };
  }

  private static class StoreAppAttemptTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreAppAttemptEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationAttemptStateData attemptState =
          ((RMStateStoreAppAttemptEvent) event).getAppAttemptState();
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Storing info for attempt: " + attemptState.getAttemptId());
        }
        store.storeApplicationAttemptStateInternal(attemptState.getAttemptId(),
            attemptState);
        store.notifyApplicationAttempt(new RMAppAttemptEvent
               (attemptState.getAttemptId(),
               RMAppAttemptEventType.ATTEMPT_NEW_SAVED));
      } catch (Exception e) {
        LOG.error("Error storing appAttempt: " + attemptState.getAttemptId(), e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    };
  }

  private static class UpdateAppAttemptTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateUpdateAppAttemptEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationAttemptStateData attemptState =
          ((RMStateUpdateAppAttemptEvent) event).getAppAttemptState();
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updating info for attempt: " + attemptState.getAttemptId());
        }
        store.updateApplicationAttemptStateInternal(attemptState.getAttemptId(),
            attemptState);
        store.notifyApplicationAttempt(new RMAppAttemptEvent
               (attemptState.getAttemptId(),
               RMAppAttemptEventType.ATTEMPT_UPDATE_SAVED));
      } catch (Exception e) {
        LOG.error("Error updating appAttempt: " + attemptState.getAttemptId(), e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    };
  }

  private static class StoreRMDTTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRMDTEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent) event;
      try {
        LOG.info("Storing RMDelegationToken and SequenceNumber");
        store.storeRMDelegationTokenState(
            dtEvent.getRmDTIdentifier(), dtEvent.getRenewDate());
      } catch (Exception e) {
        LOG.error("Error While Storing RMDelegationToken and SequenceNumber ",
            e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class RemoveRMDTTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRMDTEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent) event;
      try {
        LOG.info("Removing RMDelegationToken and SequenceNumber");
        store.removeRMDelegationTokenState(dtEvent.getRmDTIdentifier());
      } catch (Exception e) {
        LOG.error("Error While Removing RMDelegationToken and SequenceNumber ",
            e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class UpdateRMDTTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRMDTEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreRMDTEvent dtEvent = (RMStateStoreRMDTEvent) event;
      try {
        LOG.info("Updating RMDelegationToken and SequenceNumber");
        store.updateRMDelegationTokenState(
            dtEvent.getRmDTIdentifier(), dtEvent.getRenewDate());
      } catch (Exception e) {
        LOG.error("Error While Updating RMDelegationToken and SequenceNumber ",
            e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class StoreRMDTMasterKeyTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRMDTMasterKeyEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreRMDTMasterKeyEvent dtEvent =
          (RMStateStoreRMDTMasterKeyEvent) event;
      try {
        LOG.info("Storing RMDTMasterKey.");
        store.storeRMDTMasterKeyState(dtEvent.getDelegationKey());
      } catch (Exception e) {
        LOG.error("Error While Storing RMDTMasterKey.", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class RemoveRMDTMasterKeyTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRMDTMasterKeyEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreRMDTMasterKeyEvent dtEvent =
          (RMStateStoreRMDTMasterKeyEvent) event;
      try {
        LOG.info("Removing RMDTMasterKey.");
        store.removeRMDTMasterKeyState(dtEvent.getDelegationKey());
      } catch (Exception e) {
        LOG.error("Error While Removing RMDTMasterKey.", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class StoreOrUpdateAMRMTokenTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreAMRMTokenEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      RMStateStoreAMRMTokenEvent amrmEvent = (RMStateStoreAMRMTokenEvent) event;
      boolean isFenced = false;
      try {
        LOG.info("Updating AMRMToken");
        store.storeOrUpdateAMRMTokenSecretManagerState(
            amrmEvent.getAmrmTokenSecretManagerState(), amrmEvent.isUpdate());
      } catch (Exception e) {
        LOG.error("Error storing info for AMRMTokenSecretManager", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static RMStateStoreState finalState(boolean isFenced) {
    return isFenced ? RMStateStoreState.FENCED : RMStateStoreState.ACTIVE;
  }

  public RMStateStore() {
    super(RMStateStore.class.getName());
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
    stateMachine = stateMachineFactory.make(this);
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
    Map<ApplicationId, ApplicationStateData> appState =
        new TreeMap<ApplicationId, ApplicationStateData>();

    RMDTSecretManagerState rmSecretManagerState = new RMDTSecretManagerState();

    AMRMTokenSecretManagerState amrmTokenSecretManagerState = null;

    public Map<ApplicationId, ApplicationStateData> getApplicationState() {
      return appState;
    }

    public RMDTSecretManagerState getRMDTSecretManagerState() {
      return rmSecretManagerState;
    }

    public AMRMTokenSecretManagerState getAMRMTokenSecretManagerState() {
      return amrmTokenSecretManagerState;
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
    dispatcher.stop();
    closeInternal();
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
   * 3) If theres's no version, treat it as CURRENT_VERSION_INFO.
   * 4) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 5) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade RM state.
   */
  public void checkVersion() throws Exception {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded RM state version info " + loadedVersion);
    if (loadedVersion != null && loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    // if there is no version info, treat it as CURRENT_VERSION_INFO;
    if (loadedVersion == null) {
      loadedVersion = getCurrentVersion();
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
  protected abstract Version loadVersion() throws Exception;

  /**
   * Derived class use this method to store the version information.
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * Get the current version of the underlying state store.
   */
  protected abstract Version getCurrentVersion();


  /**
   * Get the current epoch of RM and increment the value.
   */
  public abstract long getAndIncrementEpoch() throws Exception;
  
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
  public void storeNewApplication(RMApp app) {
    ApplicationSubmissionContext context = app
                                            .getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationStateData appState =
        ApplicationStateData.newInstance(
            app.getSubmitTime(), app.getStartTime(), context, app.getUser());
    dispatcher.getEventHandler().handle(new RMStateStoreAppEvent(appState));
  }

  @SuppressWarnings("unchecked")
  public void updateApplicationState(
      ApplicationStateData appState) {
    dispatcher.getEventHandler().handle(new RMStateUpdateAppEvent(appState));
  }

  public void updateFencedState() {
    handleStoreEvent(new RMStateStoreEvent(RMStateStoreEventType.FENCED));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application.
   */
  protected abstract void storeApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws Exception;

  protected abstract void updateApplicationStateInternal(ApplicationId appId,
      ApplicationStateData appStateData) throws Exception;
  
  @SuppressWarnings("unchecked")
  /**
   * Non-blocking API
   * ResourceManager services call this to store state on an application attempt
   * This does not block the dispatcher threads
   * RMAppAttemptStoredEvent will be sent on completion to notify the RMAppAttempt
   */
  public void storeNewApplicationAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = getCredentialsFromAppAttempt(appAttempt);

    AggregateAppResourceUsage resUsage =
        appAttempt.getRMAppAttemptMetrics().getAggregateAppResourceUsage();
    ApplicationAttemptStateData attemptState =
        ApplicationAttemptStateData.newInstance(
            appAttempt.getAppAttemptId(),
            appAttempt.getMasterContainer(),
            credentials, appAttempt.getStartTime(),
            resUsage.getMemorySeconds(),
            resUsage.getVcoreSeconds());

    dispatcher.getEventHandler().handle(
      new RMStateStoreAppAttemptEvent(attemptState));
  }

  @SuppressWarnings("unchecked")
  public void updateApplicationAttemptState(
      ApplicationAttemptStateData attemptState) {
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
      ApplicationAttemptStateData attemptStateData) throws Exception;

  protected abstract void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a delegation token
   * and sequence number
   */
  public void storeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate,
        RMStateStoreEventType.STORE_DELEGATION_TOKEN));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a delegation token
   */
  public void removeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, null,
        RMStateStoreEventType.REMOVE_DELEGATION_TOKEN));
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
  public void updateRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate,
        RMStateStoreEventType.UPDATE_DELEGATION_TOKEN));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to update the state of
   * RMDelegationToken and sequence number
   */
  protected abstract void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a master key
   */
  public void storeRMDTMasterKey(DelegationKey delegationKey) {
    handleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey,
        RMStateStoreEventType.STORE_MASTERKEY));
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
  public void removeRMDTMasterKey(DelegationKey delegationKey) {
    handleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey,
        RMStateStoreEventType.REMOVE_MASTERKEY));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * DelegationToken Master Key
   */
  protected abstract void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * Blocking API Derived classes must implement this method to store or update
   * the state of AMRMToken Master Key
   */
  protected abstract void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate)
      throws Exception;

  /**
   * Store or Update state of AMRMToken Master Key
   */
  public void storeOrUpdateAMRMTokenSecretManager(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate) {
    handleStoreEvent(new RMStateStoreAMRMTokenEvent(
        amrmTokenSecretManagerState, isUpdate,
        RMStateStoreEventType.UPDATE_AMRM_TOKEN));
  }

  /**
   * Non-blocking API
   * ResourceManager services call this to remove an application from the state
   * store
   * This does not block the dispatcher threads
   * There is no notification of completion for this operation.
   */
  @SuppressWarnings("unchecked")
  public void removeApplication(RMApp app) {
    ApplicationStateData appState =
        ApplicationStateData.newInstance(
            app.getSubmitTime(), app.getStartTime(),
            app.getApplicationSubmissionContext(), app.getUser());
    for(RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      appState.attempts.put(appAttempt.getAppAttemptId(), null);
    }
    
    dispatcher.getEventHandler().handle(new RMStateStoreRemoveAppEvent(appState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of an 
   * application and its attempts
   */
  protected abstract void removeApplicationStateInternal(
      ApplicationStateData appState) throws Exception;

  // TODO: This should eventually become cluster-Id + "AM_RM_TOKEN_SERVICE". See
  // YARN-1779
  public static final Text AM_RM_TOKEN_SERVICE = new Text(
    "AM_RM_TOKEN_SERVICE");

  public static final Text AM_CLIENT_TOKEN_MASTER_KEY_NAME =
      new Text("YARN_CLIENT_TOKEN_MASTER_KEY");
  
  public Credentials getCredentialsFromAppAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = new Credentials();

    SecretKey clientTokenMasterKey =
        appAttempt.getClientTokenMasterKey();
    if(clientTokenMasterKey != null){
      credentials.addSecretKey(AM_CLIENT_TOKEN_MASTER_KEY_NAME,
          clientTokenMasterKey.getEncoded());
    }
    return credentials;
  }
  
  @VisibleForTesting
  protected boolean isFencedState() {
    return (RMStateStoreState.FENCED == getRMStateStoreState());
  }

  // Dispatcher related code
  protected void handleStoreEvent(RMStateStoreEvent event) {
    this.writeLock.lock();
    try {

      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing event of type " + event.getType());
      }

      final RMStateStoreState oldState = getRMStateStoreState();

      this.stateMachine.doTransition(event.getType(), event);

      if (oldState != getRMStateStoreState()) {
        LOG.info("RMStateStore state change from " + oldState + " to "
            + getRMStateStoreState());
      }

    } catch (InvalidStateTransitonException e) {
      LOG.error("Can't handle this event at current state", e);
    } finally {
      this.writeLock.unlock();
    }
  }

  /**
   * This method is called to notify the ResourceManager that the store
   * operation has failed.
   * @param failureCause the exception due to which the operation failed
   */
  protected void notifyStoreOperationFailed(Exception failureCause) {
    if (isFencedState()) {
      return;
    }
    if (notifyStoreOperationFailedInternal(failureCause)) {
      updateFencedState();
    }
  }

  @SuppressWarnings("unchecked")
  private boolean notifyStoreOperationFailedInternal(
      Exception failureCause) {
    boolean isFenced = false;
    LOG.error("State store operation failed ", failureCause);
    if (HAUtil.isHAEnabled(getConfig())) {
      LOG.warn("State-store fenced ! Transitioning RM to standby");
      isFenced = true;
      Thread standByTransitionThread =
          new Thread(new StandByTransitionThread());
      standByTransitionThread.setName("StandByTransitionThread Handler");
      standByTransitionThread.start();
    } else if (YarnConfiguration.shouldRMFailFast(getConfig())) {
      LOG.fatal("Fail RM now due to state-store error!");
      rmDispatcher.getEventHandler().handle(
          new RMFatalEvent(RMFatalEventType.STATE_STORE_OP_FAILED,
              failureCause));
    } else {
      LOG.warn("Skip the state-store error.");
    }
    return isFenced;
  }
 
  @SuppressWarnings("unchecked")
  /**
   * This method is called to notify the application that
   * new application is stored or updated in state store
   * @param event App event containing the app id and event type
   */
  private void notifyApplication(RMAppEvent event) {
    rmDispatcher.getEventHandler().handle(event);
  }
  
  @SuppressWarnings("unchecked")
  /**
   * This method is called to notify the application attempt
   * that new attempt is stored or updated in state store
   * @param event App attempt event containing the app attempt
   * id and event type
   */
  private void notifyApplicationAttempt(RMAppAttemptEvent event) {
    rmDispatcher.getEventHandler().handle(event);
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

  /**
   * Derived classes must implement this method to delete the state store
   * @throws Exception
   */
  public abstract void deleteStore() throws Exception;

  public void setResourceManager(ResourceManager rm) {
    this.resourceManager = rm;
  }

  private class StandByTransitionThread implements Runnable {
    @Override
    public void run() {
      LOG.info("RMStateStore has been fenced");
      resourceManager.handleTransitionToStandBy();
    }
  }

  public RMStateStoreState getRMStateStoreState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }
}
