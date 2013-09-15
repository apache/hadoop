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
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppStoredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRemovedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStoredEvent;

@Private
@Unstable
/**
 * Base class to implement storage of ResourceManager state.
 * Takes care of asynchronous notifications and interfacing with YARN objects.
 * Real store implementations need to derive from it and implement blocking
 * store and load methods to actually store and load the state.
 */
public abstract class RMStateStore extends AbstractService {

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

    public ApplicationAttemptState(ApplicationAttemptId attemptId,
        Container masterContainer,
        Credentials appAttemptCredentials) {
      this.attemptId = attemptId;
      this.masterContainer = masterContainer;
      this.appAttemptCredentials = appAttemptCredentials;
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
  }
  
  /**
   * State of an application application
   */
  public static class ApplicationState {
    final ApplicationSubmissionContext context;
    final long submitTime;
    final String user;
    Map<ApplicationAttemptId, ApplicationAttemptState> attempts =
                  new HashMap<ApplicationAttemptId, ApplicationAttemptState>();
    
    ApplicationState(long submitTime, ApplicationSubmissionContext context,
        String user) {
      this.submitTime = submitTime;
      this.context = context;
      this.user = user;
    }

    public ApplicationId getAppId() {
      return context.getApplicationId();
    }
    public long getSubmitTime() {
      return submitTime;
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
  
  public synchronized void serviceInit(Configuration conf) throws Exception{    
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.register(RMStateStoreEventType.class, 
                        new ForwardingEventHandler());
    initInternal(conf);
  }
  
  protected synchronized void serviceStart() throws Exception {
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

  public synchronized void serviceStop() throws Exception {
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
  public synchronized void storeApplication(RMApp app) {
    ApplicationSubmissionContext context = app
                                            .getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationState appState = new ApplicationState(
        app.getSubmitTime(), context, app.getUser());
    dispatcher.getEventHandler().handle(new RMStateStoreAppEvent(appState));
  }
    
  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application.
   */
  protected abstract void storeApplicationState(String appId,
                                      ApplicationStateDataPBImpl appStateData) 
                                      throws Exception;
  
  @SuppressWarnings("unchecked")
  /**
   * Non-blocking API
   * ResourceManager services call this to store state on an application attempt
   * This does not block the dispatcher threads
   * RMAppAttemptStoredEvent will be sent on completion to notify the RMAppAttempt
   */
  public synchronized void storeApplicationAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = getCredentialsFromAppAttempt(appAttempt);

    ApplicationAttemptState attemptState =
        new ApplicationAttemptState(appAttempt.getAppAttemptId(),
          appAttempt.getMasterContainer(), credentials);

    dispatcher.getEventHandler().handle(
      new RMStateStoreAppAttemptEvent(attemptState));
  }
  
  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application attempt
   */
  protected abstract void storeApplicationAttemptState(String attemptId,
                            ApplicationAttemptStateDataPBImpl attemptStateData) 
                            throws Exception;


  /**
   * RMDTSecretManager call this to store the state of a delegation token
   * and sequence number
   */
  public synchronized void storeRMDelegationTokenAndSequenceNumber(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
      int latestSequenceNumber) throws Exception {
    storeRMDelegationTokenAndSequenceNumberState(rmDTIdentifier, renewDate,
      latestSequenceNumber);
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
      RMDelegationTokenIdentifier rmDTIdentifier, int sequenceNumber)
      throws Exception {
    removeRMDelegationTokenState(rmDTIdentifier);
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of RMDelegationToken
   */
  protected abstract void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a master key
   */
  public synchronized void storeRMDTMasterKey(DelegationKey delegationKey)
      throws Exception {
    storeRMDTMasterKeyState(delegationKey);
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
  public synchronized void removeRMDTMasterKey(DelegationKey delegationKey)
      throws Exception {
    removeRMDTMasterKeyState(delegationKey);
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
  public synchronized void removeApplication(RMApp app) {
    ApplicationState appState = new ApplicationState(
            app.getSubmitTime(), app.getApplicationSubmissionContext(),
            app.getUser());
    for(RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      Credentials credentials = getCredentialsFromAppAttempt(appAttempt);
      ApplicationAttemptState attemptState =
          new ApplicationAttemptState(appAttempt.getAppAttemptId(),
            appAttempt.getMasterContainer(), credentials);
      appState.attempts.put(attemptState.getAttemptId(), attemptState);
    }
    
    removeApplication(appState);
  }
  
  @SuppressWarnings("unchecked")
  /**
   * Non-Blocking API
   */
  public synchronized void removeApplication(ApplicationState appState) {
    dispatcher.getEventHandler().handle(new RMStateStoreRemoveAppEvent(appState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of an 
   * application and its attempts
   */
  protected abstract void removeApplicationState(ApplicationState appState) 
                                                             throws Exception;

  // TODO: This should eventually become cluster-Id + "AM_RM_TOKEN_SERVICE". See
  // YARN-986 
  public static final Text AM_RM_TOKEN_SERVICE = new Text(
    "AM_RM_TOKEN_SERVICE");

  public static final Text AM_CLIENT_TOKEN_MASTER_KEY_NAME =
      new Text("YARN_CLIENT_TOKEN_MASTER_KEY");
  
  private Credentials getCredentialsFromAppAttempt(RMAppAttempt appAttempt) {
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
  
  private synchronized void handleStoreEvent(RMStateStoreEvent event) {
    switch(event.getType()) {
      case STORE_APP:
        {
          ApplicationState apptState =
              ((RMStateStoreAppEvent) event).getAppState();
          Exception storedException = null;
          ApplicationStateDataPBImpl appStateData =
              new ApplicationStateDataPBImpl();
          appStateData.setSubmitTime(apptState.getSubmitTime());
          appStateData.setApplicationSubmissionContext(
              apptState.getApplicationSubmissionContext());
          appStateData.setUser(apptState.getUser());
          ApplicationId appId =
              apptState.getApplicationSubmissionContext().getApplicationId();

          LOG.info("Storing info for app: " + appId);
          try {
            storeApplicationState(appId.toString(), appStateData);
          } catch (Exception e) {
            LOG.error("Error storing app: " + appId, e);
            storedException = e;
          } finally {
            notifyDoneStoringApplication(appId, storedException);
          }
        }
        break;
      case STORE_APP_ATTEMPT:
        {
          ApplicationAttemptState attemptState = 
                    ((RMStateStoreAppAttemptEvent) event).getAppAttemptState();
          Exception storedException = null;

          Credentials credentials = attemptState.getAppAttemptCredentials();
          ByteBuffer appAttemptTokens = null;
          try {
            if(credentials != null){
              DataOutputBuffer dob = new DataOutputBuffer();
                credentials.writeTokenStorageToStream(dob);
              appAttemptTokens =
                  ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            }
            ApplicationAttemptStateDataPBImpl attemptStateData =
              (ApplicationAttemptStateDataPBImpl) ApplicationAttemptStateDataPBImpl
                  .newApplicationAttemptStateData(attemptState.getAttemptId(),
                    attemptState.getMasterContainer(), appAttemptTokens);

            LOG.info("Storing info for attempt: " + attemptState.getAttemptId());
            storeApplicationAttemptState(attemptState.getAttemptId().toString(), 
                                         attemptStateData);
          } catch (Exception e) {
            LOG.error("Error storing appAttempt: " 
                      + attemptState.getAttemptId(), e);
            storedException = e;
          } finally {
            notifyDoneStoringApplicationAttempt(attemptState.getAttemptId(), 
                                                storedException);            
          }
        }
        break;
      case REMOVE_APP:
        {
          ApplicationState appState = 
                          ((RMStateStoreRemoveAppEvent) event).getAppState();
          ApplicationId appId = appState.getAppId();
          Exception removedException = null;
          LOG.info("Removing info for app: " + appId);
          try {
            removeApplicationState(appState);
          } catch (Exception e) {
            LOG.error("Error removing app: " + appId, e);
            removedException = e;
          } finally {
            notifyDoneRemovingApplcation(appId, removedException);
          }
        }
        break;
      default:
        LOG.error("Unknown RMStateStoreEvent type: " + event.getType());
    }
  }

  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application about operation completion
   * @param appId id of the application that has been saved
   * @param storedException the exception that is thrown when storing the
   * application
   */
  private void notifyDoneStoringApplication(ApplicationId appId,
                                                  Exception storedException) {
    rmDispatcher.getEventHandler().handle(
        new RMAppStoredEvent(appId, storedException));
  }
  
  @SuppressWarnings("unchecked")
  /**
   * In (@link handleStoreEvent}, this method is called to notify the
   * application attempt about operation completion
   * @param appAttempt attempt that has been saved
   */
  private void notifyDoneStoringApplicationAttempt(ApplicationAttemptId attemptId,
                                                  Exception storedException) {
    rmDispatcher.getEventHandler().handle(
        new RMAppAttemptStoredEvent(attemptId, storedException));
  }

  @SuppressWarnings("unchecked")
  /**
   * This is to notify RMApp that this application has been removed from
   * RMStateStore
   */
  private void notifyDoneRemovingApplcation(ApplicationId appId,
      Exception removedException) {
    rmDispatcher.getEventHandler().handle(
      new RMAppRemovedEvent(appId, removedException));
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
