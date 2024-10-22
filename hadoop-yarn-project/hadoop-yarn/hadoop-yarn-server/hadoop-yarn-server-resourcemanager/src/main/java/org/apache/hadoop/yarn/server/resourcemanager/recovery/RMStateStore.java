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

import java.io.ByteArrayInputStream;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerLaunchContextPBImpl;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.event.multidispatcher.MultiDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationAllocationStateProto;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
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
  @VisibleForTesting
  public static final String RM_APP_ROOT = "RMAppRoot";
  protected static final String RM_DT_SECRET_MANAGER_ROOT = "RMDTSecretManagerRoot";
  protected static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
      "RMDelegationTokensRoot";
  protected static final String DELEGATION_KEY_PREFIX = "DelegationKey_";
  protected static final String DELEGATION_TOKEN_PREFIX = "RMDelegationToken_";
  protected static final String DELEGATION_TOKEN_SEQUENCE_NUMBER_PREFIX =
      "RMDTSequenceNumber_";
  protected static final String AMRMTOKEN_SECRET_MANAGER_ROOT =
      "AMRMTokenSecretManagerRoot";
  protected static final String RESERVATION_SYSTEM_ROOT =
      "ReservationSystemRoot";
  protected static final String PROXY_CA_ROOT = "ProxyCARoot";
  protected static final String PROXY_CA_CERT_NODE = "caCert";
  protected static final String PROXY_CA_PRIVATE_KEY_NODE = "caPrivateKey";
  protected static final String VERSION_NODE = "RMVersionNode";
  protected static final String EPOCH_NODE = "EpochNode";
  protected long baseEpoch;
  private long epochRange;
  protected ResourceManager resourceManager;

  public static final Logger LOG =
      LoggerFactory.getLogger(RMStateStore.class);

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
          RMStateStoreEventType.REMOVE_APP_ATTEMPT,
          new RemoveAppAttemptTransition())
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
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_RESERVATION,
          new StoreReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.REMOVE_RESERVATION,
          new RemoveReservationAllocationTransition())
      .addTransition(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED,
          RMStateStoreEventType.FENCED)
      .addTransition(RMStateStoreState.ACTIVE,
          EnumSet.of(RMStateStoreState.ACTIVE, RMStateStoreState.FENCED),
          RMStateStoreEventType.STORE_PROXY_CA_CERT,
          new StoreProxyCACertTransition())
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
          RMStateStoreEventType.UPDATE_AMRM_TOKEN,
          RMStateStoreEventType.STORE_RESERVATION,
          RMStateStoreEventType.REMOVE_RESERVATION,
          RMStateStoreEventType.STORE_PROXY_CA_CERT));

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
        store.notifyApplication(
            new RMAppEvent(appId, RMAppEventType.APP_NEW_SAVED));
      } catch (Exception e) {
        LOG.error("Error storing app: " + appId, e);
        if (e instanceof StoreLimitException) {
          store.notifyApplication(
              new RMAppEvent(appId, RMAppEventType.APP_SAVE_FAILED,
                  e.getMessage()));
        } else {
          isFenced = store.notifyStoreOperationFailedInternal(e);
        }
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
      SettableFuture<Object> result =
          ((RMStateUpdateAppEvent) event).getResult();
      ApplicationId appId =
          appState.getApplicationSubmissionContext().getApplicationId();
      LOG.info("Updating info for app: " + appId);
      try {
        if (isAppStateFinal(appState)) {
          pruneAppState(appState);
        }
        store.updateApplicationStateInternal(appId, appState);
        if (((RMStateUpdateAppEvent) event).isNotifyApplication()) {
          store.notifyApplication(new RMAppEvent(appId,
              RMAppEventType.APP_UPDATE_SAVED));
        }

        if (result != null) {
          result.set(null);
        }

      } catch (Exception e) {
        String msg = "Error updating app: " + appId;
        LOG.error(msg, e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
        if (result != null) {
          result.setException(new YarnException(msg, e));
        }
      }
      return finalState(isFenced);
    }

    private boolean isAppStateFinal(ApplicationStateData appState) {
      RMAppState state = appState.getState();
      return state == RMAppState.FINISHED || state == RMAppState.FAILED ||
          state == RMAppState.KILLED;
    }

    private void pruneAppState(ApplicationStateData appState) {
      ApplicationSubmissionContext srcCtx =
          appState.getApplicationSubmissionContext();
      ApplicationSubmissionContextPBImpl context =
          new ApplicationSubmissionContextPBImpl();
      // most fields in the ApplicationSubmissionContext are not needed,
      // but the following few need to be present for recovery to succeed
      context.setApplicationId(srcCtx.getApplicationId());
      context.setResource(srcCtx.getResource());
      context.setQueue(srcCtx.getQueue());
      context.setAMContainerResourceRequests(
          srcCtx.getAMContainerResourceRequests());
      context.setApplicationName(srcCtx.getApplicationName());
      context.setPriority(srcCtx.getPriority());
      context.setApplicationTags(srcCtx.getApplicationTags());
      context.setApplicationType(srcCtx.getApplicationType());
      context.setUnmanagedAM(srcCtx.getUnmanagedAM());
      context.setNodeLabelExpression(srcCtx.getNodeLabelExpression());
      ContainerLaunchContextPBImpl amContainerSpec =
              new ContainerLaunchContextPBImpl();
      amContainerSpec.setApplicationACLs(
              srcCtx.getAMContainerSpec().getApplicationACLs());
      context.setAMContainerSpec(amContainerSpec);
      appState.setApplicationSubmissionContext(context);
    }
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
        LOG.debug("Storing info for attempt: {}", attemptState.getAttemptId());
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
        LOG.debug("Updating info for attempt: {}",
            attemptState.getAttemptId());
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

  private static class StoreReservationAllocationTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreStoreReservationEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreStoreReservationEvent reservationEvent =
          (RMStateStoreStoreReservationEvent) event;
      try {
        LOG.info("Storing reservation allocation." + reservationEvent
                .getReservationIdName());
        store.storeReservationState(
            reservationEvent.getReservationAllocation(),
            reservationEvent.getPlanName(),
            reservationEvent.getReservationIdName());
      } catch (Exception e) {
        LOG.error("Error while storing reservation allocation.", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class RemoveReservationAllocationTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreStoreReservationEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreStoreReservationEvent reservationEvent =
          (RMStateStoreStoreReservationEvent) event;
      try {
        LOG.info("Removing reservation allocation." + reservationEvent
                .getReservationIdName());
        store.removeReservationState(
            reservationEvent.getPlanName(),
            reservationEvent.getReservationIdName());
      } catch (Exception e) {
        LOG.error("Error while removing reservation allocation.", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static class StoreProxyCACertTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreProxyCAEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      RMStateStoreProxyCAEvent caEvent = (RMStateStoreProxyCAEvent) event;
      try {
        LOG.info("Storing CA Certificate and Private Key");
        store.storeProxyCACertState(
            caEvent.getCaCert(), caEvent.getCaPrivateKey());
      } catch (Exception e) {
        LOG.error("Error While Storing CA Certificate and Private Key", e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  private static RMStateStoreState finalState(boolean isFenced) {
    return isFenced ? RMStateStoreState.FENCED : RMStateStoreState.ACTIVE;
  }

  private static class RemoveAppAttemptTransition implements
      MultipleArcTransition<RMStateStore, RMStateStoreEvent,
          RMStateStoreState> {
    @Override
    public RMStateStoreState transition(RMStateStore store,
        RMStateStoreEvent event) {
      if (!(event instanceof RMStateStoreRemoveAppAttemptEvent)) {
        // should never happen
        LOG.error("Illegal event type: " + event.getClass());
        return RMStateStoreState.ACTIVE;
      }
      boolean isFenced = false;
      ApplicationAttemptId attemptId =
          ((RMStateStoreRemoveAppAttemptEvent) event).getApplicationAttemptId();
      ApplicationId appId = attemptId.getApplicationId();
      LOG.info("Removing attempt " + attemptId + " from app: " + appId);
      try {
        store.removeApplicationAttemptInternal(attemptId);
      } catch (Exception e) {
        LOG.error("Error removing attempt: " + attemptId, e);
        isFenced = store.notifyStoreOperationFailedInternal(e);
      }
      return finalState(isFenced);
    }
  }

  public RMStateStore() {
    super(RMStateStore.class.getName());
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

  public static class ProxyCAState {
    private X509Certificate caCert;
    private PrivateKey caPrivateKey;

    public X509Certificate getCaCert() {
      return caCert;
    }

    public PrivateKey getCaPrivateKey() {
      return caPrivateKey;
    }

    public void setCaCert(X509Certificate caCert) {
      this.caCert = caCert;
    }

    public void setCaPrivateKey(PrivateKey caPrivateKey) {
      this.caPrivateKey = caPrivateKey;
    }

    public void setCaCert(byte[] caCertData) throws CertificateException {
      ByteArrayInputStream bais = new ByteArrayInputStream(caCertData);
      caCert = (X509Certificate)
          CertificateFactory.getInstance("X.509").generateCertificate(bais);
    }

    public void setCaPrivateKey(byte[] caPrivateKeyData)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
      caPrivateKey = KeyFactory.getInstance("RSA").generatePrivate(
          new PKCS8EncodedKeySpec(caPrivateKeyData));
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

    private Map<String, Map<ReservationId, ReservationAllocationStateProto>>
        reservationState = new TreeMap<>();

    ProxyCAState proxyCAState = new ProxyCAState();

    public Map<ApplicationId, ApplicationStateData> getApplicationState() {
      return appState;
    }

    public RMDTSecretManagerState getRMDTSecretManagerState() {
      return rmSecretManagerState;
    }

    public AMRMTokenSecretManagerState getAMRMTokenSecretManagerState() {
      return amrmTokenSecretManagerState;
    }

    public Map<String, Map<ReservationId, ReservationAllocationStateProto>>
        getReservationState() {
      return reservationState;
    }

    public ProxyCAState getProxyCAState() {
      return proxyCAState;
    }
  }
    
  private Dispatcher rmDispatcher;

  /**
   * Dispatcher used to send state operation completion events to 
   * ResourceManager services.
   *
   * @param dispatcher Dispatcher.
   */
  public void setRMDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }
  
  MultiDispatcher dispatcher;
  @SuppressWarnings("rawtypes")
  @VisibleForTesting
  protected EventHandler rmStateStoreEventHandler;

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    dispatcher = new MultiDispatcher("RM State Store");
    dispatcher.init(conf);
    rmStateStoreEventHandler = new ForwardingEventHandler();
    dispatcher.register(RMStateStoreEventType.class, 
                        rmStateStoreEventHandler);
    // read the base epoch value from conf
    baseEpoch = conf.getLong(YarnConfiguration.RM_EPOCH,
        YarnConfiguration.DEFAULT_RM_EPOCH);
    epochRange = conf.getLong(YarnConfiguration.RM_EPOCH_RANGE,
        YarnConfiguration.DEFAULT_RM_EPOCH_RANGE);
    initInternal(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dispatcher.start();
    startInternal();
  }

  /**
   * Derived classes initialize themselves using this method.
   *
   * @param conf Configuration.
   * @throws Exception error occur.
   */
  protected abstract void initInternal(Configuration conf) throws Exception;

  /**
   * Derived classes start themselves using this method.
   * The base class is started and the event dispatcher is ready to use at
   * this point.
   *
   * @throws Exception error occur.
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
   * after this.
   *
   * @throws Exception error occur.
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
   *
   * @throws Exception error occur.
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
   * @throws Exception error occur.
   * @return current version.
   */
  protected abstract Version loadVersion() throws Exception;

  /**
   * Derived class use this method to store the version information.
   * @throws Exception error occur.
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * Get the current version of the underlying state store.
   * @return current version.
   */
  protected abstract Version getCurrentVersion();


  /**
   * Get the current epoch of RM and increment the value.
   * @throws Exception error occur.
   * @return current epoch.
   */
  public abstract long getAndIncrementEpoch() throws Exception;

  /**
   * Compute the next epoch value by incrementing by one.
   * Wraps around if the epoch range is exceeded so that
   * when federation is enabled epoch collisions can be avoided.
   *
   * @param epoch epoch value.
   * @return next epoch value.
   */
  protected long nextEpoch(long epoch){
    long epochVal = epoch - baseEpoch + 1;
    if (epochRange > 0) {
      epochVal %= epochRange;
    }
    return  epochVal + baseEpoch;
  }

  /**
   * Blocking API
   * The derived class must recover state from the store and return a new 
   * RMState object populated with that state
   * This must not be called on the dispatcher thread.
   * @throws Exception error occur.
   * @return RMState.
   */
  public abstract RMState loadState() throws Exception;
  
  /**
   * Non-Blocking API
   * ResourceManager services use this to store the application's state
   * This does not block the dispatcher threads
   * RMAppStoredEvent will be sent on completion to notify the RMApp.
   *
   * @param app rmApp.
   */
  @SuppressWarnings("unchecked")
  public void storeNewApplication(RMApp app) {
    ApplicationSubmissionContext context = app
                                            .getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;
    ApplicationStateData appState =
        ApplicationStateData.newInstance(app.getSubmitTime(),
            app.getStartTime(), context, app.getUser(), app.getRealUser(),
            app.getCallerContext());
    appState.setApplicationTimeouts(app.getApplicationTimeouts());
    getRMStateStoreEventHandler().handle(new RMStateStoreAppEvent(appState));
  }

  @SuppressWarnings("unchecked")
  public void updateApplicationState(ApplicationStateData appState) {
    getRMStateStoreEventHandler().handle(new RMStateUpdateAppEvent(appState));
  }

  @SuppressWarnings("unchecked")
  public void updateApplicationState(ApplicationStateData appState,
      boolean notifyApp) {
    getRMStateStoreEventHandler().handle(new RMStateUpdateAppEvent(appState,
        notifyApp));
  }

  public void updateApplicationStateSynchronously(ApplicationStateData appState,
      boolean notifyApp, SettableFuture<Object> resultFuture) {
    handleStoreEvent(
        new RMStateUpdateAppEvent(appState, notifyApp, resultFuture));
  }

  public void updateFencedState() {
    handleStoreEvent(new RMStateStoreEvent(RMStateStoreEventType.FENCED));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application.
   *
   * @param appId application Id.
   * @param appStateData application StateData.
   * @throws Exception error occur.
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
   * RMAppAttemptStoredEvent will be sent on completion to notify the RMAppAttempt.
   *
   * @param appAttempt RM AppAttempt.
   */
  public void storeNewApplicationAttempt(RMAppAttempt appAttempt) {
    Credentials credentials = getCredentialsFromAppAttempt(appAttempt);
    RMAppAttemptMetrics attempMetrics = appAttempt.getRMAppAttemptMetrics();
    AggregateAppResourceUsage resUsage =
        attempMetrics.getAggregateAppResourceUsage();
    ApplicationAttemptStateData attemptState =
        ApplicationAttemptStateData.newInstance(
            appAttempt.getAppAttemptId(),
            appAttempt.getMasterContainer(),
            credentials, appAttempt.getStartTime(),
            resUsage.getResourceUsageSecondsMap(),
            attempMetrics.getPreemptedResourceSecondsMap(),
            attempMetrics.getTotalAllocatedContainers());

    getRMStateStoreEventHandler().handle(
      new RMStateStoreAppAttemptEvent(attemptState));
  }

  @SuppressWarnings("unchecked")
  public void updateApplicationAttemptState(
      ApplicationAttemptStateData attemptState) {
    getRMStateStoreEventHandler().handle(
      new RMStateUpdateAppAttemptEvent(attemptState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of an 
   * application attempt.
   *
   * @param attemptId Application AttemptId.
   * @param attemptStateData Application AttemptStateData.
   * @throws Exception error occur.
   */
  protected abstract void storeApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws Exception;

  protected abstract void updateApplicationAttemptStateInternal(
      ApplicationAttemptId attemptId,
      ApplicationAttemptStateData attemptStateData) throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a delegation token
   * and sequence number.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   * @param renewDate token renew date.
   */
  public void storeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate,
        RMStateStoreEventType.STORE_DELEGATION_TOKEN));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * RMDelegationToken and sequence number.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   * @param renewDate token renew date.
   * @throws Exception error occur.
   */
  protected abstract void storeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a delegation token.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   */
  public void removeRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, null,
        RMStateStoreEventType.REMOVE_DELEGATION_TOKEN));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of RMDelegationToken.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   * @throws Exception error occurs.
   */
  protected abstract void removeRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier) throws Exception;

  /**
   * RMDTSecretManager call this to update the state of a delegation token
   * and sequence number.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   * @param renewDate token renew date.
   */
  public void updateRMDelegationToken(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate) {
    handleStoreEvent(new RMStateStoreRMDTEvent(rmDTIdentifier, renewDate,
        RMStateStoreEventType.UPDATE_DELEGATION_TOKEN));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to update the state of
   * RMDelegationToken and sequence number.
   *
   * @param rmDTIdentifier RMDelegationTokenIdentifier.
   * @param renewDate token renew date.
   * @throws Exception error occurs.
   */
  protected abstract void updateRMDelegationTokenState(
      RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate)
      throws Exception;

  /**
   * RMDTSecretManager call this to store the state of a master key.
   *
   * @param delegationKey DelegationToken Master Key.
   */
  public void storeRMDTMasterKey(DelegationKey delegationKey) {
    handleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey,
        RMStateStoreEventType.STORE_MASTERKEY));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * DelegationToken Master Key.
   *
   * @param delegationKey DelegationToken Master Key.
   * @throws Exception error occur.
   */
  protected abstract void storeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * RMDTSecretManager call this to remove the state of a master key.
   *
   * @param delegationKey DelegationToken Master Key.
   */
  public void removeRMDTMasterKey(DelegationKey delegationKey) {
    handleStoreEvent(new RMStateStoreRMDTMasterKeyEvent(delegationKey,
        RMStateStoreEventType.REMOVE_MASTERKEY));
  }

  /**
   * Blocking Apis to maintain reservation state.
   *
   * @param reservationAllocation reservation Allocation.
   * @param planName plan Name.
   * @param reservationIdName reservationId Name.
   */
  public void storeNewReservation(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) {
    handleStoreEvent(new RMStateStoreStoreReservationEvent(
        reservationAllocation, RMStateStoreEventType.STORE_RESERVATION,
        planName, reservationIdName));
  }

  public void removeReservation(String planName, String reservationIdName) {
    handleStoreEvent(new RMStateStoreStoreReservationEvent(
            null, RMStateStoreEventType.REMOVE_RESERVATION,
            planName, reservationIdName));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the state of
   * a reservation allocation.
   *
   * @param reservationAllocation reservation Allocation.
   * @param planName plan Name.
   * @param reservationIdName reservationId Name.
   * @throws Exception error occurs.
   */
  protected abstract void storeReservationState(
      ReservationAllocationStateProto reservationAllocation, String planName,
      String reservationIdName) throws Exception;

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * a reservation allocation.
   *
   * @param planName plan Name.
   * @param reservationIdName reservationId Name.
   * @throws Exception exception occurs.
   */
  protected abstract void removeReservationState(String planName,
      String reservationIdName) throws Exception;

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of
   * DelegationToken Master Key.
   *
   * @param delegationKey DelegationKey.
   * @throws Exception exception occurs.
   */
  protected abstract void removeRMDTMasterKeyState(DelegationKey delegationKey)
      throws Exception;

  /**
   * Blocking API Derived classes must implement this method to store or update
   * the state of AMRMToken Master Key.
   *
   * @param amrmTokenSecretManagerState amrmTokenSecretManagerState.
   * @param isUpdate true, update; otherwise not update.
   * @throws Exception exception occurs.
   */
  protected abstract void storeOrUpdateAMRMTokenSecretManagerState(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState, boolean isUpdate)
      throws Exception;

  /**
   * Store or Update state of AMRMToken Master Key.
   *
   * @param amrmTokenSecretManagerState amrmTokenSecretManagerState.
   * @param isUpdate true, update; otherwise not update.
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
   *
   * @param app RMApp.
   */
  @SuppressWarnings("unchecked")
  public void removeApplication(RMApp app) {
    ApplicationStateData appState =
        ApplicationStateData.newInstance(app.getSubmitTime(),
            app.getStartTime(), app.getApplicationSubmissionContext(),
            app.getUser(), app.getRealUser(), app.getCallerContext());
    for(RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      appState.attempts.put(appAttempt.getAppAttemptId(), null);
    }
    
    getRMStateStoreEventHandler().handle(
        new RMStateStoreRemoveAppEvent(appState));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of an 
   * application and its attempts.
   *
   * @param appState ApplicationStateData.
   * @throws Exception error occurs.
   */
  protected abstract void removeApplicationStateInternal(
      ApplicationStateData appState) throws Exception;

  /**
   * Non-blocking API
   * ResourceManager services call this to remove an attempt from the state
   * store
   * This does not block the dispatcher threads
   * There is no notification of completion for this operation.
   *
   * @param applicationAttemptId applicationAttemptId.
   */
  @SuppressWarnings("unchecked")
  public synchronized void removeApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    getRMStateStoreEventHandler().handle(
        new RMStateStoreRemoveAppAttemptEvent(applicationAttemptId));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to remove the state of specified
   * attempt.
   * @param attemptId application attempt id.
   * @throws Exception exception occurs.
   */
  protected abstract void removeApplicationAttemptInternal(
      ApplicationAttemptId attemptId) throws Exception;


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
    try {

      LOG.debug("Processing event of type {}", event.getType());

      final RMStateStoreState oldState = getRMStateStoreState();

      this.stateMachine.doTransition(event.getType(), event);

      if (oldState != getRMStateStoreState()) {
        LOG.info("RMStateStore state change from " + oldState + " to "
            + getRMStateStoreState());
      }

    } catch (InvalidStateTransitionException e) {
      LOG.error("Can't handle this event at current state", e);
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
      rmDispatcher.getEventHandler().handle(
          new RMFatalEvent(RMFatalEventType.STATE_STORE_FENCED,
              failureCause));
      isFenced = true;
    } else {
      rmDispatcher.getEventHandler().handle(
          new RMFatalEvent(RMFatalEventType.STATE_STORE_OP_FAILED,
              failureCause));
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
   * Derived classes must implement this method to delete the state store.
   *
   * @throws Exception exception occurs.
   */
  public abstract void deleteStore() throws Exception;

  /**
   * Derived classes must implement this method to remove application from the
   * state store.
   *
   * @param removeAppId application Id.
   * @throws Exception exception occurs.
   */
  public abstract void removeApplication(ApplicationId removeAppId)
      throws Exception;

  public void setResourceManager(ResourceManager rm) {
    this.resourceManager = rm;
  }

  public RMStateStoreState getRMStateStoreState() {
    return this.stateMachine.getCurrentState();
  }

  @SuppressWarnings("rawtypes")
  protected EventHandler getRMStateStoreEventHandler() {
    return dispatcher.getEventHandler();
  }

  /**
   * ProxyCAManager calls this to store the CA Certificate and Private Key.
   * @param caCert X509Certificate.
   * @param caPrivateKey PrivateKey.
   */
  public void storeProxyCACert(X509Certificate caCert,
      PrivateKey caPrivateKey) {
    handleStoreEvent(new RMStateStoreProxyCAEvent(caCert, caPrivateKey,
        RMStateStoreEventType.STORE_PROXY_CA_CERT));
  }

  /**
   * Blocking API
   * Derived classes must implement this method to store the CA Certificate
   * and Private Key.
   *
   * @param caCert X509Certificate.
   * @param caPrivateKey PrivateKey.
   * @throws Exception error occurs.
   */
  protected abstract void storeProxyCACertState(
      X509Certificate caCert, PrivateKey caPrivateKey) throws Exception;
}
