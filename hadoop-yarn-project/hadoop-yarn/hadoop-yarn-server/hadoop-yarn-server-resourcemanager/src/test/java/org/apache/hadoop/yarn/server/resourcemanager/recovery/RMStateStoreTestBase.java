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

import org.apache.hadoop.yarn.event.Event;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.crypto.SecretKey;

import org.junit.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMDTSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class RMStateStoreTestBase extends ClientBaseWithFixes{

  public static final Log LOG = LogFactory.getLog(RMStateStoreTestBase.class);

  static class TestDispatcher implements Dispatcher, EventHandler<Event> {

    ApplicationAttemptId attemptId;

    boolean notified = false;

    @SuppressWarnings("rawtypes")
    @Override
    public void register(Class<? extends Enum> eventType,
                         EventHandler handler) {
    }

    @Override
    public void handle(Event event) {
      if (event instanceof RMAppAttemptEvent) {
        RMAppAttemptEvent rmAppAttemptEvent = (RMAppAttemptEvent) event;
        assertEquals(attemptId, rmAppAttemptEvent.getApplicationAttemptId());
      }
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return this;
    }

  }

  public static class StoreStateVerifier {
    void afterStoreApp(RMStateStore store, ApplicationId appId) {}
    void afterStoreAppAttempt(RMStateStore store, ApplicationAttemptId
            appAttId) {}
  }

  interface RMStateStoreHelper {
    RMStateStore getRMStateStore() throws Exception;
    boolean isFinalStateValid() throws Exception;
    void writeVersion(Version version) throws Exception;
    Version getCurrentVersion() throws Exception;
    boolean appExists(RMApp app) throws Exception;
  }

  void waitNotify(TestDispatcher dispatcher) {
    long startTime = System.currentTimeMillis();
    while(!dispatcher.notified) {
      synchronized (dispatcher) {
        try {
          dispatcher.wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if(System.currentTimeMillis() - startTime > 1000*60) {
        fail("Timed out attempt store notification");
      }
    }
    dispatcher.notified = false;
  }

  protected RMApp storeApp(RMStateStore store, ApplicationId appId,
      long submitTime,
      long startTime) throws Exception {
    ApplicationSubmissionContext context =
        new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appId);

    RMApp mockApp = mock(RMApp.class);
    when(mockApp.getApplicationId()).thenReturn(appId);
    when(mockApp.getSubmitTime()).thenReturn(submitTime);
    when(mockApp.getStartTime()).thenReturn(startTime);
    when(mockApp.getApplicationSubmissionContext()).thenReturn(context);
    when(mockApp.getUser()).thenReturn("test");
    store.storeNewApplication(mockApp);
    return mockApp;
  }

  protected ContainerId storeAttempt(RMStateStore store,
      ApplicationAttemptId attemptId,
      String containerIdStr, Token<AMRMTokenIdentifier> appToken,
      SecretKey clientTokenMasterKey, TestDispatcher dispatcher)
      throws Exception {

    RMAppAttemptMetrics mockRmAppAttemptMetrics = 
        mock(RMAppAttemptMetrics.class);
    Container container = new ContainerPBImpl();
    container.setId(ConverterUtils.toContainerId(containerIdStr));
    RMAppAttempt mockAttempt = mock(RMAppAttempt.class);
    when(mockAttempt.getAppAttemptId()).thenReturn(attemptId);
    when(mockAttempt.getMasterContainer()).thenReturn(container);
    when(mockAttempt.getAMRMToken()).thenReturn(appToken);
    when(mockAttempt.getClientTokenMasterKey())
        .thenReturn(clientTokenMasterKey);
    when(mockAttempt.getRMAppAttemptMetrics())
        .thenReturn(mockRmAppAttemptMetrics);
    when(mockRmAppAttemptMetrics.getAggregateAppResourceUsage())
        .thenReturn(new AggregateAppResourceUsage(0, 0));
    dispatcher.attemptId = attemptId;
    store.storeNewApplicationAttempt(mockAttempt);
    waitNotify(dispatcher);
    return container.getId();
  }

  void testRMAppStateStore(RMStateStoreHelper stateStoreHelper)
          throws Exception {
    testRMAppStateStore(stateStoreHelper, new StoreStateVerifier());
  }

  void testRMAppStateStore(RMStateStoreHelper stateStoreHelper,
                           StoreStateVerifier verifier)
      throws Exception {
    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;
    Configuration conf = new YarnConfiguration();
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(store);

    AMRMTokenSecretManager appTokenMgr =
        spy(new AMRMTokenSecretManager(conf, rmContext));

    MasterKeyData masterKeyData = appTokenMgr.createNewMasterKey();
    when(appTokenMgr.getMasterKey()).thenReturn(masterKeyData);

    ClientToAMTokenSecretManagerInRM clientToAMTokenMgr =
        new ClientToAMTokenSecretManagerInRM();

    ApplicationAttemptId attemptId1 = ConverterUtils
        .toApplicationAttemptId("appattempt_1352994193343_0001_000001");
    ApplicationId appId1 = attemptId1.getApplicationId();
    storeApp(store, appId1, submitTime, startTime);
    verifier.afterStoreApp(store, appId1);

    // create application token and client token key for attempt1
    Token<AMRMTokenIdentifier> appAttemptToken1 =
        generateAMRMToken(attemptId1, appTokenMgr);
    SecretKey clientTokenKey1 =
        clientToAMTokenMgr.createMasterKey(attemptId1);

    ContainerId containerId1 = storeAttempt(store, attemptId1,
          "container_1352994193343_0001_01_000001",
          appAttemptToken1, clientTokenKey1, dispatcher);

    String appAttemptIdStr2 = "appattempt_1352994193343_0001_000002";
    ApplicationAttemptId attemptId2 =
        ConverterUtils.toApplicationAttemptId(appAttemptIdStr2);

    // create application token and client token key for attempt2
    Token<AMRMTokenIdentifier> appAttemptToken2 =
        generateAMRMToken(attemptId2, appTokenMgr);
    SecretKey clientTokenKey2 =
        clientToAMTokenMgr.createMasterKey(attemptId2);

    ContainerId containerId2 = storeAttempt(store, attemptId2,
          "container_1352994193343_0001_02_000001",
          appAttemptToken2, clientTokenKey2, dispatcher);

    ApplicationAttemptId attemptIdRemoved = ConverterUtils
        .toApplicationAttemptId("appattempt_1352994193343_0002_000001");
    ApplicationId appIdRemoved = attemptIdRemoved.getApplicationId();
    storeApp(store, appIdRemoved, submitTime, startTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", null, null, dispatcher);
    verifier.afterStoreAppAttempt(store, attemptIdRemoved);

    RMApp mockRemovedApp = mock(RMApp.class);
    RMAppAttemptMetrics mockRmAppAttemptMetrics = 
        mock(RMAppAttemptMetrics.class);
    HashMap<ApplicationAttemptId, RMAppAttempt> attempts =
                              new HashMap<ApplicationAttemptId, RMAppAttempt>();
    ApplicationSubmissionContext context =
        new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appIdRemoved);
    when(mockRemovedApp.getSubmitTime()).thenReturn(submitTime);
    when(mockRemovedApp.getApplicationSubmissionContext()).thenReturn(context);
    when(mockRemovedApp.getAppAttempts()).thenReturn(attempts);
    when(mockRemovedApp.getUser()).thenReturn("user1");
    RMAppAttempt mockRemovedAttempt = mock(RMAppAttempt.class);
    when(mockRemovedAttempt.getAppAttemptId()).thenReturn(attemptIdRemoved);
    when(mockRemovedAttempt.getRMAppAttemptMetrics())
        .thenReturn(mockRmAppAttemptMetrics);
    when(mockRmAppAttemptMetrics.getAggregateAppResourceUsage())
        .thenReturn(new AggregateAppResourceUsage(0,0));
    attempts.put(attemptIdRemoved, mockRemovedAttempt);
    store.removeApplication(mockRemovedApp);

    // remove application directory recursively.
    storeApp(store, appIdRemoved, submitTime, startTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", null, null, dispatcher);
    store.removeApplication(mockRemovedApp);

    // let things settle down
    Thread.sleep(1000);
    store.close();

    // give tester a chance to modify app state in the store
    modifyAppState();

    // load state
    store = stateStoreHelper.getRMStateStore();
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    Map<ApplicationId, ApplicationStateData> rmAppState =
        state.getApplicationState();

    ApplicationStateData appState = rmAppState.get(appId1);
    // app is loaded
    assertNotNull(appState);
    // app is loaded correctly
    assertEquals(submitTime, appState.getSubmitTime());
    assertEquals(startTime, appState.getStartTime());
    // submission context is loaded correctly
    assertEquals(appId1,
                 appState.getApplicationSubmissionContext().getApplicationId());
    ApplicationAttemptStateData attemptState = appState.getAttempt(attemptId1);
    // attempt1 is loaded correctly
    assertNotNull(attemptState);
    assertEquals(attemptId1, attemptState.getAttemptId());
    assertEquals(-1000, attemptState.getAMContainerExitStatus());
    // attempt1 container is loaded correctly
    assertEquals(containerId1, attemptState.getMasterContainer().getId());
    // attempt1 client token master key is loaded correctly
    assertArrayEquals(
        clientTokenKey1.getEncoded(),
        attemptState.getAppAttemptTokens()
            .getSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));

    attemptState = appState.getAttempt(attemptId2);
    // attempt2 is loaded correctly
    assertNotNull(attemptState);
    assertEquals(attemptId2, attemptState.getAttemptId());
    // attempt2 container is loaded correctly
    assertEquals(containerId2, attemptState.getMasterContainer().getId());
    // attempt2 client token master key is loaded correctly
    assertArrayEquals(
        clientTokenKey2.getEncoded(),
        attemptState.getAppAttemptTokens()
            .getSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));

    //******* update application/attempt state *******//
    ApplicationStateData appState2 =
        ApplicationStateData.newInstance(appState.getSubmitTime(),
            appState.getStartTime(), appState.getUser(),
            appState.getApplicationSubmissionContext(), RMAppState.FINISHED,
            "appDiagnostics", 1234);
    appState2.attempts.putAll(appState.attempts);
    store.updateApplicationState(appState2);

    ApplicationAttemptStateData oldAttemptState = attemptState;
    ApplicationAttemptStateData newAttemptState =
        ApplicationAttemptStateData.newInstance(
            oldAttemptState.getAttemptId(),
            oldAttemptState.getMasterContainer(),
            oldAttemptState.getAppAttemptTokens(),
            oldAttemptState.getStartTime(), RMAppAttemptState.FINISHED,
            "myTrackingUrl", "attemptDiagnostics",
            FinalApplicationStatus.SUCCEEDED, 100,
            oldAttemptState.getFinishTime(), 0, 0);
    store.updateApplicationAttemptState(newAttemptState);

    // test updating the state of an app/attempt whose initial state was not
    // saved.
    ApplicationId dummyAppId = ApplicationId.newInstance(1234, 10);
    ApplicationSubmissionContext dummyContext =
        new ApplicationSubmissionContextPBImpl();
    dummyContext.setApplicationId(dummyAppId);
    ApplicationStateData dummyApp =
        ApplicationStateData.newInstance(appState.getSubmitTime(),
            appState.getStartTime(), appState.getUser(), dummyContext,
            RMAppState.FINISHED, "appDiagnostics", 1234);
    store.updateApplicationState(dummyApp);

    ApplicationAttemptId dummyAttemptId =
        ApplicationAttemptId.newInstance(dummyAppId, 6);
    ApplicationAttemptStateData dummyAttempt =
        ApplicationAttemptStateData.newInstance(dummyAttemptId,
            oldAttemptState.getMasterContainer(),
            oldAttemptState.getAppAttemptTokens(),
            oldAttemptState.getStartTime(), RMAppAttemptState.FINISHED,
            "myTrackingUrl", "attemptDiagnostics",
            FinalApplicationStatus.SUCCEEDED, 111,
            oldAttemptState.getFinishTime(), 0, 0);
    store.updateApplicationAttemptState(dummyAttempt);

    // let things settle down
    Thread.sleep(1000);
    store.close();

    // check updated application state.
    store = stateStoreHelper.getRMStateStore();
    store.setRMDispatcher(dispatcher);
    RMState newRMState = store.loadState();
    Map<ApplicationId, ApplicationStateData> newRMAppState =
        newRMState.getApplicationState();
    assertNotNull(newRMAppState.get(
        dummyApp.getApplicationSubmissionContext().getApplicationId()));
    ApplicationStateData updatedAppState = newRMAppState.get(appId1);
    assertEquals(appState.getApplicationSubmissionContext().getApplicationId(),
        updatedAppState.getApplicationSubmissionContext().getApplicationId());
    assertEquals(appState.getSubmitTime(), updatedAppState.getSubmitTime());
    assertEquals(appState.getStartTime(), updatedAppState.getStartTime());
    assertEquals(appState.getUser(), updatedAppState.getUser());
    // new app state fields
    assertEquals( RMAppState.FINISHED, updatedAppState.getState());
    assertEquals("appDiagnostics", updatedAppState.getDiagnostics());
    assertEquals(1234, updatedAppState.getFinishTime());

    // check updated attempt state
    assertNotNull(newRMAppState.get(dummyApp.getApplicationSubmissionContext
        ().getApplicationId()).getAttempt(dummyAttemptId));
    ApplicationAttemptStateData updatedAttemptState =
        updatedAppState.getAttempt(newAttemptState.getAttemptId());
    assertEquals(oldAttemptState.getAttemptId(),
      updatedAttemptState.getAttemptId());
    assertEquals(containerId2, updatedAttemptState.getMasterContainer().getId());
    assertArrayEquals(
        clientTokenKey2.getEncoded(),
        attemptState.getAppAttemptTokens()
            .getSecretKey(RMStateStore.AM_CLIENT_TOKEN_MASTER_KEY_NAME));
    // new attempt state fields
    assertEquals(RMAppAttemptState.FINISHED, updatedAttemptState.getState());
    assertEquals("myTrackingUrl", updatedAttemptState.getFinalTrackingUrl());
    assertEquals("attemptDiagnostics", updatedAttemptState.getDiagnostics());
    assertEquals(100, updatedAttemptState.getAMContainerExitStatus());
    assertEquals(FinalApplicationStatus.SUCCEEDED,
      updatedAttemptState.getFinalApplicationStatus());

    // assert store is in expected state after everything is cleaned
    assertTrue(stateStoreHelper.isFinalStateValid());

    store.close();
  }

  public void testRMDTSecretManagerStateStore(
      RMStateStoreHelper stateStoreHelper) throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    // store RM delegation token;
    RMDelegationTokenIdentifier dtId1 =
        new RMDelegationTokenIdentifier(new Text("owner1"),
          new Text("renewer1"), new Text("realuser1"));
    int sequenceNumber = 1111;
    dtId1.setSequenceNumber(sequenceNumber);
    byte[] tokenBeforeStore = dtId1.getBytes();
    Long renewDate1 = new Long(System.currentTimeMillis());
    store.storeRMDelegationToken(dtId1, renewDate1);
    modifyRMDelegationTokenState();
    Map<RMDelegationTokenIdentifier, Long> token1 =
        new HashMap<RMDelegationTokenIdentifier, Long>();
    token1.put(dtId1, renewDate1);
    // store delegation key;
    DelegationKey key = new DelegationKey(1234, 4321 , "keyBytes".getBytes());
    HashSet<DelegationKey> keySet = new HashSet<DelegationKey>();
    keySet.add(key);
    store.storeRMDTMasterKey(key);

    RMDTSecretManagerState secretManagerState =
        store.loadState().getRMDTSecretManagerState();
    Assert.assertEquals(token1, secretManagerState.getTokenState());
    Assert.assertEquals(keySet, secretManagerState.getMasterKeyState());
    Assert.assertEquals(sequenceNumber,
        secretManagerState.getDTSequenceNumber());
    RMDelegationTokenIdentifier tokenAfterStore =
        secretManagerState.getTokenState().keySet().iterator().next();
    Assert.assertTrue(Arrays.equals(tokenBeforeStore,
      tokenAfterStore.getBytes()));

    // update RM delegation token;
    renewDate1 = new Long(System.currentTimeMillis());
    store.updateRMDelegationToken(dtId1, renewDate1);
    token1.put(dtId1, renewDate1);

    RMDTSecretManagerState updateSecretManagerState =
        store.loadState().getRMDTSecretManagerState();
    Assert.assertEquals(token1, updateSecretManagerState.getTokenState());
    Assert.assertEquals(keySet, updateSecretManagerState.getMasterKeyState());
    Assert.assertEquals(sequenceNumber,
        updateSecretManagerState.getDTSequenceNumber());

    // check to delete delegationKey
    store.removeRMDTMasterKey(key);
    keySet.clear();
    RMDTSecretManagerState noKeySecretManagerState =
        store.loadState().getRMDTSecretManagerState();
    Assert.assertEquals(token1, noKeySecretManagerState.getTokenState());
    Assert.assertEquals(keySet, noKeySecretManagerState.getMasterKeyState());
    Assert.assertEquals(sequenceNumber,
        noKeySecretManagerState.getDTSequenceNumber());

    // check to delete delegationToken
    store.removeRMDelegationToken(dtId1);
    RMDTSecretManagerState noKeyAndTokenSecretManagerState =
        store.loadState().getRMDTSecretManagerState();
    token1.clear();
    Assert.assertEquals(token1,
        noKeyAndTokenSecretManagerState.getTokenState());
    Assert.assertEquals(keySet,
        noKeyAndTokenSecretManagerState.getMasterKeyState());
    Assert.assertEquals(sequenceNumber,
        noKeySecretManagerState.getDTSequenceNumber());
    store.close();

  }

  protected Token<AMRMTokenIdentifier> generateAMRMToken(
      ApplicationAttemptId attemptId,
      AMRMTokenSecretManager appTokenMgr) {
    Token<AMRMTokenIdentifier> appToken =
        appTokenMgr.createAndGetAMRMToken(attemptId);
    appToken.setService(new Text("appToken service"));
    return appToken;
  }

  public void testCheckVersion(RMStateStoreHelper stateStoreHelper)
      throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    store.setRMDispatcher(new TestDispatcher());

    // default version
    Version defaultVersion = stateStoreHelper.getCurrentVersion();
    store.checkVersion();
    Assert.assertEquals(defaultVersion, store.loadVersion());

    // compatible version
    Version compatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion(),
          defaultVersion.getMinorVersion() + 2);
    stateStoreHelper.writeVersion(compatibleVersion);
    Assert.assertEquals(compatibleVersion, store.loadVersion());
    store.checkVersion();
    // overwrite the compatible version
    Assert.assertEquals(defaultVersion, store.loadVersion());

    // incompatible version
    Version incompatibleVersion =
        Version.newInstance(defaultVersion.getMajorVersion() + 2,
          defaultVersion.getMinorVersion());
    stateStoreHelper.writeVersion(incompatibleVersion);
    try {
      store.checkVersion();
      Assert.fail("Invalid version, should fail.");
    } catch (Throwable t) {
      Assert.assertTrue(t instanceof RMStateVersionIncompatibleException);
    }
  }
  
  public void testEpoch(RMStateStoreHelper stateStoreHelper)
      throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    store.setRMDispatcher(new TestDispatcher());
    
    long firstTimeEpoch = store.getAndIncrementEpoch();
    Assert.assertEquals(0, firstTimeEpoch);
    
    long secondTimeEpoch = store.getAndIncrementEpoch();
    Assert.assertEquals(1, secondTimeEpoch);
    
    long thirdTimeEpoch = store.getAndIncrementEpoch();
    Assert.assertEquals(2, thirdTimeEpoch);
  }

  public void testAppDeletion(RMStateStoreHelper stateStoreHelper)
      throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    store.setRMDispatcher(new TestDispatcher());
    ArrayList<RMApp> appList = createAndStoreApps(stateStoreHelper, store, 5);

    for (RMApp app : appList) {
      // remove the app
      store.removeApplication(app);
      // wait for app to be removed.
      while (true) {
        if (!stateStoreHelper.appExists(app)) {
          break;
        } else {
          Thread.sleep(100);
        }
      }
    }
  }

  private ArrayList<RMApp> createAndStoreApps(
      RMStateStoreHelper stateStoreHelper, RMStateStore store, int numApps)
      throws Exception {
    ArrayList<RMApp> appList = new ArrayList<RMApp>();
    for (int i = 0; i < numApps; i++) {
      ApplicationId appId = ApplicationId.newInstance(1383183338, i);
      RMApp app = storeApp(store, appId, 123456789, 987654321);
      appList.add(app);
    }

    Assert.assertEquals(numApps, appList.size());
    for (RMApp app : appList) {
      // wait for app to be stored.
      while (true) {
        if (stateStoreHelper.appExists(app)) {
          break;
        } else {
          Thread.sleep(100);
        }
      }
    }
    return appList;
  }

  public void testDeleteStore(RMStateStoreHelper stateStoreHelper)
      throws Exception {
    RMStateStore store = stateStoreHelper.getRMStateStore();
    ArrayList<RMApp> appList = createAndStoreApps(stateStoreHelper, store, 5);
    store.deleteStore();
    // verify apps deleted
    for (RMApp app : appList) {
      Assert.assertFalse(stateStoreHelper.appExists(app));
    }
  }

  protected void modifyAppState() throws Exception {

  }

  protected void modifyRMDelegationTokenState() throws Exception {

  }

  public void testAMRMTokenSecretManagerStateStore(
      RMStateStoreHelper stateStoreHelper) throws Exception {
    System.out.println("Start testing");
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    RMContext rmContext = mock(RMContext.class);
    when(rmContext.getStateStore()).thenReturn(store);
    Configuration conf = new YarnConfiguration();
    AMRMTokenSecretManager appTokenMgr =
        new AMRMTokenSecretManager(conf, rmContext);

    //create and save the first masterkey
    MasterKeyData firstMasterKeyData = appTokenMgr.createNewMasterKey();

    AMRMTokenSecretManagerState state1 =
        AMRMTokenSecretManagerState.newInstance(
          firstMasterKeyData.getMasterKey(), null);
    rmContext.getStateStore()
        .storeOrUpdateAMRMTokenSecretManager(state1,
      false);

    // load state
    store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    RMState state = store.loadState();
    Assert.assertNotNull(state.getAMRMTokenSecretManagerState());
    Assert.assertEquals(firstMasterKeyData.getMasterKey(), state
      .getAMRMTokenSecretManagerState().getCurrentMasterKey());
    Assert.assertNull(state
      .getAMRMTokenSecretManagerState().getNextMasterKey());

    //create and save the second masterkey
    MasterKeyData secondMasterKeyData = appTokenMgr.createNewMasterKey();
    AMRMTokenSecretManagerState state2 =
        AMRMTokenSecretManagerState
          .newInstance(firstMasterKeyData.getMasterKey(),
            secondMasterKeyData.getMasterKey());
    rmContext.getStateStore().storeOrUpdateAMRMTokenSecretManager(state2,
      true);

    // load state
    store = stateStoreHelper.getRMStateStore();
    when(rmContext.getStateStore()).thenReturn(store);
    store.setRMDispatcher(dispatcher);
    RMState state_2 = store.loadState();
    Assert.assertNotNull(state_2.getAMRMTokenSecretManagerState());
    Assert.assertEquals(firstMasterKeyData.getMasterKey(), state_2
      .getAMRMTokenSecretManagerState().getCurrentMasterKey());
    Assert.assertEquals(secondMasterKeyData.getMasterKey(), state_2
      .getAMRMTokenSecretManagerState().getNextMasterKey());

    // re-create the masterKeyData based on the recovered masterkey
    // should have the same secretKey
    appTokenMgr.recover(state_2);
    Assert.assertEquals(appTokenMgr.getCurrnetMasterKeyData().getSecretKey(),
      firstMasterKeyData.getSecretKey());
    Assert.assertEquals(appTokenMgr.getNextMasterKeyData().getSecretKey(),
      secondMasterKeyData.getSecretKey());

    store.close();
  }
}
