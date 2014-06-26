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

package org.apache.hadoop.ha;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.UUID;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.ha.ActiveStandbyElector.State;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.log4j.Level;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;

import com.google.common.primitives.Ints;

/**
 * Test for {@link ActiveStandbyElector} using real zookeeper.
 */
public class TestActiveStandbyElectorRealZK extends ClientBaseWithFixes {
  static final int NUM_ELECTORS = 2;
  
  static {
    ((Log4JLogger)ActiveStandbyElector.LOG).getLogger().setLevel(
        Level.ALL);
  }
  
  static final String PARENT_DIR = "/" + UUID.randomUUID();

  ActiveStandbyElector[] electors = new ActiveStandbyElector[NUM_ELECTORS];
  private byte[][] appDatas = new byte[NUM_ELECTORS][];
  private ActiveStandbyElectorCallback[] cbs =
      new ActiveStandbyElectorCallback[NUM_ELECTORS];
  private ZooKeeperServer zkServer;

  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    zkServer = getServer(serverFactory);

    for (int i = 0; i < NUM_ELECTORS; i++) {
      cbs[i] =  Mockito.mock(ActiveStandbyElectorCallback.class);
      appDatas[i] = Ints.toByteArray(i);
      electors[i] = new ActiveStandbyElector(hostPort, 5000, PARENT_DIR,
          Ids.OPEN_ACL_UNSAFE, Collections.<ZKAuthInfo> emptyList(), cbs[i],
          CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT);
    }
  }
  
  private void checkFatalsAndReset() throws Exception {
    for (int i = 0; i < NUM_ELECTORS; i++) {
      Mockito.verify(cbs[i], Mockito.never()).notifyFatalError(
          Mockito.anyString());
      Mockito.reset(cbs[i]);
    }
  }

  /**
   * the test creates 2 electors which try to become active using a real
   * zookeeper server. It verifies that 1 becomes active and 1 becomes standby.
   * Upon becoming active the leader quits election and the test verifies that
   * the standby now becomes active.
   */
  @Test(timeout=20000)
  public void testActiveStandbyTransition() throws Exception {
    LOG.info("starting test with parentDir:" + PARENT_DIR);

    assertFalse(electors[0].parentZNodeExists());
    electors[0].ensureParentZNode();
    assertTrue(electors[0].parentZNodeExists());

    // First elector joins election, becomes active.
    electors[0].joinElection(appDatas[0]);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zkServer, PARENT_DIR, appDatas[0]);
    Mockito.verify(cbs[0], Mockito.timeout(1000)).becomeActive();
    checkFatalsAndReset();

    // Second elector joins election, becomes standby.
    electors[1].joinElection(appDatas[1]);
    Mockito.verify(cbs[1], Mockito.timeout(1000)).becomeStandby();
    checkFatalsAndReset();
    
    // First elector quits, second one should become active
    electors[0].quitElection(true);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zkServer, PARENT_DIR, appDatas[1]);
    Mockito.verify(cbs[1], Mockito.timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    // First one rejoins, becomes standby, second one stays active
    electors[0].joinElection(appDatas[0]);
    Mockito.verify(cbs[0], Mockito.timeout(1000)).becomeStandby();
    checkFatalsAndReset();
    
    // Second one expires, first one becomes active
    electors[1].preventSessionReestablishmentForTests();
    try {
      zkServer.closeSession(electors[1].getZKSessionIdForTests());
      
      ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
          zkServer, PARENT_DIR, appDatas[0]);
      Mockito.verify(cbs[1], Mockito.timeout(1000)).enterNeutralMode();
      Mockito.verify(cbs[0], Mockito.timeout(1000)).fenceOldActive(
          AdditionalMatchers.aryEq(appDatas[1]));
      Mockito.verify(cbs[0], Mockito.timeout(1000)).becomeActive();
    } finally {
      electors[1].allowSessionReestablishmentForTests();
    }
    
    // Second one eventually reconnects and becomes standby
    Mockito.verify(cbs[1], Mockito.timeout(5000)).becomeStandby();
    checkFatalsAndReset();
    
    // First one expires, second one should become active
    electors[0].preventSessionReestablishmentForTests();
    try {
      zkServer.closeSession(electors[0].getZKSessionIdForTests());
      
      ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
          zkServer, PARENT_DIR, appDatas[1]);
      Mockito.verify(cbs[0], Mockito.timeout(1000)).enterNeutralMode();
      Mockito.verify(cbs[1], Mockito.timeout(1000)).fenceOldActive(
          AdditionalMatchers.aryEq(appDatas[0]));
      Mockito.verify(cbs[1], Mockito.timeout(1000)).becomeActive();
    } finally {
      electors[0].allowSessionReestablishmentForTests();
    }
    
    checkFatalsAndReset();
  }
  
  @Test(timeout=15000)
  public void testHandleSessionExpiration() throws Exception {
    ActiveStandbyElectorCallback cb = cbs[0];
    byte[] appData = appDatas[0];
    ActiveStandbyElector elector = electors[0];
    
    // Let the first elector become active
    elector.ensureParentZNode();
    elector.joinElection(appData);
    ZooKeeperServer zks = getServer(serverFactory);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appData);
    Mockito.verify(cb, Mockito.timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    LOG.info("========================== Expiring session");
    zks.closeSession(elector.getZKSessionIdForTests());

    // Should enter neutral mode when disconnected
    Mockito.verify(cb, Mockito.timeout(1000)).enterNeutralMode();

    // Should re-join the election and regain active
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appData);
    Mockito.verify(cb, Mockito.timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    LOG.info("========================== Quitting election");
    elector.quitElection(false);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    // Double check that we don't accidentally re-join the election
    // due to receiving the "expired" event.
    Thread.sleep(1000);
    Mockito.verify(cb, Mockito.never()).becomeActive();
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    checkFatalsAndReset();
  }
  
  @Test(timeout=15000)
  public void testHandleSessionExpirationOfStandby() throws Exception {
    // Let elector 0 be active
    electors[0].ensureParentZNode();
    electors[0].joinElection(appDatas[0]);
    ZooKeeperServer zks = getServer(serverFactory);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, appDatas[0]);
    Mockito.verify(cbs[0], Mockito.timeout(1000)).becomeActive();
    checkFatalsAndReset();
    
    // Let elector 1 be standby
    electors[1].joinElection(appDatas[1]);
    ActiveStandbyElectorTestUtil.waitForElectorState(null, electors[1],
        State.STANDBY);
    
    LOG.info("========================== Expiring standby's session");
    zks.closeSession(electors[1].getZKSessionIdForTests());

    // Should enter neutral mode when disconnected
    Mockito.verify(cbs[1], Mockito.timeout(1000)).enterNeutralMode();

    // Should re-join the election and go back to STANDBY
    ActiveStandbyElectorTestUtil.waitForElectorState(null, electors[1],
        State.STANDBY);
    checkFatalsAndReset();
    
    LOG.info("========================== Quitting election");
    electors[1].quitElection(false);

    // Double check that we don't accidentally re-join the election
    // by quitting elector 0 and ensuring elector 1 doesn't become active
    electors[0].quitElection(false);
    
    // due to receiving the "expired" event.
    Thread.sleep(1000);
    Mockito.verify(cbs[1], Mockito.never()).becomeActive();
    ActiveStandbyElectorTestUtil.waitForActiveLockData(null,
        zks, PARENT_DIR, null);

    checkFatalsAndReset();
  }

  @Test(timeout=15000)
  public void testDontJoinElectionOnDisconnectAndReconnect() throws Exception {
    electors[0].ensureParentZNode();

    stopServer();
    ActiveStandbyElectorTestUtil.waitForElectorState(
        null, electors[0], State.NEUTRAL);
    startServer();
    waitForServerUp(hostPort, CONNECTION_TIMEOUT);
    // Have to sleep to allow time for the clients to reconnect.
    Thread.sleep(2000);
    Mockito.verify(cbs[0], Mockito.never()).becomeActive();
    Mockito.verify(cbs[1], Mockito.never()).becomeActive();
    checkFatalsAndReset();
  }
}
