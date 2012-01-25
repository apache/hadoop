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

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveNotFoundException;

public class TestActiveStandbyElector {

  static ZooKeeper mockZK;
  static int count;
  static ActiveStandbyElectorCallback mockApp;
  static final byte[] data = new byte[8];

  ActiveStandbyElectorTester elector;

  class ActiveStandbyElectorTester extends ActiveStandbyElector {
    ActiveStandbyElectorTester(String hostPort, int timeout, String parent,
        List<ACL> acl, ActiveStandbyElectorCallback app) throws IOException {
      super(hostPort, timeout, parent, acl, app);
    }

    @Override
    public ZooKeeper getNewZooKeeper() {
      ++TestActiveStandbyElector.count;
      return TestActiveStandbyElector.mockZK;
    }

  }

  private static final String zkParentName = "/zookeeper";
  private static final String zkLockPathName = "/zookeeper/"
      + ActiveStandbyElector.LOCKFILENAME;

  @Before
  public void init() throws IOException {
    count = 0;
    mockZK = Mockito.mock(ZooKeeper.class);
    mockApp = Mockito.mock(ActiveStandbyElectorCallback.class);
    elector = new ActiveStandbyElectorTester("hostPort", 1000, zkParentName,
        Ids.OPEN_ACL_UNSAFE, mockApp);
  }

  /**
   * verify that joinElection checks for null data
   */
  @Test(expected = HadoopIllegalArgumentException.class)
  public void testJoinElectionException() {
    elector.joinElection(null);
  }

  /**
   * verify that joinElection tries to create ephemeral lock znode
   */
  @Test
  public void testJoinElection() {
    elector.joinElection(data);
    Mockito.verify(mockZK, Mockito.times(1)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
  }

  /**
   * verify that successful znode create result becomes active and monitoring is
   * started
   */
  @Test
  public void testCreateNodeResultBecomeActive() {
    elector.joinElection(data);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    // monitor callback verifies the leader is ephemeral owner of lock but does
    // not call becomeActive since its already active
    Stat stat = new Stat();
    stat.setEphemeralOwner(1L);
    Mockito.when(mockZK.getSessionId()).thenReturn(1L);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null, stat);
    // should not call neutral mode/standby/active
    Mockito.verify(mockApp, Mockito.times(0)).enterNeutralMode();
    Mockito.verify(mockApp, Mockito.times(0)).becomeStandby();
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    // another joinElection not called.
    Mockito.verify(mockZK, Mockito.times(1)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
    // no new monitor called
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);
  }

  /**
   * verify that znode create for existing node and no retry becomes standby and
   * monitoring is started
   */
  @Test
  public void testCreateNodeResultBecomeStandby() {
    elector.joinElection(data);

    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);
  }

  /**
   * verify that znode create error result in fatal error
   */
  @Test
  public void testCreateNodeResultError() {
    elector.joinElection(data);

    elector.processResult(Code.APIERROR.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).notifyFatalError(
        "Received create error from Zookeeper. code:APIERROR");
  }

  /**
   * verify that retry of network errors verifies master by session id and
   * becomes active if they match. monitoring is started.
   */
  @Test
  public void testCreateNodeResultRetryBecomeActive() {
    elector.joinElection(data);

    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    // 4 errors results in fatalError
    Mockito
        .verify(mockApp, Mockito.times(1))
        .notifyFatalError(
            "Received create error from Zookeeper. code:CONNECTIONLOSS. "+
            "Not retrying further znode create connection errors.");

    elector.joinElection(data);
    // recreate connection via getNewZooKeeper
    Assert.assertEquals(2, TestActiveStandbyElector.count);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    Stat stat = new Stat();
    stat.setEphemeralOwner(1L);
    Mockito.when(mockZK.getSessionId()).thenReturn(1L);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null, stat);
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);
    Mockito.verify(mockZK, Mockito.times(6)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
  }

  /**
   * verify that retry of network errors verifies active by session id and
   * becomes standby if they dont match. monitoring is started.
   */
  @Test
  public void testCreateNodeResultRetryBecomeStandby() {
    elector.joinElection(data);

    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    Stat stat = new Stat();
    stat.setEphemeralOwner(0);
    Mockito.when(mockZK.getSessionId()).thenReturn(1L);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null, stat);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);
  }

  /**
   * verify that if create znode results in nodeexists and that znode is deleted
   * before exists() watch is set then the return of the exists() method results
   * in attempt to re-create the znode and become active
   */
  @Test
  public void testCreateNodeResultRetryNoNode() {
    elector.joinElection(data);

    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        zkLockPathName);
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    elector.processResult(Code.NONODE.intValue(), zkLockPathName, null,
        (Stat) null);
    Mockito.verify(mockApp, Mockito.times(1)).enterNeutralMode();
    Mockito.verify(mockZK, Mockito.times(4)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
  }

  /**
   * verify that more than 3 network error retries result fatalError
   */
  @Test
  public void testStatNodeRetry() {
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        (Stat) null);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        (Stat) null);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        (Stat) null);
    elector.processResult(Code.CONNECTIONLOSS.intValue(), zkLockPathName, null,
        (Stat) null);
    Mockito
        .verify(mockApp, Mockito.times(1))
        .notifyFatalError(
            "Received stat error from Zookeeper. code:CONNECTIONLOSS. "+
            "Not retrying further znode monitoring connection errors.");
  }

  /**
   * verify error in exists() callback results in fatal error
   */
  @Test
  public void testStatNodeError() {
    elector.processResult(Code.RUNTIMEINCONSISTENCY.intValue(), zkLockPathName,
        null, (Stat) null);
    Mockito.verify(mockApp, Mockito.times(0)).enterNeutralMode();
    Mockito.verify(mockApp, Mockito.times(1)).notifyFatalError(
        "Received stat error from Zookeeper. code:RUNTIMEINCONSISTENCY");
  }

  /**
   * verify behavior of watcher.process callback with non-node event
   */
  @Test
  public void testProcessCallbackEventNone() {
    elector.joinElection(data);

    WatchedEvent mockEvent = Mockito.mock(WatchedEvent.class);
    Mockito.when(mockEvent.getType()).thenReturn(Event.EventType.None);

    // first SyncConnected should not do anything
    Mockito.when(mockEvent.getState()).thenReturn(
        Event.KeeperState.SyncConnected);
    elector.process(mockEvent);
    Mockito.verify(mockZK, Mockito.times(0)).exists(Mockito.anyString(),
        Mockito.anyBoolean(), Mockito.<AsyncCallback.StatCallback> anyObject(),
        Mockito.<Object> anyObject());

    // disconnection should enter safe mode
    Mockito.when(mockEvent.getState()).thenReturn(
        Event.KeeperState.Disconnected);
    elector.process(mockEvent);
    Mockito.verify(mockApp, Mockito.times(1)).enterNeutralMode();

    // re-connection should monitor master status
    Mockito.when(mockEvent.getState()).thenReturn(
        Event.KeeperState.SyncConnected);
    elector.process(mockEvent);
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    // session expired should enter safe mode and initiate re-election
    // re-election checked via checking re-creation of new zookeeper and
    // call to create lock znode
    Mockito.when(mockEvent.getState()).thenReturn(Event.KeeperState.Expired);
    elector.process(mockEvent);
    // already in safe mode above. should not enter safe mode again
    Mockito.verify(mockApp, Mockito.times(1)).enterNeutralMode();
    // called getNewZooKeeper to create new session. first call was in
    // constructor
    Assert.assertEquals(2, TestActiveStandbyElector.count);
    // once in initial joinElection and one now
    Mockito.verify(mockZK, Mockito.times(2)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);

    // create znode success. become master and monitor
    elector.processResult(Code.OK.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    Mockito.verify(mockZK, Mockito.times(2)).exists(zkLockPathName, true,
        elector, null);

    // error event results in fatal error
    Mockito.when(mockEvent.getState()).thenReturn(Event.KeeperState.AuthFailed);
    elector.process(mockEvent);
    Mockito.verify(mockApp, Mockito.times(1)).notifyFatalError(
        "Unexpected Zookeeper watch event state: AuthFailed");
    // only 1 state change callback is called at a time
    Mockito.verify(mockApp, Mockito.times(1)).enterNeutralMode();
  }

  /**
   * verify behavior of watcher.process with node event
   */
  @Test
  public void testProcessCallbackEventNode() {
    elector.joinElection(data);

    // make the object go into the monitoring state
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    WatchedEvent mockEvent = Mockito.mock(WatchedEvent.class);
    Mockito.when(mockEvent.getPath()).thenReturn(zkLockPathName);

    // monitoring should be setup again after event is received
    Mockito.when(mockEvent.getType()).thenReturn(
        Event.EventType.NodeDataChanged);
    elector.process(mockEvent);
    Mockito.verify(mockZK, Mockito.times(2)).exists(zkLockPathName, true,
        elector, null);

    // monitoring should be setup again after event is received
    Mockito.when(mockEvent.getType()).thenReturn(
        Event.EventType.NodeChildrenChanged);
    elector.process(mockEvent);
    Mockito.verify(mockZK, Mockito.times(3)).exists(zkLockPathName, true,
        elector, null);

    // lock node deletion when in standby mode should create znode again
    // successful znode creation enters active state and sets monitor
    Mockito.when(mockEvent.getType()).thenReturn(Event.EventType.NodeDeleted);
    elector.process(mockEvent);
    // enterNeutralMode not called when app is standby and leader is lost
    Mockito.verify(mockApp, Mockito.times(0)).enterNeutralMode();
    // once in initial joinElection() and one now
    Mockito.verify(mockZK, Mockito.times(2)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeActive();
    Mockito.verify(mockZK, Mockito.times(4)).exists(zkLockPathName, true,
        elector, null);

    // lock node deletion in active mode should enter neutral mode and create
    // znode again successful znode creation enters active state and sets
    // monitor
    Mockito.when(mockEvent.getType()).thenReturn(Event.EventType.NodeDeleted);
    elector.process(mockEvent);
    Mockito.verify(mockApp, Mockito.times(1)).enterNeutralMode();
    // another joinElection called
    Mockito.verify(mockZK, Mockito.times(3)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
    elector.processResult(Code.OK.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(2)).becomeActive();
    Mockito.verify(mockZK, Mockito.times(5)).exists(zkLockPathName, true,
        elector, null);

    // bad path name results in fatal error
    Mockito.when(mockEvent.getPath()).thenReturn(null);
    elector.process(mockEvent);
    Mockito.verify(mockApp, Mockito.times(1)).notifyFatalError(
        "Unexpected watch error from Zookeeper");
    // fatal error means no new connection other than one from constructor
    Assert.assertEquals(1, TestActiveStandbyElector.count);
    // no new watches after fatal error
    Mockito.verify(mockZK, Mockito.times(5)).exists(zkLockPathName, true,
        elector, null);

  }

  /**
   * verify becomeStandby is not called if already in standby
   */
  @Test
  public void testSuccessiveStandbyCalls() {
    elector.joinElection(data);

    // make the object go into the monitoring standby state
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

    WatchedEvent mockEvent = Mockito.mock(WatchedEvent.class);
    Mockito.when(mockEvent.getPath()).thenReturn(zkLockPathName);

    // notify node deletion
    // monitoring should be setup again after event is received
    Mockito.when(mockEvent.getType()).thenReturn(Event.EventType.NodeDeleted);
    elector.process(mockEvent);
    // is standby. no need to notify anything now
    Mockito.verify(mockApp, Mockito.times(0)).enterNeutralMode();
    // another joinElection called.
    Mockito.verify(mockZK, Mockito.times(2)).create(zkLockPathName, data,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, elector, null);
    // lost election
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    // still standby. so no need to notify again
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    // monitor is set again
    Mockito.verify(mockZK, Mockito.times(2)).exists(zkLockPathName, true,
        elector, null);
  }

  /**
   * verify quit election terminates connection and there are no new watches.
   * next call to joinElection creates new connection and performs election
   */
  @Test
  public void testQuitElection() throws InterruptedException {
    elector.quitElection();
    Mockito.verify(mockZK, Mockito.times(1)).close();
    // no watches added
    Mockito.verify(mockZK, Mockito.times(0)).exists(zkLockPathName, true,
        elector, null);

    byte[] data = new byte[8];
    elector.joinElection(data);
    // getNewZooKeeper called 2 times. once in constructor and once now
    Assert.assertEquals(2, TestActiveStandbyElector.count);
    elector.processResult(Code.NODEEXISTS.intValue(), zkLockPathName, null,
        zkLockPathName);
    Mockito.verify(mockApp, Mockito.times(1)).becomeStandby();
    Mockito.verify(mockZK, Mockito.times(1)).exists(zkLockPathName, true,
        elector, null);

  }

  /**
   * verify that receiveActiveData gives data when active exists, tells that
   * active does not exist and reports error in getting active information
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ActiveNotFoundException
   */
  @Test
  public void testGetActiveData() throws ActiveNotFoundException,
      KeeperException, InterruptedException, IOException {
    // get valid active data
    byte[] data = new byte[8];
    Mockito.when(
        mockZK.getData(Mockito.eq(zkLockPathName), Mockito.eq(false),
            Mockito.<Stat> anyObject())).thenReturn(data);
    Assert.assertEquals(data, elector.getActiveData());
    Mockito.verify(mockZK, Mockito.times(1)).getData(
        Mockito.eq(zkLockPathName), Mockito.eq(false),
        Mockito.<Stat> anyObject());

    // active does not exist
    Mockito.when(
        mockZK.getData(Mockito.eq(zkLockPathName), Mockito.eq(false),
            Mockito.<Stat> anyObject())).thenThrow(
        new KeeperException.NoNodeException());
    try {
      elector.getActiveData();
      Assert.fail("ActiveNotFoundException expected");
    } catch(ActiveNotFoundException e) {
      Mockito.verify(mockZK, Mockito.times(2)).getData(
          Mockito.eq(zkLockPathName), Mockito.eq(false),
          Mockito.<Stat> anyObject());
    }

    // error getting active data rethrows keeperexception
    try {
      Mockito.when(
          mockZK.getData(Mockito.eq(zkLockPathName), Mockito.eq(false),
              Mockito.<Stat> anyObject())).thenThrow(
          new KeeperException.AuthFailedException());
      elector.getActiveData();
      Assert.fail("KeeperException.AuthFailedException expected");
    } catch(KeeperException.AuthFailedException ke) {
      Mockito.verify(mockZK, Mockito.times(3)).getData(
          Mockito.eq(zkLockPathName), Mockito.eq(false),
          Mockito.<Stat> anyObject());
    }
  }

}
