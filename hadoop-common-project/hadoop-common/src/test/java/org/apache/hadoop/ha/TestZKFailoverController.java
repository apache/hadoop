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

import static org.junit.Assert.*;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.primitives.Ints;

public class TestZKFailoverController extends ClientBase {
  private Configuration conf;
  private DummyHAService svc1;
  private DummyHAService svc2;
  private TestContext ctx;
  private DummyZKFCThread thr1, thr2;
  
  static {
    ((Log4JLogger)ActiveStandbyElector.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @Override
  public void setUp() throws Exception {
    // build.test.dir is used by zookeeper
    new File(System.getProperty("build.test.dir", "build")).mkdirs();
    super.setUp();
  }
  
  @Before
  public void setupConfAndServices() {
    conf = new Configuration();
    conf.set(ZKFailoverController.ZK_QUORUM_KEY, hostPort);
    // Fast check interval so tests run faster
    conf.setInt(CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 50);
    svc1 = new DummyHAService(HAServiceState.INITIALIZING,
        new InetSocketAddress("svc1", 1234));
    svc2 = new DummyHAService(HAServiceState.INITIALIZING,
        new InetSocketAddress("svc2", 1234));
  }
  
  /**
   * Set up two services and their failover controllers. svc1 is started
   * first, so that it enters ACTIVE state, and then svc2 is started,
   * which enters STANDBY
   */
  private void setupFCs() throws Exception {
    // Format the base dir, should succeed
    assertEquals(0, runFC(svc1, "-formatZK"));

    ctx = new MultithreadedTestUtil.TestContext();
    thr1 = new DummyZKFCThread(ctx, svc1);
    ctx.addThread(thr1);
    thr1.start();
    
    LOG.info("Waiting for svc1 to enter active state");
    waitForHAState(svc1, HAServiceState.ACTIVE);
    
    LOG.info("Adding svc2");
    thr2 = new DummyZKFCThread(ctx, svc2);
    thr2.start();
    waitForHAState(svc2, HAServiceState.STANDBY);
  }
  
  private void stopFCs() throws Exception {
    if (thr1 != null) {
      thr1.interrupt();
    }
    if (thr2 != null) {
      thr2.interrupt();
    }
    if (ctx != null) {
      ctx.stop();
    }
  }

  /**
   * Test that the various command lines for formatting the ZK directory
   * function correctly.
   */
  @Test(timeout=15000)
  public void testFormatZK() throws Exception {
    // Run without formatting the base dir,
    // should barf
    assertEquals(ZKFailoverController.ERR_CODE_NO_PARENT_ZNODE,
        runFC(svc1));

    // Format the base dir, should succeed
    assertEquals(0, runFC(svc1, "-formatZK"));

    // Should fail to format if already formatted
    assertEquals(ZKFailoverController.ERR_CODE_FORMAT_DENIED,
        runFC(svc1, "-formatZK", "-nonInteractive"));
  
    // Unless '-force' is on
    assertEquals(0, runFC(svc1, "-formatZK", "-force"));
  }
  
  /**
   * Test that the ZKFC won't run if fencing is not configured for the
   * local service.
   */
  @Test(timeout=15000)
  public void testFencingMustBeConfigured() throws Exception {
    svc1 = Mockito.spy(svc1);
    Mockito.doThrow(new BadFencingConfigurationException("no fencing"))
        .when(svc1).checkFencingConfigured();
    // Format the base dir, should succeed
    assertEquals(0, runFC(svc1, "-formatZK"));
    // Try to run the actual FC, should fail without a fencer
    assertEquals(ZKFailoverController.ERR_CODE_NO_FENCER,
        runFC(svc1));
  }
  
  /**
   * Test that, when the health monitor indicates bad health status,
   * failover is triggered. Also ensures that graceful active->standby
   * transition is used when possible, falling back to fencing when
   * the graceful approach fails.
   */
  @Test(timeout=15000)
  public void testAutoFailoverOnBadHealth() throws Exception {
    try {
      setupFCs();
      
      LOG.info("Faking svc1 unhealthy, should failover to svc2");
      svc1.isHealthy = false;
      LOG.info("Waiting for svc1 to enter standby state");
      waitForHAState(svc1, HAServiceState.STANDBY);
      waitForHAState(svc2, HAServiceState.ACTIVE);
  
      LOG.info("Allowing svc1 to be healthy again, making svc2 unreachable " +
          "and fail to gracefully go to standby");
      svc1.isHealthy = true;
      svc2.actUnreachable = true;
      
      // Allow fencing to succeed
      Mockito.doReturn(true).when(svc2.fencer).fence(Mockito.same(svc2));
      // Should fail back to svc1 at this point
      waitForHAState(svc1, HAServiceState.ACTIVE);
      // and fence svc2
      Mockito.verify(svc2.fencer).fence(Mockito.same(svc2));
    } finally {
      stopFCs();
    }
  }
  
  @Test(timeout=15000)
  public void testAutoFailoverOnLostZKSession() throws Exception {
    try {
      setupFCs();

      // Expire svc1, it should fail over to svc2
      expireAndVerifyFailover(thr1, thr2);
      
      // Expire svc2, it should fail back to svc1
      expireAndVerifyFailover(thr2, thr1);
      
      LOG.info("======= Running test cases second time to test " +
          "re-establishment =========");
      // Expire svc1, it should fail over to svc2
      expireAndVerifyFailover(thr1, thr2);
      
      // Expire svc2, it should fail back to svc1
      expireAndVerifyFailover(thr2, thr1);
    } finally {
      stopFCs();
    }
  }
  
  private void expireAndVerifyFailover(DummyZKFCThread fromThr,
      DummyZKFCThread toThr) throws Exception {
    DummyHAService fromSvc = fromThr.zkfc.localTarget;
    DummyHAService toSvc = toThr.zkfc.localTarget;
    
    fromThr.zkfc.getElectorForTests().preventSessionReestablishmentForTests();
    try {
      expireActiveLockHolder(fromSvc);
      
      waitForHAState(fromSvc, HAServiceState.STANDBY);
      waitForHAState(toSvc, HAServiceState.ACTIVE);
    } finally {
      fromThr.zkfc.getElectorForTests().allowSessionReestablishmentForTests();
    }
  }

  /**
   * Test that, if the standby node is unhealthy, it doesn't try to become
   * active
   */
  @Test(timeout=15000)
  public void testDontFailoverToUnhealthyNode() throws Exception {
    try {
      setupFCs();

      // Make svc2 unhealthy, and wait for its FC to notice the bad health.
      svc2.isHealthy = false;
      waitForHealthState(thr2.zkfc,
          HealthMonitor.State.SERVICE_UNHEALTHY);
      
      // Expire svc1
      thr1.zkfc.getElectorForTests().preventSessionReestablishmentForTests();
      try {
        expireActiveLockHolder(svc1);

        LOG.info("Expired svc1's ZK session. Waiting a second to give svc2" +
            " a chance to take the lock, if it is ever going to.");
        Thread.sleep(1000);
        
        // Ensure that no one holds the lock.
        waitForActiveLockHolder(null);
        
      } finally {
        LOG.info("Allowing svc1's elector to re-establish its connection");
        thr1.zkfc.getElectorForTests().allowSessionReestablishmentForTests();
      }
      // svc1 should get the lock again
      waitForActiveLockHolder(svc1);
    } finally {
      stopFCs();
    }
  }

  /**
   * Test that the ZKFC successfully quits the election when it fails to
   * become active. This allows the old node to successfully fail back.
   */
  @Test(timeout=15000)
  public void testBecomingActiveFails() throws Exception {
    try {
      setupFCs();
      
      LOG.info("Making svc2 fail to become active");
      svc2.failToBecomeActive = true;
      
      LOG.info("Faking svc1 unhealthy, should NOT successfully " +
          "failover to svc2");
      svc1.isHealthy = false;
      waitForHealthState(thr1.zkfc, State.SERVICE_UNHEALTHY);
      waitForActiveLockHolder(null);

      Mockito.verify(svc2.proxy).transitionToActive();

      waitForHAState(svc1, HAServiceState.STANDBY);
      waitForHAState(svc2, HAServiceState.STANDBY);
      
      LOG.info("Faking svc1 healthy again, should go back to svc1");
      svc1.isHealthy = true;
      waitForHAState(svc1, HAServiceState.ACTIVE);
      waitForHAState(svc2, HAServiceState.STANDBY);
      waitForActiveLockHolder(svc1);
    } finally {
      stopFCs();
    }
  }
  
  /**
   * Test that, when ZooKeeper fails, the system remains in its
   * current state, without triggering any failovers, and without
   * causing the active node to enter standby state.
   */
  @Test(timeout=15000)
  public void testZooKeeperFailure() throws Exception {
    try {
      setupFCs();

      // Record initial ZK sessions
      long session1 = thr1.zkfc.getElectorForTests().getZKSessionIdForTests();
      long session2 = thr2.zkfc.getElectorForTests().getZKSessionIdForTests();

      LOG.info("====== Stopping ZK server");
      stopServer();
      waitForServerDown(hostPort, CONNECTION_TIMEOUT);
      
      LOG.info("====== Waiting for services to enter NEUTRAL mode");
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr1.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.NEUTRAL);
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr2.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.NEUTRAL);

      LOG.info("====== Checking that the services didn't change HA state");
      assertEquals(HAServiceState.ACTIVE, svc1.state);
      assertEquals(HAServiceState.STANDBY, svc2.state);
      
      LOG.info("====== Restarting server");
      startServer();
      waitForServerUp(hostPort, CONNECTION_TIMEOUT);

      // Nodes should go back to their original states, since they re-obtain
      // the same sessions.
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr1.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.ACTIVE);
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr2.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.STANDBY);
      // Check HA states didn't change.
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr1.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.ACTIVE);
      ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
          thr2.zkfc.getElectorForTests(),
          ActiveStandbyElector.State.STANDBY);
      // Check they re-used the same sessions and didn't spuriously reconnect
      assertEquals(session1,
          thr1.zkfc.getElectorForTests().getZKSessionIdForTests());
      assertEquals(session2,
          thr2.zkfc.getElectorForTests().getZKSessionIdForTests());
    } finally {
      stopFCs();
    }
  }

  /**
   * Expire the ZK session of the given service. This requires
   * (and asserts) that the given service be the current active.
   * @throws NoNodeException if no service holds the lock
   */
  private void expireActiveLockHolder(DummyHAService expectedActive)
      throws NoNodeException {
    ZooKeeperServer zks = getServer(serverFactory);
    Stat stat = new Stat();
    byte[] data = zks.getZKDatabase().getData(
        ZKFailoverController.ZK_PARENT_ZNODE_DEFAULT + "/" +
        ActiveStandbyElector.LOCK_FILENAME, stat, null);
    
    assertArrayEquals(Ints.toByteArray(expectedActive.index), data);
    long session = stat.getEphemeralOwner();
    LOG.info("Expiring svc " + expectedActive + "'s zookeeper session " + session);
    zks.closeSession(session);
  }
  
  /**
   * Wait for the given HA service to enter the given HA state.
   */
  private void waitForHAState(DummyHAService svc, HAServiceState state)
      throws Exception {
    while (svc.state != state) {
      ctx.checkException();
      Thread.sleep(50);
    }
  }
  
  /**
   * Wait for the ZKFC to be notified of a change in health state.
   */
  private void waitForHealthState(DummyZKFC zkfc, State state)
      throws Exception {
    while (zkfc.getLastHealthState() != state) {
      ctx.checkException();
      Thread.sleep(50);
    }
  }

  /**
   * Wait for the given HA service to become the active lock holder.
   * If the passed svc is null, waits for there to be no active
   * lock holder.
   */
  private void waitForActiveLockHolder(DummyHAService svc)
      throws Exception {
    ZooKeeperServer zks = getServer(serverFactory);
    ActiveStandbyElectorTestUtil.waitForActiveLockData(ctx, zks,
        ZKFailoverController.ZK_PARENT_ZNODE_DEFAULT,
        (svc == null) ? null : Ints.toByteArray(svc.index));
  }


  private int runFC(DummyHAService target, String ... args) throws Exception {
    DummyZKFC zkfc = new DummyZKFC(target);
    zkfc.setConf(conf);
    return zkfc.run(args);
  }

  /**
   * Test-thread which runs a ZK Failover Controller corresponding
   * to a given dummy service.
   */
  private class DummyZKFCThread extends TestingThread {
    private final DummyZKFC zkfc;

    public DummyZKFCThread(TestContext ctx, DummyHAService svc) {
      super(ctx);
      this.zkfc = new DummyZKFC(svc);
      zkfc.setConf(conf);
    }

    @Override
    public void doWork() throws Exception {
      try {
        assertEquals(0, zkfc.run(new String[0]));
      } catch (InterruptedException ie) {
        // Interrupted by main thread, that's OK.
      }
    }
  }
  
  private static class DummyZKFC extends ZKFailoverController {
    private final DummyHAService localTarget;
    
    public DummyZKFC(DummyHAService localTarget) {
      this.localTarget = localTarget;
    }

    @Override
    protected byte[] targetToData(HAServiceTarget target) {
      return Ints.toByteArray(((DummyHAService)target).index);
    }
    
    @Override
    protected HAServiceTarget dataToTarget(byte[] data) {
      int index = Ints.fromByteArray(data);
      return DummyHAService.getInstance(index);
    }

    @Override
    protected HAServiceTarget getLocalTarget() {
      return localTarget;
    }
  }
}
