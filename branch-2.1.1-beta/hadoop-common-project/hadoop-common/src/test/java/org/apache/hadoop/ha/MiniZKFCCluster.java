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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

/**
 * Harness for starting two dummy ZK FailoverControllers, associated with
 * DummyHAServices. This harness starts two such ZKFCs, designated by
 * indexes 0 and 1, and provides utilities for building tests around them.
 */
public class MiniZKFCCluster {
  private final TestContext ctx;
  private final ZooKeeperServer zks;

  private DummyHAService svcs[];
  private DummyZKFCThread thrs[];
  private Configuration conf;
  
  private DummySharedResource sharedResource = new DummySharedResource();
  
  private static final Log LOG = LogFactory.getLog(MiniZKFCCluster.class);
  
  public MiniZKFCCluster(Configuration conf, ZooKeeperServer zks) {
    this.conf = conf;
    // Fast check interval so tests run faster
    conf.setInt(CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 50);
    svcs = new DummyHAService[2];
    svcs[0] = new DummyHAService(HAServiceState.INITIALIZING,
        new InetSocketAddress("svc1", 1234));
    svcs[0].setSharedResource(sharedResource);
    svcs[1] = new DummyHAService(HAServiceState.INITIALIZING,
        new InetSocketAddress("svc2", 1234));
    svcs[1].setSharedResource(sharedResource);
    
    this.ctx = new TestContext();
    this.zks = zks;
  }
  
  /**
   * Set up two services and their failover controllers. svc1 is started
   * first, so that it enters ACTIVE state, and then svc2 is started,
   * which enters STANDBY
   */
  public void start() throws Exception {
    // Format the base dir, should succeed
    thrs = new DummyZKFCThread[2];
    thrs[0] = new DummyZKFCThread(ctx, svcs[0]);
    assertEquals(0, thrs[0].zkfc.run(new String[]{"-formatZK"}));
    ctx.addThread(thrs[0]);
    thrs[0].start();
    
    LOG.info("Waiting for svc0 to enter active state");
    waitForHAState(0, HAServiceState.ACTIVE);
    
    LOG.info("Adding svc1");
    thrs[1] = new DummyZKFCThread(ctx, svcs[1]);
    thrs[1].start();
    waitForHAState(1, HAServiceState.STANDBY);
  }
  
  /**
   * Stop the services.
   * @throws Exception if either of the services had encountered a fatal error
   */
  public void stop() throws Exception {
    for (DummyZKFCThread thr : thrs) {
      if (thr != null) {
        thr.interrupt();
      }
    }
    if (ctx != null) {
      ctx.stop();
    }
    sharedResource.assertNoViolations();
  }

  /**
   * @return the TestContext implementation used internally. This allows more
   * threads to be added to the context, etc.
   */
  public TestContext getTestContext() {
    return ctx;
  }
  
  public DummyHAService getService(int i) {
    return svcs[i];
  }

  public ActiveStandbyElector getElector(int i) {
    return thrs[i].zkfc.getElectorForTests();
  }

  public DummyZKFC getZkfc(int i) {
    return thrs[i].zkfc;
  }
  
  public void setHealthy(int idx, boolean healthy) {
    svcs[idx].isHealthy = healthy;
  }

  public void setFailToBecomeActive(int idx, boolean doFail) {
    svcs[idx].failToBecomeActive = doFail;
  }

  public void setFailToBecomeStandby(int idx, boolean doFail) {
    svcs[idx].failToBecomeStandby = doFail;
  }
  
  public void setFailToFence(int idx, boolean doFail) {
    svcs[idx].failToFence = doFail;
  }
  
  public void setUnreachable(int idx, boolean unreachable) {
    svcs[idx].actUnreachable = unreachable;
  }

  /**
   * Wait for the given HA service to enter the given HA state.
   */
  public void waitForHAState(int idx, HAServiceState state)
      throws Exception {
    DummyHAService svc = getService(idx);
    while (svc.state != state) {
      ctx.checkException();
      Thread.sleep(50);
    }
  }
  
  /**
   * Wait for the ZKFC to be notified of a change in health state.
   */
  public void waitForHealthState(int idx, State state)
      throws Exception {
    ZKFCTestUtil.waitForHealthState(thrs[idx].zkfc, state, ctx);
  }

  /**
   * Wait for the given elector to enter the given elector state.
   * @param idx the service index (0 or 1)
   * @param state the state to wait for
   * @throws Exception if it times out, or an exception occurs on one
   * of the ZKFC threads while waiting.
   */
  public void waitForElectorState(int idx,
      ActiveStandbyElector.State state) throws Exception {
    ActiveStandbyElectorTestUtil.waitForElectorState(ctx,
        getElector(idx), state);
  }

  

  /**
   * Expire the ZK session of the given service. This requires
   * (and asserts) that the given service be the current active.
   * @throws NoNodeException if no service holds the lock
   */
  public void expireActiveLockHolder(int idx)
      throws NoNodeException {
    Stat stat = new Stat();
    byte[] data = zks.getZKDatabase().getData(
        DummyZKFC.LOCK_ZNODE, stat, null);
    
    assertArrayEquals(Ints.toByteArray(svcs[idx].index), data);
    long session = stat.getEphemeralOwner();
    LOG.info("Expiring svc " + idx + "'s zookeeper session " + session);
    zks.closeSession(session);
  }
  

  /**
   * Wait for the given HA service to become the active lock holder.
   * If the passed svc is null, waits for there to be no active
   * lock holder.
   */
  public void waitForActiveLockHolder(Integer idx)
      throws Exception {
    DummyHAService svc = idx == null ? null : svcs[idx];
    ActiveStandbyElectorTestUtil.waitForActiveLockData(ctx, zks,
        DummyZKFC.SCOPED_PARENT_ZNODE,
        (idx == null) ? null : Ints.toByteArray(svc.index));
  }
  

  /**
   * Expires the ZK session associated with service 'fromIdx', and waits
   * until service 'toIdx' takes over.
   * @throws Exception if the target service does not become active
   */
  public void expireAndVerifyFailover(int fromIdx, int toIdx)
      throws Exception {
    Preconditions.checkArgument(fromIdx != toIdx);
    
    getElector(fromIdx).preventSessionReestablishmentForTests();
    try {
      expireActiveLockHolder(fromIdx);
      
      waitForHAState(fromIdx, HAServiceState.STANDBY);
      waitForHAState(toIdx, HAServiceState.ACTIVE);
    } finally {
      getElector(fromIdx).allowSessionReestablishmentForTests();
    }
  }

  /**
   * Test-thread which runs a ZK Failover Controller corresponding
   * to a given dummy service.
   */
  private class DummyZKFCThread extends TestingThread {
    private final DummyZKFC zkfc;

    public DummyZKFCThread(TestContext ctx, DummyHAService svc) {
      super(ctx);
      this.zkfc = new DummyZKFC(conf, svc);
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
  
  static class DummyZKFC extends ZKFailoverController {
    private static final String DUMMY_CLUSTER = "dummy-cluster";
    public static final String SCOPED_PARENT_ZNODE =
      ZKFailoverController.ZK_PARENT_ZNODE_DEFAULT + "/" +
      DUMMY_CLUSTER;
    private static final String LOCK_ZNODE = 
      SCOPED_PARENT_ZNODE + "/" + ActiveStandbyElector.LOCK_FILENAME;
    private final DummyHAService localTarget;
    
    public DummyZKFC(Configuration conf, DummyHAService localTarget) {
      super(conf, localTarget);
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
    protected void loginAsFCUser() throws IOException {
    }

    @Override
    protected String getScopeInsideParentNode() {
      return DUMMY_CLUSTER;
    }

    @Override
    protected void checkRpcAdminAccess() throws AccessControlException {
    }

    @Override
    protected InetSocketAddress getRpcAddressToBindTo() {
      return new InetSocketAddress(0);
    }

    @Override
    protected void initRPC() throws IOException {
      super.initRPC();
      localTarget.zkfcProxy = this.getRpcServerForTests();
    }

    @Override
    protected PolicyProvider getPolicyProvider() {
      return null;
    }
  }
}