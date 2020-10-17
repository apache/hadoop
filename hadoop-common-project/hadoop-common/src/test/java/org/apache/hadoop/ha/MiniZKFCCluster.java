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
import java.util.ArrayList;
import java.util.List;

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

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Harness for starting two dummy ZK FailoverControllers, associated with
 * DummyHAServices. This harness starts two such ZKFCs, designated by
 * indexes 0 and 1, and provides utilities for building tests around them.
 */
public class MiniZKFCCluster {
  private final TestContext ctx;
  private final ZooKeeperServer zks;

  private List<DummyHAService> svcs;
  private DummyZKFCThread thrs[];
  private Configuration conf;
  
  private DummySharedResource sharedResource = new DummySharedResource();
  
  private static final Logger LOG = LoggerFactory.getLogger(MiniZKFCCluster
      .class);
  
  public MiniZKFCCluster(Configuration conf, ZooKeeperServer zks) {
    this.conf = conf;
    // Fast check interval so tests run faster
    conf.setInt(CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 50);
    svcs = new ArrayList<DummyHAService>(2);
    // remove any existing instances we are keeping track of
    DummyHAService.instances.clear();

    for (int i = 0; i < 2; i++) {
      addSvcs(svcs, i);
    }

    this.ctx = new TestContext();
    this.zks = zks;
  }

  private void addSvcs(List<DummyHAService> svcs, int i) {
    svcs.add(new DummyHAService(HAServiceState.INITIALIZING, new InetSocketAddress("svc" + (i + 1),
        1234)));
    svcs.get(i).setSharedResource(sharedResource);
  }

  /**
   * Set up two services and their failover controllers. svc1 is started
   * first, so that it enters ACTIVE state, and then svc2 is started,
   * which enters STANDBY
   */
  public void start() throws Exception {
    start(2);
  }

  /**
   * Set up the specified number of services and their failover controllers. svc1 is
   * started first, so that it enters ACTIVE state, and then svc2...svcN is started, which enters
   * STANDBY.
   * <p>
   * Adds any extra svc needed beyond the first two before starting the rest of the cluster.
   * @param count number of zkfcs to start
   */
  public void start(int count) throws Exception {
    // setup the expected number of zkfcs, if we need to add more. This seemed the least invasive
    // way to add the services - otherwise its a large test rewrite or changing a lot of assumptions
    if (count > 2) {
      for (int i = 2; i < count; i++) {
        addSvcs(svcs, i);
      }
    }

    // Format the base dir, should succeed
    thrs = new DummyZKFCThread[count];
    thrs[0] = new DummyZKFCThread(ctx, svcs.get(0));
    assertEquals(0, thrs[0].zkfc.run(new String[]{"-formatZK"}));
    ctx.addThread(thrs[0]);
    thrs[0].start();
    
    LOG.info("Waiting for svc0 to enter active state");
    waitForHAState(0, HAServiceState.ACTIVE);

    // add the remaining zkfc
    for (int i = 1; i < count; i++) {
      LOG.info("Adding svc" + i);
      thrs[i] = new DummyZKFCThread(ctx, svcs.get(i));
      thrs[i].start();
      waitForHAState(i, HAServiceState.STANDBY);
    }
  }
  
  /**
   * Stop the services.
   * @throws Exception if either of the services had encountered a fatal error
   */
  public void stop() throws Exception {
    if (thrs != null) {
      for (DummyZKFCThread thr : thrs) {
        if (thr != null) {
          thr.interrupt();
        }
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
    return svcs.get(i);
  }

  public ActiveStandbyElector getElector(int i) {
    return thrs[i].zkfc.getElectorForTests();
  }

  public DummyZKFC getZkfc(int i) {
    return thrs[i].zkfc;
  }
  
  public void setHealthy(int idx, boolean healthy) {
    svcs.get(idx).isHealthy = healthy;
  }

  public void setFailToBecomeActive(int idx, boolean doFail) {
    svcs.get(idx).failToBecomeActive = doFail;
  }

  public void setFailToBecomeStandby(int idx, boolean doFail) {
    svcs.get(idx).failToBecomeStandby = doFail;
  }
  
  public void setFailToFence(int idx, boolean doFail) {
    svcs.get(idx).failToFence = doFail;
  }
  
  public void setUnreachable(int idx, boolean unreachable) {
    svcs.get(idx).actUnreachable = unreachable;
  }

  public void setFailToBecomeObserver(int idx, boolean doFail) {
    svcs.get(idx).failToBecomeObserver = doFail;
  }

  /**
   * Wait for the given HA service to enter the given HA state.
   * This is based on the state of ZKFC, not the state of HA service.
   * There could be difference between the two. For example,
   * When the service becomes unhealthy, ZKFC will quit ZK election and
   * transition to HAServiceState.INITIALIZING and remain in that state
   * until the service becomes healthy.
   */
  public void waitForHAState(int idx, HAServiceState state)
      throws Exception {
    DummyZKFC svc = getZkfc(idx);
    while (svc.getServiceState() != state) {
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
    
    assertArrayEquals(Ints.toByteArray(svcs.get(idx).index), data);
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
    DummyHAService svc = idx == null ? null : svcs.get(idx);
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

    @Override
    protected List<HAServiceTarget> getAllOtherNodes() {
      List<HAServiceTarget> services = new ArrayList<HAServiceTarget>(
          DummyHAService.instances.size());
      for (DummyHAService service : DummyHAService.instances) {
        if (service != this.localTarget) {
          services.add(service);
        }
      }
      return services;
    }
  }
}