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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRMEmbeddedElector extends ClientBaseWithFixes {
  private static final Log LOG =
      LogFactory.getLog(TestRMEmbeddedElector.class.getName());

  private static final String RM1_NODE_ID = "rm1";
  private static final int RM1_PORT_BASE = 10000;
  private static final String RM2_NODE_ID = "rm2";
  private static final int RM2_PORT_BASE = 20000;

  private Configuration conf;
  private AtomicBoolean callbackCalled;
  private AtomicInteger transitionToActiveCounter;
  private AtomicInteger transitionToStandbyCounter;

  private enum SyncTestType {
    ACTIVE,
    STANDBY,
    NEUTRAL,
    ACTIVE_TIMING,
    STANDBY_TIMING
  }

  @Before
  public void setup() throws IOException {
    conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, true);
    conf.setBoolean(YarnConfiguration.AUTO_FAILOVER_EMBEDDED, true);
    conf.set(YarnConfiguration.RM_CLUSTER_ID, "yarn-test-cluster");
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, hostPort);
    conf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 2000);

    conf.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + "," + RM2_NODE_ID);
    conf.set(YarnConfiguration.RM_HA_ID, RM1_NODE_ID);
    HATestUtil.setRpcAddressForRM(RM1_NODE_ID, RM1_PORT_BASE, conf);
    HATestUtil.setRpcAddressForRM(RM2_NODE_ID, RM2_PORT_BASE, conf);

    conf.setLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS, 100L);

    callbackCalled = new AtomicBoolean(false);
    transitionToActiveCounter = new AtomicInteger(0);
    transitionToStandbyCounter = new AtomicInteger(0);
  }

  /**
   * Test that tries to see if there is a deadlock between
   * (a) the thread stopping the RM
   * (b) thread processing the ZK event asking RM to transition to active
   *
   * The test times out if there is a deadlock.
   */
  @Test (timeout = 10000)
  public void testDeadlockShutdownBecomeActive() throws InterruptedException {
    MockRM rm = new MockRMWithElector(conf, 1000);
    rm.start();
    LOG.info("Waiting for callback");
    while (!callbackCalled.get());
    LOG.info("Stopping RM");
    rm.stop();
    LOG.info("Stopped RM");
  }

  /**
   * Test that neutral mode plays well with all other transitions.
   *
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   */
  @Test
  public void testCallbackSynchronization()
      throws IOException, InterruptedException, TimeoutException {
    testCallbackSynchronization(SyncTestType.ACTIVE);
    testCallbackSynchronization(SyncTestType.STANDBY);
    testCallbackSynchronization(SyncTestType.NEUTRAL);
    testCallbackSynchronization(SyncTestType.ACTIVE_TIMING);
    testCallbackSynchronization(SyncTestType.STANDBY_TIMING);
  }

  /**
   * Helper method to test that neutral mode plays well with other transitions.
   *
   * @param type the type of test to run
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronization(SyncTestType type)
      throws IOException, InterruptedException, TimeoutException {
    AdminService as = mock(AdminService.class);
    RMContext rc = mock(RMContext.class);
    ResourceManager rm = mock(ResourceManager.class);
    Configuration myConf = new Configuration(conf);

    myConf.setInt(YarnConfiguration.RM_ZK_TIMEOUT_MS, 50);
    when(rm.getRMContext()).thenReturn(rc);
    when(rc.getRMAdminService()).thenReturn(as);

    doAnswer(invocation -> {
      transitionToActiveCounter.incrementAndGet();
      return null;
    }).when(as).transitionToActive(any());
    transitionToActiveCounter.set(0);
    doAnswer(invocation -> {
      transitionToStandbyCounter.incrementAndGet();
      return null;
    }).when(as).transitionToStandby(any());
    transitionToStandbyCounter.set(0);

    ActiveStandbyElectorBasedElectorService ees =
        new ActiveStandbyElectorBasedElectorService(rm);
    ees.init(myConf);

    ees.enterNeutralMode();

    switch (type) {
    case ACTIVE:
      testCallbackSynchronizationActive(as, ees);
      break;
    case STANDBY:
      testCallbackSynchronizationStandby(as, ees);
      break;
    case NEUTRAL:
      testCallbackSynchronizationNeutral(as, ees);
      break;
    case ACTIVE_TIMING:
      testCallbackSynchronizationTimingActive(as, ees);
      break;
    case STANDBY_TIMING:
      testCallbackSynchronizationTimingStandby(as, ees);
      break;
    default:
      fail("Unknown test type: " + type);
      break;
    }
  }

  /**
   * Helper method to test that neutral mode plays well with an active
   * transition.
   *
   * @param as the admin service
   * @param ees the embedded elector service
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronizationActive(AdminService as,
      ActiveStandbyElectorBasedElectorService ees)
      throws IOException, InterruptedException, TimeoutException {
    ees.becomeActive();

    GenericTestUtils.waitFor(
        () -> transitionToActiveCounter.get() >= 1, 500, 10 * 1000);
    verify(as, times(1)).transitionToActive(any());
    verify(as, never()).transitionToStandby(any());
  }

  /**
   * Helper method to test that neutral mode plays well with a standby
   * transition.
   *
   * @param as the admin service
   * @param ees the embedded elector service
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronizationStandby(AdminService as,
      ActiveStandbyElectorBasedElectorService ees)
      throws IOException, InterruptedException, TimeoutException {
    ees.becomeStandby();

    GenericTestUtils.waitFor(
        () -> transitionToStandbyCounter.get() >= 1, 500, 10 * 1000);
    verify(as, times(1)).transitionToStandby(any());
  }

  /**
   * Helper method to test that neutral mode plays well with itself.
   *
   * @param as the admin service
   * @param ees the embedded elector service
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronizationNeutral(AdminService as,
      ActiveStandbyElectorBasedElectorService ees)
      throws IOException, InterruptedException, TimeoutException {
    ees.enterNeutralMode();

    GenericTestUtils.waitFor(
        () -> transitionToStandbyCounter.get() >= 1, 500, 10 * 1000);
    verify(as, times(1)).transitionToStandby(any());
  }

  /**
   * Helper method to test that neutral mode does not race with an active
   * transition.
   *
   * @param as the admin service
   * @param ees the embedded elector service
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronizationTimingActive(AdminService as,
      ActiveStandbyElectorBasedElectorService ees)
      throws IOException, InterruptedException, TimeoutException {
    synchronized (ees.zkDisconnectLock) {
      // Sleep while holding the lock so that the timer thread can't do
      // anything when it runs.  Sleep until we're pretty sure the timer thread
      // has tried to run.
      Thread.sleep(100);
      // While still holding the lock cancel the timer by transitioning. This
      // simulates a race where the callback goes to cancel the timer while the
      // timer is trying to run.
      ees.becomeActive();
    }

    // Sleep just a little more so that the timer thread can do whatever it's
    // going to do, hopefully nothing.
    Thread.sleep(50);

    GenericTestUtils.waitFor(
        () -> transitionToActiveCounter.get() >= 1, 500, 10 * 1000);
    verify(as, times(1)).transitionToActive(any());
    verify(as, never()).transitionToStandby(any());
  }

  /**
   * Helper method to test that neutral mode does not race with an active
   * transition.
   *
   * @param as the admin service
   * @param ees the embedded elector service
   * @throws IOException if there's an issue transitioning
   * @throws InterruptedException if interrupted
   * @throws TimeoutException if waitFor timeout reached
   */
  private void testCallbackSynchronizationTimingStandby(AdminService as,
      ActiveStandbyElectorBasedElectorService ees)
      throws IOException, InterruptedException, TimeoutException {
    synchronized (ees.zkDisconnectLock) {
      // Sleep while holding the lock so that the timer thread can't do
      // anything when it runs.  Sleep until we're pretty sure the timer thread
      // has tried to run.
      Thread.sleep(100);
      // While still holding the lock cancel the timer by transitioning. This
      // simulates a race where the callback goes to cancel the timer while the
      // timer is trying to run.
      ees.becomeStandby();
    }

    // Sleep just a little more so that the timer thread can do whatever it's
    // going to do, hopefully nothing.
    Thread.sleep(50);

    GenericTestUtils.waitFor(
        () -> transitionToStandbyCounter.get() >= 1, 500, 10 * 1000);
    verify(as, times(1)).transitionToStandby(any());
  }

  /**
   * Test that active elector service triggers a fatal RM Event when connection
   * to ZK fails. YARN-8409
   */
  @Test
  public void testFailureToConnectToZookeeper() throws Exception {
    stopServer();
    Configuration myConf = new Configuration(conf);
    ResourceManager rm = new MockRM(conf);

    ActiveStandbyElectorBasedElectorService ees =
        new ActiveStandbyElectorBasedElectorService(rm);
    try {
      ees.init(myConf);
      Assert.fail("expect failure to connect to Zookeeper");
    } catch (ServiceStateException sse) {
      Assert.assertTrue(sse.getMessage().contains("ConnectionLoss"));
    }
  }

  private class MockRMWithElector extends MockRM {
    private long delayMs = 0;

    MockRMWithElector(Configuration conf) {
      super(conf);
    }

    MockRMWithElector(Configuration conf, long delayMs) {
      this(conf);
      this.delayMs = delayMs;
    }

    @Override
    protected EmbeddedElector createEmbeddedElector() {
      return new ActiveStandbyElectorBasedElectorService(this) {
        @Override
        public void becomeActive() throws
            ServiceFailedException {
          try {
            callbackCalled.set(true);
            TestRMEmbeddedElector.LOG.info("Callback called. Sleeping now");
            Thread.sleep(delayMs);
            TestRMEmbeddedElector.LOG.info("Sleep done");
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          super.becomeActive();
        }
      };
    }
  }
}
