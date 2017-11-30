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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HealthMonitor.Callback;
import org.apache.hadoop.ha.HealthMonitor.State;
import org.apache.hadoop.util.Time;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHealthMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestHealthMonitor.class);
  
  /** How many times has createProxy been called */
  private AtomicInteger createProxyCount = new AtomicInteger(0);
  private volatile boolean throwOOMEOnCreate = false;

  private HealthMonitor hm;

  private DummyHAService svc;
  
  @Before
  public void setupHM() throws InterruptedException, IOException {
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.setInt(CommonConfigurationKeys.HA_HM_CHECK_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_CONNECT_RETRY_INTERVAL_KEY, 50);
    conf.setInt(CommonConfigurationKeys.HA_HM_SLEEP_AFTER_DISCONNECT_KEY, 50);
    
    svc = createDummyHAService();
    hm = new HealthMonitor(conf, svc) {
      @Override
      protected HAServiceProtocol createProxy() throws IOException {
        createProxyCount.incrementAndGet();
        if (throwOOMEOnCreate) {
          throw new OutOfMemoryError("oome");
        }
        return super.createProxy();
      }
    };
    LOG.info("Starting health monitor");
    hm.start();
    
    LOG.info("Waiting for HEALTHY signal");    
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY);
  }

  protected DummyHAService createDummyHAService() {
    return new DummyHAService(HAServiceState.ACTIVE,
        new InetSocketAddress("0.0.0.0", 0), true);
  }

  @Test(timeout=15000)
  public void testMonitor() throws Exception {
    LOG.info("Mocking bad health check, waiting for UNHEALTHY");
    svc.isHealthy = false;
    waitForState(hm, HealthMonitor.State.SERVICE_UNHEALTHY);
    
    LOG.info("Returning to healthy state, waiting for HEALTHY");
    svc.isHealthy = true;
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY);

    LOG.info("Returning an IOException, as if node went down");
    // should expect many rapid retries
    int countBefore = createProxyCount.get();
    svc.actUnreachable = true;
    waitForState(hm, HealthMonitor.State.SERVICE_NOT_RESPONDING);

    // Should retry several times
    while (createProxyCount.get() < countBefore + 3) {
      Thread.sleep(10);
    }
    
    LOG.info("Returning to healthy state, waiting for HEALTHY");
    svc.actUnreachable = false;
    waitForState(hm, HealthMonitor.State.SERVICE_HEALTHY);
    
    hm.shutdown();
    hm.join();
    assertFalse(hm.isAlive());
  }

  /**
   * Test that the proper state is propagated when the health monitor
   * sees an uncaught exception in its thread.
   */
  @Test(timeout=15000)
  public void testHealthMonitorDies() throws Exception {
    LOG.info("Mocking RTE in health monitor, waiting for FAILED");
    throwOOMEOnCreate = true;
    svc.actUnreachable = true;
    waitForState(hm, HealthMonitor.State.HEALTH_MONITOR_FAILED);
    hm.shutdown();
    hm.join();
    assertFalse(hm.isAlive());
  }
  
  /**
   * Test that, if the callback throws an RTE, this will terminate the
   * health monitor and thus change its state to FAILED
   * @throws Exception
   */
  @Test(timeout=15000)
  public void testCallbackThrowsRTE() throws Exception {
    hm.addCallback(new Callback() {
      @Override
      public void enteredState(State newState) {
        throw new RuntimeException("Injected RTE");
      }
    });
    LOG.info("Mocking bad health check, waiting for UNHEALTHY");
    svc.isHealthy = false;
    waitForState(hm, HealthMonitor.State.HEALTH_MONITOR_FAILED);
  }

  private void waitForState(HealthMonitor hm, State state)
      throws InterruptedException {
    long st = Time.now();
    while (Time.now() - st < 2000) {
      if (hm.getHealthState() == state) {
        return;
      }
      Thread.sleep(50);
    }
    assertEquals(state, hm.getHealthState());
  }
}
