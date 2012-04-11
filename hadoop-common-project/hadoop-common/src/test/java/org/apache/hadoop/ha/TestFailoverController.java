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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceProtocol.StateChangeRequestInfo;
import org.apache.hadoop.ha.HAServiceProtocol.RequestSource;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer;
import static org.apache.hadoop.ha.TestNodeFencer.setupFencer;
import org.apache.hadoop.security.AccessControlException;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.ThrowsException;

import static org.junit.Assert.*;

public class TestFailoverController {
  private InetSocketAddress svc1Addr = new InetSocketAddress("svc1", 1234); 
  private InetSocketAddress svc2Addr = new InetSocketAddress("svc2", 5678);
  
  private Configuration conf = new Configuration();

  HAServiceStatus STATE_NOT_READY = new HAServiceStatus(HAServiceState.STANDBY)
      .setNotReadyToBecomeActive("injected not ready");

  @Test
  public void testFailoverAndFailback() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    AlwaysSucceedFencer.fenceCalled = 0;
    doFailover(svc1, svc2, false, false);
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.ACTIVE, svc2.state);

    AlwaysSucceedFencer.fenceCalled = 0;
    doFailover(svc2, svc1, false, false);
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testFailoverFromStandbyToStandby() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.STANDBY, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    doFailover(svc1, svc2, false, false);
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.ACTIVE, svc2.state);
  }

  @Test
  public void testFailoverFromActiveToActive() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.ACTIVE, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Can't failover to an already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.ACTIVE, svc2.state);
  }

  @Test
  public void testFailoverWithoutPermission() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    Mockito.doThrow(new AccessControlException("Access denied"))
        .when(svc1.proxy).getServiceStatus();
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new AccessControlException("Access denied"))
        .when(svc2.proxy).getServiceStatus();
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Can't failover when access is denied");
    } catch (FailoverFailedException ffe) {
      assertTrue(ffe.getCause().getMessage().contains("Access denied"));
    }
  }


  @Test
  public void testFailoverToUnreadyService() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doReturn(STATE_NOT_READY).when(svc2.proxy)
        .getServiceStatus();
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Can't failover to a service that's not ready");
    } catch (FailoverFailedException ffe) {
      // Expected
      if (!ffe.getMessage().contains("injected not ready")) {
        throw ffe;
      }
    }

    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);

    // Forcing it means we ignore readyToBecomeActive
    doFailover(svc1, svc2, false, true);
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.ACTIVE, svc2.state);
  }

  @Test
  public void testFailoverToUnhealthyServiceFailsAndFailsback() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new HealthCheckFailedException("Failed!"))
        .when(svc2.proxy).monitorHealth();
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failover to unhealthy service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testFailoverFromFaultyServiceSucceeds() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc1.proxy).transitionToStandby(anyReqInfo());

    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    AlwaysSucceedFencer.fenceCalled = 0;
    try {
      doFailover(svc1, svc2, false, false);
    } catch (FailoverFailedException ffe) {
      fail("Faulty active prevented failover");
    }

    // svc1 still thinks it's active, that's OK, it was fenced
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertSame(svc1, AlwaysSucceedFencer.fencedSvc);
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.ACTIVE, svc2.state);
  }

  @Test
  public void testFailoverFromFaultyServiceFencingFailure() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc1.proxy).transitionToStandby(anyReqInfo());

    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysFailFencer.class.getName());

    AlwaysFailFencer.fenceCalled = 0;
    try {
      doFailover(svc1, svc2, false, false);
      fail("Failed over even though fencing failed");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertSame(svc1, AlwaysFailFencer.fencedSvc);
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testFencingFailureDuringFailover() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysFailFencer.class.getName());

    AlwaysFailFencer.fenceCalled = 0;
    try {
      doFailover(svc1, svc2, true, false);
      fail("Failed over even though fencing requested and failed");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // If fencing was requested and it failed we don't try to make
    // svc2 active anyway, and we don't failback to svc1.
    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertSame(svc1, AlwaysFailFencer.fencedSvc);
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }
  
  @Test
  public void testFailoverFromNonExistantServiceWithFencer() throws Exception {
    DummyHAService svc1 = spy(new DummyHAService(null, svc1Addr));
    // Getting a proxy to a dead server will throw IOException on call,
    // not on creation of the proxy.
    HAServiceProtocol errorThrowingProxy = Mockito.mock(HAServiceProtocol.class,
        Mockito.withSettings()
          .defaultAnswer(new ThrowsException(
              new IOException("Could not connect to host")))
          .extraInterfaces(Closeable.class));
    Mockito.doNothing().when((Closeable)errorThrowingProxy).close();

    Mockito.doReturn(errorThrowingProxy).when(svc1).getProxy(
        Mockito.<Configuration>any(),
        Mockito.anyInt());
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
    } catch (FailoverFailedException ffe) {
      fail("Non-existant active prevented failover");
    }
    // Verify that the proxy created to try to make it go to standby
    // gracefully used the right rpc timeout
    Mockito.verify(svc1).getProxy(
        Mockito.<Configuration>any(),
        Mockito.eq(
          CommonConfigurationKeys.HA_FC_GRACEFUL_FENCE_TIMEOUT_DEFAULT));
        
    // Don't check svc1 because we can't reach it, but that's OK, it's been fenced.
    assertEquals(HAServiceState.ACTIVE, svc2.state);
  }

  @Test
  public void testFailoverToNonExistantServiceFails() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = spy(new DummyHAService(null, svc2Addr));
    Mockito.doThrow(new IOException("Failed to connect"))
      .when(svc2).getProxy(Mockito.<Configuration>any(),
          Mockito.anyInt());
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failed over to a non-existant standby");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.state);
  }

  @Test
  public void testFailoverToFaultyServiceFailsbackOK() throws Exception {
    DummyHAService svc1 = spy(new DummyHAService(HAServiceState.ACTIVE, svc1Addr));
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc2.proxy).transitionToActive(anyReqInfo());
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failover to already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // svc1 went standby then back to active
    verify(svc1.proxy).transitionToStandby(anyReqInfo());
    verify(svc1.proxy).transitionToActive(anyReqInfo());
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testWeDontFailbackIfActiveWasFenced() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc2.proxy).transitionToActive(anyReqInfo());
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, true, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We failed to failover and did not failback because we fenced
    // svc1 (we forced it), therefore svc1 and svc2 should be standby.
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testWeFenceOnFailbackIfTransitionToActiveFails() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc2.proxy).transitionToActive(anyReqInfo());
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());
    AlwaysSucceedFencer.fenceCalled = 0;

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We failed to failover. We did not fence svc1 because it cooperated
    // and we didn't force it, so we failed back to svc1 and fenced svc2.
    // Note svc2 still thinks it's active, that's OK, we fenced it.
    assertEquals(HAServiceState.ACTIVE, svc1.state);
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertSame(svc2, AlwaysSucceedFencer.fencedSvc);
  }

  private StateChangeRequestInfo anyReqInfo() {
    return Mockito.<StateChangeRequestInfo>any();
  }

  @Test
  public void testFailureToFenceOnFailbackFailsTheFailback() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new IOException("Failed!"))
        .when(svc2.proxy).transitionToActive(anyReqInfo());
    svc1.fencer = svc2.fencer = setupFencer(AlwaysFailFencer.class.getName());
    AlwaysFailFencer.fenceCalled = 0;

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We did not fence svc1 because it cooperated and we didn't force it, 
    // we failed to failover so we fenced svc2, we failed to fence svc2
    // so we did not failback to svc1, ie it's still standby.
    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertSame(svc2, AlwaysFailFencer.fencedSvc);
  }

  @Test
  public void testFailbackToFaultyServiceFails() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc1.proxy).transitionToActive(anyReqInfo());
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    Mockito.doThrow(new ServiceFailedException("Failed!"))
        .when(svc2.proxy).transitionToActive(anyReqInfo());

    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      doFailover(svc1, svc2, false, false);
      fail("Failover to already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.STANDBY, svc1.state);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }

  @Test
  public void testSelfFailoverFails() throws Exception {
    DummyHAService svc1 = new DummyHAService(HAServiceState.ACTIVE, svc1Addr);
    DummyHAService svc2 = new DummyHAService(HAServiceState.STANDBY, svc2Addr);
    svc1.fencer = svc2.fencer = setupFencer(AlwaysSucceedFencer.class.getName());
    AlwaysSucceedFencer.fenceCalled = 0;

    try {
      doFailover(svc1, svc1, false, false);
      fail("Can't failover to yourself");
    } catch (FailoverFailedException ffe) {
      // Expected
    }
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.ACTIVE, svc1.state);

    try {
      doFailover(svc2, svc2, false, false);
      fail("Can't failover to yourself");
    } catch (FailoverFailedException ffe) {
      // Expected
    }
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.STANDBY, svc2.state);
  }
  
  private void doFailover(HAServiceTarget tgt1, HAServiceTarget tgt2,
      boolean forceFence, boolean forceActive) throws FailoverFailedException {
    FailoverController fc = new FailoverController(conf, 
        RequestSource.REQUEST_BY_USER);
    fc.failover(tgt1, tgt2, forceFence, forceActive);
  }

}
