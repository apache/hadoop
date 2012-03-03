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
import java.net.InetSocketAddress;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolClientSideTranslatorPB;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysSucceedFencer;
import org.apache.hadoop.ha.TestNodeFencer.AlwaysFailFencer;
import static org.apache.hadoop.ha.TestNodeFencer.setupFencer;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestFailoverController {

  private InetSocketAddress svc1Addr = new InetSocketAddress("svc1", 1234); 
  private InetSocketAddress svc2Addr = new InetSocketAddress("svc2", 5678); 

  private class DummyService implements HAServiceProtocol {
    HAServiceState state;

    DummyService(HAServiceState state) {
      this.state = state;
    }

    @Override
    public void monitorHealth() throws HealthCheckFailedException, IOException {
      // Do nothing
    }

    @Override
    public void transitionToActive() throws ServiceFailedException, IOException {
      state = HAServiceState.ACTIVE;
    }

    @Override
    public void transitionToStandby() throws ServiceFailedException, IOException {
      state = HAServiceState.STANDBY;
    }

    @Override
    public HAServiceState getServiceState() throws IOException {
      return state;
    }

    @Override
    public boolean readyToBecomeActive() throws ServiceFailedException, IOException {
      return true;
    }
  }
  
  @Test
  public void testFailoverAndFailback() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    AlwaysSucceedFencer.fenceCalled = 0;
    FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());

    AlwaysSucceedFencer.fenceCalled = 0;
    FailoverController.failover(svc2, svc2Addr, svc1, svc1Addr, fencer, false, false);
    assertEquals(0, TestNodeFencer.AlwaysSucceedFencer.fenceCalled);
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromStandbyToStandby() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.STANDBY);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromActiveToActive() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.ACTIVE);
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Can't failover to an already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverWithoutPermission() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE) {
      @Override
      public HAServiceState getServiceState() throws IOException {
        throw new AccessControlException("Access denied");
      }
    };
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public HAServiceState getServiceState() throws IOException {
        throw new AccessControlException("Access denied");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Can't failover when access is denied");
    } catch (FailoverFailedException ffe) {
      assertTrue(ffe.getCause().getMessage().contains("Access denied"));
    }
  }


  @Test
  public void testFailoverToUnreadyService() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public boolean readyToBecomeActive() throws ServiceFailedException, IOException {
        return false;
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Can't failover to a service that's not ready");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());

    // Forcing it means we ignore readyToBecomeActive
    FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, true);
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverToUnhealthyServiceFailsAndFailsback() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void monitorHealth() throws HealthCheckFailedException {
        throw new HealthCheckFailedException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failover to unhealthy service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromFaultyServiceSucceeds() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE) {
      @Override
      public void transitionToStandby() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    AlwaysSucceedFencer.fenceCalled = 0;
    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
    } catch (FailoverFailedException ffe) {
      fail("Faulty active prevented failover");
    }

    // svc1 still thinks it's active, that's OK, it was fenced
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("svc1:1234", AlwaysSucceedFencer.fencedSvc);
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromFaultyServiceFencingFailure() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE) {
      @Override
      public void transitionToStandby() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysFailFencer.class.getName());

    AlwaysFailFencer.fenceCalled = 0;
    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failed over even though fencing failed");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertEquals("svc1:1234", AlwaysFailFencer.fencedSvc);
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testFencingFailureDuringFailover() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysFailFencer.class.getName());

    AlwaysFailFencer.fenceCalled = 0;
    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, true, false);
      fail("Failed over even though fencing requested and failed");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // If fencing was requested and it failed we don't try to make
    // svc2 active anyway, and we don't failback to svc1.
    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertEquals("svc1:1234", AlwaysFailFencer.fencedSvc);
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }
  
  private HAServiceProtocol getProtocol(String target)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(target);
    Configuration conf = new Configuration();
    // Lower the timeout so we quickly fail to connect
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    return new HAServiceProtocolClientSideTranslatorPB(addr, conf);
  }

  @Test
  public void testFailoverFromNonExistantServiceWithFencer() throws Exception {
    HAServiceProtocol svc1 = getProtocol("localhost:1234");
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
    } catch (FailoverFailedException ffe) {
      fail("Non-existant active prevented failover");
    }

    // Don't check svc1 because we can't reach it, but that's OK, it's been fenced.
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverToNonExistantServiceFails() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    HAServiceProtocol svc2 = getProtocol("localhost:1234");
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failed over to a non-existant standby");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
  }

  @Test
  public void testFailoverToFaultyServiceFailsbackOK() throws Exception {
    DummyService svc1 = spy(new DummyService(HAServiceState.ACTIVE));
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void transitionToActive() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failover to already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // svc1 went standby then back to active
    verify(svc1).transitionToStandby();
    verify(svc1).transitionToActive();
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testWeDontFailbackIfActiveWasFenced() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void transitionToActive() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, true, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We failed to failover and did not failback because we fenced
    // svc1 (we forced it), therefore svc1 and svc2 should be standby.
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testWeFenceOnFailbackIfTransitionToActiveFails() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void transitionToActive() throws ServiceFailedException, IOException {
        throw new IOException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());
    AlwaysSucceedFencer.fenceCalled = 0;

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We failed to failover. We did not fence svc1 because it cooperated
    // and we didn't force it, so we failed back to svc1 and fenced svc2.
    // Note svc2 still thinks it's active, that's OK, we fenced it.
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(1, AlwaysSucceedFencer.fenceCalled);
    assertEquals("svc2:5678", AlwaysSucceedFencer.fencedSvc);
  }

  @Test
  public void testFailureToFenceOnFailbackFailsTheFailback() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void transitionToActive() throws ServiceFailedException, IOException {
        throw new IOException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysFailFencer.class.getName());
    AlwaysFailFencer.fenceCalled = 0;

    try {
      FailoverController.failover(svc1,  svc1Addr,  svc2,  svc2Addr, fencer, false, false);
      fail("Failed over to service that won't transition to active");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    // We did not fence svc1 because it cooperated and we didn't force it, 
    // we failed to failover so we fenced svc2, we failed to fence svc2
    // so we did not failback to svc1, ie it's still standby.
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(1, AlwaysFailFencer.fenceCalled);
    assertEquals("svc2:5678", AlwaysFailFencer.fencedSvc);
  }

  @Test
  public void testFailbackToFaultyServiceFails() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE) {
      @Override
      public void transitionToActive() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void transitionToActive() throws ServiceFailedException {
        throw new ServiceFailedException("Failed!");
      }
    };
    NodeFencer fencer = setupFencer(AlwaysSucceedFencer.class.getName());

    try {
      FailoverController.failover(svc1, svc1Addr, svc2, svc2Addr, fencer, false, false);
      fail("Failover to already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }
}
