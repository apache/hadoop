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
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestFailoverController {

  private class DummyService implements HAServiceProtocol {
    HAServiceState state;

    DummyService(HAServiceState state) {
      this.state = state;
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      return null;
    }

    @Override
    public void monitorHealth() throws HealthCheckFailedException {
      // Do nothing
    }

    @Override
    public void transitionToActive() throws ServiceFailedException {
      state = HAServiceState.ACTIVE;
    }

    @Override
    public void transitionToStandby() throws ServiceFailedException {
      state = HAServiceState.STANDBY;
    }

    @Override
    public HAServiceState getServiceState() {
      return state;
    }
  }

  @Test
  public void testFailoverAndFailback() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);

    FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());

    FailoverController.failover(svc2, "svc2", svc1, "svc1");
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromStandbyToStandby() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.STANDBY);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);

    FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverFromActiveToActive() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.ACTIVE);

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
      fail("Can't failover to an already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverToUnhealthyServiceFails() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    DummyService svc2 = new DummyService(HAServiceState.STANDBY) {
      @Override
      public void monitorHealth() throws HealthCheckFailedException {
        throw new HealthCheckFailedException("Failed!");
      }
    };

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
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

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
    } catch (FailoverFailedException ffe) {
      fail("Faulty active prevented failover");
    }
    // svc1 still thinks they're active, that's OK, we'll fence them
    assertEquals(HAServiceState.ACTIVE, svc1.getServiceState());
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  private HAServiceProtocol getProtocol(String target)
      throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddr(target);
    Configuration conf = new Configuration();
    // Lower the timeout so we quickly fail to connect
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    return (HAServiceProtocol)RPC.getProxy(
        HAServiceProtocol.class, HAServiceProtocol.versionID, addr, conf);
  }

  @Test
  public void testFailoverFromNonExistantServiceSucceeds() throws Exception {
    HAServiceProtocol svc1 = getProtocol("localhost:1234");
    DummyService svc2 = new DummyService(HAServiceState.STANDBY);

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
    } catch (FailoverFailedException ffe) {
      fail("Non-existant active prevented failover");
    }

    // Don't check svc1 (we can't reach it, but that's OK, we'll fence)
    assertEquals(HAServiceState.ACTIVE, svc2.getServiceState());
  }

  @Test
  public void testFailoverToNonExistantServiceFails() throws Exception {
    DummyService svc1 = new DummyService(HAServiceState.ACTIVE);
    HAServiceProtocol svc2 = getProtocol("localhost:1234");

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
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

    try {
      FailoverController.failover(svc1, "svc1",  svc2,  "svc2");
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

    try {
      FailoverController.failover(svc1, "svc1", svc2, "svc2");
      fail("Failover to already active service");
    } catch (FailoverFailedException ffe) {
      // Expected
    }

    assertEquals(HAServiceState.STANDBY, svc1.getServiceState());
    assertEquals(HAServiceState.STANDBY, svc2.getServiceState());
  }
}
