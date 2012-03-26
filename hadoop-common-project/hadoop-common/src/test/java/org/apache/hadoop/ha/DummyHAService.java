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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.security.AccessControlException;
import org.mockito.Mockito;

/**
 * Test-only implementation of {@link HAServiceTarget}, which returns
 * a mock implementation.
 */
class DummyHAService extends HAServiceTarget {
  volatile HAServiceState state;
  HAServiceProtocol proxy;
  NodeFencer fencer;
  InetSocketAddress address;
  boolean isHealthy = true;
  boolean actUnreachable = false;

  DummyHAService(HAServiceState state, InetSocketAddress address) {
    this.state = state;
    this.proxy = makeMock();
    this.fencer = Mockito.mock(NodeFencer.class);
    this.address = address;
  }
  
  private HAServiceProtocol makeMock() {
    return Mockito.spy(new HAServiceProtocol() {
      @Override
      public void monitorHealth() throws HealthCheckFailedException,
          AccessControlException, IOException {
        checkUnreachable();
        if (!isHealthy) {
          throw new HealthCheckFailedException("not healthy");
        }
      }

      @Override
      public void transitionToActive() throws ServiceFailedException,
          AccessControlException, IOException {
        checkUnreachable();
        state = HAServiceState.ACTIVE;
      }

      @Override
      public void transitionToStandby() throws ServiceFailedException,
          AccessControlException, IOException {
        checkUnreachable();
        state = HAServiceState.STANDBY;
      }

      @Override
      public HAServiceStatus getServiceStatus() throws IOException {
        checkUnreachable();
        HAServiceStatus ret = new HAServiceStatus(state);
        if (state == HAServiceState.STANDBY) {
          ret.setReadyToBecomeActive();
        }
        return ret;
      }

      private void checkUnreachable() throws IOException {
        if (actUnreachable) {
          throw new IOException("Connection refused (fake)");
        }
      }
    });
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public HAServiceProtocol getProxy(Configuration conf, int timeout)
      throws IOException {
    return proxy;
  }

  @Override
  public NodeFencer getFencer() {
    return fencer;
  }

  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
  }
}