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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.security.AccessControlException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Test-only implementation of {@link HAServiceTarget}, which returns
 * a mock implementation.
 */
class DummyHAService extends HAServiceTarget {
  public static final Log LOG = LogFactory.getLog(DummyHAService.class);
  private static final String DUMMY_FENCE_KEY = "dummy.fence.key";
  volatile HAServiceState state;
  HAServiceProtocol proxy;
  ZKFCProtocol zkfcProxy = null;
  NodeFencer fencer;
  InetSocketAddress address;
  boolean isHealthy = true;
  boolean actUnreachable = false;
  boolean failToBecomeActive, failToBecomeStandby, failToFence;
  
  DummySharedResource sharedResource;
  public int fenceCount = 0;
  public int activeTransitionCount = 0;
  
  static ArrayList<DummyHAService> instances = Lists.newArrayList();
  int index;

  DummyHAService(HAServiceState state, InetSocketAddress address) {
    this.state = state;
    this.proxy = makeMock();
    try {
      Configuration conf = new Configuration();
      conf.set(DUMMY_FENCE_KEY, DummyFencer.class.getName()); 
      this.fencer = Mockito.spy(
          NodeFencer.create(conf, DUMMY_FENCE_KEY));
    } catch (BadFencingConfigurationException e) {
      throw new RuntimeException(e);
    }
    this.address = address;
    synchronized (instances) {
      instances.add(this);
      this.index = instances.size();
    }
  }
  
  public void setSharedResource(DummySharedResource rsrc) {
    this.sharedResource = rsrc;
  }
  
  private HAServiceProtocol makeMock() {
    return Mockito.spy(new MockHAProtocolImpl());
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public InetSocketAddress getZKFCAddress() {
    return null;
  }

  @Override
  public HAServiceProtocol getProxy(Configuration conf, int timeout)
      throws IOException {
    return proxy;
  }
  
  @Override
  public ZKFCProtocol getZKFCProxy(Configuration conf, int timeout)
      throws IOException {
    assert zkfcProxy != null;
    return zkfcProxy;
  }
  
  @Override
  public NodeFencer getFencer() {
    return fencer;
  }

  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
  }
  
  @Override
  public boolean isAutoFailoverEnabled() {
    return true;
  }

  @Override
  public String toString() {
    return "DummyHAService #" + index;
  }

  public static HAServiceTarget getInstance(int serial) {
    return instances.get(serial - 1);
  }
  
  private class MockHAProtocolImpl implements
      HAServiceProtocol, Closeable {
    @Override
    public void monitorHealth() throws HealthCheckFailedException,
        AccessControlException, IOException {
      checkUnreachable();
      if (!isHealthy) {
        throw new HealthCheckFailedException("not healthy");
      }
    }
    
    @Override
    public void transitionToActive(StateChangeRequestInfo req) throws ServiceFailedException,
        AccessControlException, IOException {
      activeTransitionCount++;
      checkUnreachable();
      if (failToBecomeActive) {
        throw new ServiceFailedException("injected failure");
      }
      if (sharedResource != null) {
        sharedResource.take(DummyHAService.this);
      }
      state = HAServiceState.ACTIVE;
    }
    
    @Override
    public void transitionToStandby(StateChangeRequestInfo req) throws ServiceFailedException,
        AccessControlException, IOException {
      checkUnreachable();
      if (failToBecomeStandby) {
        throw new ServiceFailedException("injected failure");
      }
      if (sharedResource != null) {
        sharedResource.release(DummyHAService.this);
      }
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
    
    @Override
    public void close() throws IOException {
    }
  }
  
  public static class DummyFencer implements FenceMethod {
    @Override
    public void checkArgs(String args) throws BadFencingConfigurationException {
    }

    @Override
    public boolean tryFence(HAServiceTarget target, String args)
        throws BadFencingConfigurationException {
      LOG.info("tryFence(" + target + ")");
      DummyHAService svc = (DummyHAService)target;
      synchronized (svc) {
        svc.fenceCount++;
      }
      if (svc.failToFence) {
        LOG.info("Injected failure to fence");
        return false;
      }
      svc.sharedResource.release(svc);
      return true;
    }
  }

}
