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

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolPB;
import org.apache.hadoop.ha.protocolPB.HAServiceProtocolServerSideTranslatorPB;
import org.apache.hadoop.ha.proto.HAServiceProtocolProtos.HAServiceProtocolService;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HA_HM_RPC_TIMEOUT_DEFAULT;

/**
 * Test-only implementation of {@link HAServiceTarget}, which returns
 * a mock implementation.
 */
class DummyHAService extends HAServiceTarget {
  public static final Logger LOG = LoggerFactory.getLogger(DummyHAService
      .class);
  private static final String DUMMY_FENCE_KEY = "dummy.fence.key";
  volatile HAServiceState state;
  HAServiceProtocol proxy, healthMonitorProxy;
  ZKFCProtocol zkfcProxy = null;
  NodeFencer fencer;
  InetSocketAddress address, healthMonitorAddress;
  boolean isHealthy = true;
  boolean actUnreachable = false;
  boolean failToBecomeActive, failToBecomeStandby, failToFence;
  
  DummySharedResource sharedResource;
  public int fenceCount = 0;
  public int activeTransitionCount = 0;
  boolean testWithProtoBufRPC = false;
  
  static ArrayList<DummyHAService> instances = Lists.newArrayList();
  int index;

  DummyHAService(HAServiceState state, InetSocketAddress address) {
    this(state, address, false);
  }

  DummyHAService(HAServiceState state, InetSocketAddress address,
      boolean testWithProtoBufRPC) {
    this.state = state;
    this.testWithProtoBufRPC = testWithProtoBufRPC;
    if (testWithProtoBufRPC) {
      this.address = startAndGetRPCServerAddress(address);
    } else {
      this.address = address;
    }
    Configuration conf = new Configuration();
    this.proxy = makeMock(conf, HA_HM_RPC_TIMEOUT_DEFAULT);
    this.healthMonitorProxy = makeHealthMonitorMock(conf, HA_HM_RPC_TIMEOUT_DEFAULT);
    try {
      conf.set(DUMMY_FENCE_KEY, DummyFencer.class.getName());
      this.fencer = Mockito.spy(
          NodeFencer.create(conf, DUMMY_FENCE_KEY));
    } catch (BadFencingConfigurationException e) {
      throw new RuntimeException(e);
    }
    synchronized (instances) {
      instances.add(this);
      this.index = instances.size();
    }
  }

  DummyHAService(HAServiceState state, InetSocketAddress address,
        InetSocketAddress healthMonitorAddress, boolean testWithProtoBufRPC) {
    this(state, address, testWithProtoBufRPC);
    if (testWithProtoBufRPC) {
      this.healthMonitorAddress = startAndGetRPCServerAddress(
          healthMonitorAddress);
    } else {
      this.healthMonitorAddress = healthMonitorAddress;
    }
  }

  public void setSharedResource(DummySharedResource rsrc) {
    this.sharedResource = rsrc;
  }

  private InetSocketAddress startAndGetRPCServerAddress(InetSocketAddress serverAddress) {
    Configuration conf = new Configuration();

    try {
      RPC.setProtocolEngine(conf,
          HAServiceProtocolPB.class, ProtobufRpcEngine.class);
      HAServiceProtocolServerSideTranslatorPB haServiceProtocolXlator =
          new HAServiceProtocolServerSideTranslatorPB(new MockHAProtocolImpl());
      BlockingService haPbService = HAServiceProtocolService
          .newReflectiveBlockingService(haServiceProtocolXlator);

      Server server = new RPC.Builder(conf)
          .setProtocol(HAServiceProtocolPB.class)
          .setInstance(haPbService)
          .setBindAddress(serverAddress.getHostName())
          .setPort(serverAddress.getPort()).build();
      server.start();
      return NetUtils.getConnectAddress(server);
    } catch (IOException e) {
      return null;
    }
  }

  private HAServiceProtocol makeMock(Configuration conf, int timeoutMs) {
    HAServiceProtocol service;
    if (!testWithProtoBufRPC) {
      service = new MockHAProtocolImpl();
    } else {
      try {
        service = super.getProxy(conf, timeoutMs);
      } catch (IOException e) {
        return null;
      }
    }
    return Mockito.spy(service);
  }

  private HAServiceProtocol makeHealthMonitorMock(Configuration conf,
      int timeoutMs) {
    HAServiceProtocol service;
    if (!testWithProtoBufRPC) {
      service = new MockHAProtocolImpl();
    } else {
      try {
        service = super.getHealthMonitorProxy(conf, timeoutMs);
      } catch (IOException e) {
        return null;
      }
    }
    return Mockito.spy(service);
  }

  @Override
  public InetSocketAddress getAddress() {
    return address;
  }

  @Override
  public InetSocketAddress getHealthMonitorAddress() {
    return healthMonitorAddress;
  }

  @Override
  public InetSocketAddress getZKFCAddress() {
    return null;
  }

  @Override
  public HAServiceProtocol getProxy(Configuration conf, int timeout)
      throws IOException {
    if (testWithProtoBufRPC) {
      proxy = makeMock(conf, timeout);
    }
    return proxy;
  }

  @Override
  public HAServiceProtocol getHealthMonitorProxy(Configuration conf,
      int timeout) throws IOException {
    if (testWithProtoBufRPC) {
      proxy = makeHealthMonitorMock(conf, timeout);
    }
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
      if (state == HAServiceState.STANDBY || state == HAServiceState.ACTIVE) {
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
