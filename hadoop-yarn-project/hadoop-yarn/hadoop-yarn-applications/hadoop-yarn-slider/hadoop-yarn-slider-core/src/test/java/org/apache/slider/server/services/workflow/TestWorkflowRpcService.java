/*
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

package org.apache.slider.server.services.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TestWorkflowRpcService extends WorkflowServiceTestBase {

  @Test
  public void testCreateMockRPCService() throws Throwable {
    MockRPC rpc = new MockRPC();
    rpc.start();
    assertTrue(rpc.started);
    rpc.getListenerAddress();
    rpc.stop();
    assertTrue(rpc.stopped);
  }

  @Test
  public void testLifecycle() throws Throwable {
    MockRPC rpc = new MockRPC();
    WorkflowRpcService svc = new WorkflowRpcService("test", rpc);
    run(svc);
    assertTrue(rpc.started);
    svc.getConnectAddress();
    svc.stop();
    assertTrue(rpc.stopped);
  }

  @Test
  public void testStartFailure() throws Throwable {
    MockRPC rpc = new MockRPC();
    rpc.failOnStart = true;
    WorkflowRpcService svc = new WorkflowRpcService("test", rpc);
    svc.init(new Configuration());
    try {
      svc.start();
      fail("expected an exception");
    } catch (RuntimeException e) {
      assertEquals("failOnStart", e.getMessage());
    }
    svc.stop();
    assertTrue(rpc.stopped);
  }

  private static class MockRPC extends Server {

    public boolean stopped;
    public boolean started;
    public boolean failOnStart;

    private MockRPC() throws IOException {
      super("localhost", 0, null, 1, new Configuration());
    }

    @Override
    public synchronized void start() {
      if (failOnStart) {
        throw new RuntimeException("failOnStart");
      }
      started = true;
      super.start();
    }

    @Override
    public synchronized void stop() {
      stopped = true;
      super.stop();
    }

    @Override
    public synchronized InetSocketAddress getListenerAddress() {
      return super.getListenerAddress();
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind,
        String protocol,
        Writable param,
        long receiveTime) throws Exception {
      return null;
    }
  }
}
