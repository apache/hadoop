/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.protocolPB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.TestRPC;
import org.apache.hadoop.ipc.TestRpcBase;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ForkJoinPool;

import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAsyncRpcProtocolPBUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncRpcProtocolPBUtil.class);
  private static final int SERVER_PROCESS_COST_MS = 100;
  private TestClientProtocolTranslatorPB clientPB;
  private Server rpcServer;

  @Before
  public void setUp() throws IOException {
    AsyncRpcProtocolPBUtil.setWorker(ForkJoinPool.commonPool());
    Configuration conf = new Configuration();
    RPC.setProtocolEngine(conf, TestRpcBase.TestRpcService.class,
        ProtobufRpcEngine2.class);

    // Create server side implementation
    TestClientProtocolServerSideTranslatorPB serverImpl =
        new TestClientProtocolServerSideTranslatorPB(SERVER_PROCESS_COST_MS);
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // start the IPC server
    rpcServer = new RPC.Builder(conf)
        .setProtocol(TestRpcBase.TestRpcService.class)
        .setInstance(service).setBindAddress("0.0.0.0")
        .setPort(0).setNumHandlers(1).setVerbose(true).build();

    rpcServer.start();

    InetSocketAddress addr = NetUtils.getConnectAddress(rpcServer);

    TestRpcBase.TestRpcService proxy = RPC.getProxy(TestRpcBase.TestRpcService.class,
        TestRPC.TestProtocol.versionID, addr, conf);
    clientPB = new TestClientProtocolTranslatorPB(proxy);
    Client.setAsynchronousMode(true);
    clientPB.ping();
  }

  @After
  public void clear() {
    if (clientPB != null) {
      clientPB.close();
    }
    if (rpcServer != null) {
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncIpcClient() throws Exception {
    Client.setAsynchronousMode(true);
    long start = Time.monotonicNow();
    clientPB.add(1, 2);
    long cost = Time.monotonicNow() - start;
    LOG.info("rpc client add {} {}, cost: {}ms", 1, 2, cost);
    Integer res = syncReturn(Integer.class);
    checkResult(3, res, cost);

    start = Time.monotonicNow();
    clientPB.echo("test echo!");
    cost = Time.monotonicNow() - start;
    LOG.info("rpc client echo {}, cost: {}ms", "test echo!", cost);
    String value = syncReturn(String.class);
    checkResult("test echo!", value, cost);

    start = Time.monotonicNow();
    clientPB.error();
    LOG.info("rpc client error, cost: {}ms", Time.monotonicNow() - start);
    LambdaTestUtils.intercept(RemoteException.class, "test!",
        () -> AsyncUtil.syncReturn(String.class));
  }

  private void checkResult(Object expected, Object actual, long cost) {
    assertTrue(cost < SERVER_PROCESS_COST_MS);
    assertEquals(expected, actual);
  }
}
