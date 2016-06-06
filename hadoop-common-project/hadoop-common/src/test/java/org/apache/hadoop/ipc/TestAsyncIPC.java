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

package org.apache.hadoop.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.TestIPC.CallInfo;
import org.apache.hadoop.ipc.TestIPC.TestServer;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAsyncIPC {

  private static Configuration conf;
  private static final Log LOG = LogFactory.getLog(TestAsyncIPC.class);

  @Before
  public void setupConf() {
    conf = new Configuration();
    Client.setPingInterval(conf, TestIPC.PING_INTERVAL);
    // set asynchronous mode for main thread
    Client.setAsynchronousMode(true);
  }

  protected static class SerialCaller extends Thread {
    private Client client;
    private InetSocketAddress server;
    private int count;
    private boolean failed;
    Map<Integer, Future<LongWritable>> returnFutures =
        new HashMap<Integer, Future<LongWritable>>();
    Map<Integer, Long> expectedValues = new HashMap<Integer, Long>();

    public SerialCaller(Client client, InetSocketAddress server, int count) {
      this.client = client;
      this.server = server;
      this.count = count;
      // set asynchronous mode, since SerialCaller extends Thread
      Client.setAsynchronousMode(true);
    }

    @Override
    public void run() {
      // in case Thread#Start is called, which will spawn new thread
      Client.setAsynchronousMode(true);
      for (int i = 0; i < count; i++) {
        try {
          final long param = TestIPC.RANDOM.nextLong();
          TestIPC.call(client, param, server, conf);
          Future<LongWritable> returnFuture = Client.getReturnRpcResponse();
          returnFutures.put(i, returnFuture);
          expectedValues.put(i, param);
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }

    public void waitForReturnValues() throws InterruptedException,
        ExecutionException {
      for (int i = 0; i < count; i++) {
        LongWritable value = returnFutures.get(i).get();
        if (expectedValues.get(i) != value.get()) {
          LOG.fatal(String.format("Call-%d failed!", i));
          failed = true;
          break;
        }
      }
    }
  }

  @Test
  public void testSerial() throws IOException, InterruptedException,
      ExecutionException {
    internalTestSerial(3, false, 2, 5, 100);
    internalTestSerial(3, true, 2, 5, 10);
  }

  public void internalTestSerial(int handlerCount, boolean handlerSleep,
      int clientCount, int callerCount, int callCount) throws IOException,
      InterruptedException, ExecutionException {
    Server server = new TestIPC.TestServer(handlerCount, handlerSleep, conf);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }

    SerialCaller[] callers = new SerialCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new SerialCaller(clients[i % clientCount], addr, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      callers[i].waitForReturnValues();
      String msg = String.format("Expected not failed for caller-%d: %s.", i,
          callers[i]);
      assertFalse(msg, callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    server.stop();
  }

  /**
   * Test if (1) the rpc server uses the call id/retry provided by the rpc
   * client, and (2) the rpc client receives the same call id/retry from the rpc
   * server.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test(timeout = 60000)
  public void testCallIdAndRetry() throws IOException, InterruptedException,
      ExecutionException {
    final Map<Integer, CallInfo> infoMap = new HashMap<Integer, CallInfo>();

    // Override client to store the call info and check response
    final Client client = new Client(LongWritable.class, conf) {
      @Override
      Call createCall(RpcKind rpcKind, Writable rpcRequest) {
        // Set different call id and retry count for the next call
        Client.setCallIdAndRetryCount(Client.nextCallId(),
            TestIPC.RANDOM.nextInt(255));

        final Call call = super.createCall(rpcKind, rpcRequest);

        CallInfo info = new CallInfo();
        info.id = call.id;
        info.retry = call.retry;
        infoMap.put(call.id, info);

        return call;
      }

      @Override
      void checkResponse(RpcResponseHeaderProto header) throws IOException {
        super.checkResponse(header);
        Assert.assertEquals(infoMap.get(header.getCallId()).retry,
            header.getRetryCount());
      }
    };

    // Attach a listener that tracks every call received by the server.
    final TestServer server = new TestIPC.TestServer(1, false, conf);
    server.callListener = new Runnable() {
      @Override
      public void run() {
        Assert.assertEquals(infoMap.get(Server.getCallId()).retry,
            Server.getCallRetryCount());
      }
    };

    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      server.start();
      final SerialCaller caller = new SerialCaller(client, addr, 4);
      caller.run();
      caller.waitForReturnValues();
      String msg = String.format("Expected not failed for caller: %s.", caller);
      assertFalse(msg, caller.failed);
    } finally {
      client.stop();
      server.stop();
    }
  }

  /**
   * Test if the rpc server gets the retry count from client.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test(timeout = 60000)
  public void testCallRetryCount() throws IOException, InterruptedException,
      ExecutionException {
    final int retryCount = 255;
    // Override client to store the call id
    final Client client = new Client(LongWritable.class, conf);
    Client.setCallIdAndRetryCount(Client.nextCallId(), retryCount);

    // Attach a listener that tracks every call ID received by the server.
    final TestServer server = new TestIPC.TestServer(1, false, conf);
    server.callListener = new Runnable() {
      @Override
      public void run() {
        // we have not set the retry count for the client, thus on the server
        // side we should see retry count as 0
        Assert.assertEquals(retryCount, Server.getCallRetryCount());
      }
    };

    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      server.start();
      final SerialCaller caller = new SerialCaller(client, addr, 10);
      caller.run();
      caller.waitForReturnValues();
      String msg = String.format("Expected not failed for caller: %s.", caller);
      assertFalse(msg, caller.failed);
    } finally {
      client.stop();
      server.stop();
    }
  }

  /**
   * Test if the rpc server gets the default retry count (0) from client.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test(timeout = 60000)
  public void testInitialCallRetryCount() throws IOException,
      InterruptedException, ExecutionException {
    // Override client to store the call id
    final Client client = new Client(LongWritable.class, conf);

    // Attach a listener that tracks every call ID received by the server.
    final TestServer server = new TestIPC.TestServer(1, false, conf);
    server.callListener = new Runnable() {
      @Override
      public void run() {
        // we have not set the retry count for the client, thus on the server
        // side we should see retry count as 0
        Assert.assertEquals(0, Server.getCallRetryCount());
      }
    };

    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      server.start();
      final SerialCaller caller = new SerialCaller(client, addr, 10);
      caller.run();
      caller.waitForReturnValues();
      String msg = String.format("Expected not failed for caller: %s.", caller);
      assertFalse(msg, caller.failed);
    } finally {
      client.stop();
      server.stop();
    }
  }

  /**
   * Tests that client generates a unique sequential call ID for each RPC call,
   * even if multiple threads are using the same client.
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Test(timeout = 60000)
  public void testUniqueSequentialCallIds() throws IOException,
      InterruptedException, ExecutionException {
    int serverThreads = 10, callerCount = 100, perCallerCallCount = 100;
    TestServer server = new TestIPC.TestServer(serverThreads, false, conf);

    // Attach a listener that tracks every call ID received by the server. This
    // list must be synchronized, because multiple server threads will add to
    // it.
    final List<Integer> callIds = Collections
        .synchronizedList(new ArrayList<Integer>());
    server.callListener = new Runnable() {
      @Override
      public void run() {
        callIds.add(Server.getCallId());
      }
    };

    Client client = new Client(LongWritable.class, conf);

    try {
      InetSocketAddress addr = NetUtils.getConnectAddress(server);
      server.start();
      SerialCaller[] callers = new SerialCaller[callerCount];
      for (int i = 0; i < callerCount; ++i) {
        callers[i] = new SerialCaller(client, addr, perCallerCallCount);
        callers[i].start();
      }
      for (int i = 0; i < callerCount; ++i) {
        callers[i].join();
        callers[i].waitForReturnValues();
        String msg = String.format("Expected not failed for caller-%d: %s.", i,
            callers[i]);
        assertFalse(msg, callers[i].failed);
      }
    } finally {
      client.stop();
      server.stop();
    }

    int expectedCallCount = callerCount * perCallerCallCount;
    assertEquals(expectedCallCount, callIds.size());

    // It is not guaranteed that the server executes requests in sequential
    // order
    // of client call ID, so we must sort the call IDs before checking that it
    // contains every expected value.
    Collections.sort(callIds);
    final int startID = callIds.get(0).intValue();
    for (int i = 0; i < expectedCallCount; ++i) {
      assertEquals(startID + i, callIds.get(i).intValue());
    }
  }
}