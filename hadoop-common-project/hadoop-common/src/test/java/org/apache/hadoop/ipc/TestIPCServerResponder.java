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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test provokes partial writes in the server, which is 
 * serving multiple clients.
 */
public class TestIPCServerResponder {

  public static final Log LOG = 
            LogFactory.getLog(TestIPCServerResponder.class);

  private static Configuration conf = new Configuration();

  private static final Random RANDOM = new Random();

  private static final String ADDRESS = "0.0.0.0";

  private static final int BYTE_COUNT = 1024;
  private static final byte[] BYTES = new byte[BYTE_COUNT];
  static {
    for (int i = 0; i < BYTE_COUNT; i++)
      BYTES[i] = (byte) ('a' + (i % 26));
  }

  private static class TestServer extends Server {

    private boolean sleep;

    public TestServer(final int handlerCount, final boolean sleep) 
                                              throws IOException {
      super(ADDRESS, 0, BytesWritable.class, handlerCount, conf);
      // Set the buffer size to half of the maximum parameter/result size 
      // to force the socket to block
      this.setSocketSendBufSize(BYTE_COUNT / 2);
      this.sleep = sleep;
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param,
        long receiveTime) throws IOException {
      if (sleep) {
        try {
          Thread.sleep(RANDOM.nextInt(20)); // sleep a bit
        } catch (InterruptedException e) {}
      }
      return param;
    }
  }

  private static class Caller extends Thread {

    private Client client;
    private int count;
    private InetSocketAddress address;
    private boolean failed;

    public Caller(final Client client, final InetSocketAddress address, 
                                       final int count) {
      this.client = client;
      this.address = address;
      this.count = count;
    }

    @Override
    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          int byteSize = RANDOM.nextInt(BYTE_COUNT);
          byte[] bytes = new byte[byteSize];
          System.arraycopy(BYTES, 0, bytes, 0, byteSize);
          Writable param = new BytesWritable(bytes);
          client.call(param, address);
          Thread.sleep(RANDOM.nextInt(20));
        } catch (Exception e) {
          LOG.fatal("Caught Exception", e);
          failed = true;
        }
      }
    }
  }

  @Test
  public void testResponseBuffer() 
      throws IOException, InterruptedException {
    Server.INITIAL_RESP_BUF_SIZE = 1;
    conf.setInt(CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
                1);
    checkServerResponder(1, true, 1, 1, 5);
    conf = new Configuration(); // reset configuration
  }

  @Test
  public void testServerResponder()
      throws IOException, InterruptedException {
    checkServerResponder(10, true, 1, 10, 200);
  }

  public void checkServerResponder(final int handlerCount, 
                                  final boolean handlerSleep, 
                                  final int clientCount,
                                  final int callerCount,
                                  final int callCount) throws IOException,
                                  InterruptedException {
    Server server = new TestServer(handlerCount, handlerSleep);
    server.start();

    InetSocketAddress address = NetUtils.getConnectAddress(server);
    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(BytesWritable.class, conf);
    }

    Caller[] callers = new Caller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new Caller(clients[i % clientCount], address, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    server.stop();
  }

  // Test that IPC calls can be marked for a deferred response.
  // call 0: immediate
  // call 1: immediate
  // call 2: delayed with wait for 1 sendResponse, check if blocked
  // call 3: immediate, proves handler is freed
  // call 4: delayed with wait for 2 sendResponses, check if blocked
  // call 2: sendResponse, should return
  // call 4: sendResponse, should remain blocked
  // call 5: immediate, prove handler is still free
  // call 4: sendResponse, expect it to return
  @Test(timeout=10000)
  public void testDeferResponse() throws IOException, InterruptedException {
    final AtomicReference<Call> deferredCall = new AtomicReference<Call>();
    final AtomicInteger count = new AtomicInteger();
    final Writable wait0 = new IntWritable(0);
    final Writable wait1 = new IntWritable(1);
    final Writable wait2 = new IntWritable(2);

    // use only 1 handler to prove it's freed after every call
    Server server = new Server(ADDRESS, 0, IntWritable.class, 1, conf){
      @Override
      public Writable call(RPC.RpcKind rpcKind, String protocol,
          Writable waitCount, long receiveTime) throws IOException {
        Call call = Server.getCurCall().get();
        int wait = ((IntWritable)waitCount).get();
        while (wait-- > 0) {
          call.postponeResponse();
          deferredCall.set(call);
        }
        return new IntWritable(count.getAndIncrement());
      }
    };
    server.start();

    final InetSocketAddress address = NetUtils.getConnectAddress(server);
    final Client client = new Client(IntWritable.class, conf);
    Call[] waitingCalls = new Call[2];

    // calls should return immediately, check the sequence number is
    // increasing
    assertEquals(0,
        ((IntWritable)client.call(wait0, address)).get());
    assertEquals(1,
        ((IntWritable)client.call(wait0, address)).get());

    // do a call in the background that will have a deferred response
    final ExecutorService exec = Executors.newCachedThreadPool();
    Future<Integer> future1 = exec.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws IOException {
        return ((IntWritable)client.call(wait1, address)).get();
      }
    });
    // make sure it blocked
    try {
      future1.get(1, TimeUnit.SECONDS);
      Assert.fail("ipc shouldn't have responded");
    } catch (TimeoutException te) {
      // ignore, expected
    } catch (Exception ex) {
      Assert.fail("unexpected exception:"+ex);
    }
    assertFalse(future1.isDone());
    waitingCalls[0] = deferredCall.get();
    assertNotNull(waitingCalls[0]);

    // proves the handler isn't tied up, and that the prior sequence number
    // was consumed
    assertEquals(3,
        ((IntWritable)client.call(wait0, address)).get());

    // another call with wait count of 2
    Future<Integer> future2 = exec.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws IOException {
        return ((IntWritable)client.call(wait2, address)).get();
      }
    });
    // make sure it blocked
    try {
      future2.get(1, TimeUnit.SECONDS);
      Assert.fail("ipc shouldn't have responded");
    } catch (TimeoutException te) {
      // ignore, expected
    } catch (Exception ex) {
      Assert.fail("unexpected exception:"+ex);
    }
    assertFalse(future2.isDone());
    waitingCalls[1] = deferredCall.get();
    assertNotNull(waitingCalls[1]);

    // the background calls should still be blocked
    assertFalse(future1.isDone());
    assertFalse(future2.isDone());

    // trigger responses
    waitingCalls[0].sendResponse();
    waitingCalls[1].sendResponse();
    try {
      int val = future1.get(1, TimeUnit.SECONDS);
      assertEquals(2, val);
    } catch (Exception ex) {
      Assert.fail("unexpected exception:"+ex);
    }

    // make sure it's still blocked
    try {
      future2.get(1, TimeUnit.SECONDS);
      Assert.fail("ipc shouldn't have responded");
    } catch (TimeoutException te) {
      // ignore, expected
    } catch (Exception ex) {
      Assert.fail("unexpected exception:"+ex);
    }
    assertFalse(future2.isDone());

    // call should return immediately
    assertEquals(5,
        ((IntWritable)client.call(wait0, address)).get());

    // trigger last waiting call
    waitingCalls[1].sendResponse();
    try {
      int val = future2.get(1, TimeUnit.SECONDS);
      assertEquals(4, val);
    } catch (Exception ex) {
      Assert.fail("unexpected exception:"+ex);
    }

    server.stop();
  }
}
