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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestRpcServerHandoff {

  public static final Log LOG =
      LogFactory.getLog(TestRpcServerHandoff.class);

  private static final String BIND_ADDRESS = "0.0.0.0";
  private static final Configuration conf = new Configuration();


  public static class ServerForHandoffTest extends Server {

    private final AtomicBoolean invoked = new AtomicBoolean(false);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition invokedCondition = lock.newCondition();

    private volatile Writable request;
    private volatile Call deferredCall;

    protected ServerForHandoffTest(int handlerCount) throws IOException {
      super(BIND_ADDRESS, 0, BytesWritable.class, handlerCount, conf);
    }

    @Override
    public Writable call(RPC.RpcKind rpcKind, String protocol, Writable param,
                         long receiveTime) throws Exception {
      request = param;
      deferredCall = Server.getCurCall().get();
      Server.getCurCall().get().deferResponse();
      lock.lock();
      try {
        invoked.set(true);
        invokedCondition.signal();
      } finally {
        lock.unlock();
      }
      return null;
    }

    void awaitInvocation() throws InterruptedException {
      lock.lock();
      try {
        while (!invoked.get()) {
          invokedCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }

    void sendResponse() {
      deferredCall.setDeferredResponse(request);
    }

    void sendError() {
      deferredCall.setDeferredError(new IOException("DeferredError"));
    }
  }

  @Test(timeout = 10000)
  public void testDeferredResponse() throws IOException, InterruptedException,
      ExecutionException {


    ServerForHandoffTest server = new ServerForHandoffTest(2);
    server.start();
    try {
      InetSocketAddress serverAddress = NetUtils.getConnectAddress(server);
      byte[] requestBytes = generateRandomBytes(1024);
      ClientCallable clientCallable =
          new ClientCallable(serverAddress, conf, requestBytes);

      FutureTask<Writable> future = new FutureTask<Writable>(clientCallable);
      Thread clientThread = new Thread(future);
      clientThread.start();

      server.awaitInvocation();
      awaitResponseTimeout(future);

      server.sendResponse();
      BytesWritable response = (BytesWritable) future.get();

      Assert.assertEquals(new BytesWritable(requestBytes), response);
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  @Test(timeout = 10000)
  public void testDeferredException() throws IOException, InterruptedException,
      ExecutionException {
    ServerForHandoffTest server = new ServerForHandoffTest(2);
    server.start();
    try {
      InetSocketAddress serverAddress = NetUtils.getConnectAddress(server);
      byte[] requestBytes = generateRandomBytes(1024);
      ClientCallable clientCallable =
          new ClientCallable(serverAddress, conf, requestBytes);

      FutureTask<Writable> future = new FutureTask<Writable>(clientCallable);
      Thread clientThread = new Thread(future);
      clientThread.start();

      server.awaitInvocation();
      awaitResponseTimeout(future);

      server.sendError();
      try {
        future.get();
        Assert.fail("Call succeeded. Was expecting an exception");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        Assert.assertTrue(cause instanceof RemoteException);
        RemoteException re = (RemoteException) cause;
        Assert.assertTrue(re.toString().contains("DeferredError"));
      }
    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private void awaitResponseTimeout(FutureTask<Writable> future) throws
      ExecutionException,
      InterruptedException {
    long sleepTime = 3000L;
    while (sleepTime > 0) {
      try {
        future.get(200L, TimeUnit.MILLISECONDS);
        Assert.fail("Expected to timeout since" +
            " the deferred response hasn't been registered");
      } catch (TimeoutException e) {
        // Ignoring. Expected to time out.
      }
      sleepTime -= 200L;
    }
    LOG.info("Done sleeping");
  }

  private static class ClientCallable implements Callable<Writable> {

    private final InetSocketAddress address;
    private final Configuration conf;
    final byte[] requestBytes;


    private ClientCallable(InetSocketAddress address, Configuration conf,
                           byte[] requestBytes) {
      this.address = address;
      this.conf = conf;
      this.requestBytes = requestBytes;
    }

    @Override
    public Writable call() throws Exception {
      Client client = new Client(BytesWritable.class, conf);
      Writable param = new BytesWritable(requestBytes);
      final Client.ConnectionId remoteId =
          Client.ConnectionId.getConnectionId(address, null,
              null, 0, null, conf);
      Writable result = client.call(RPC.RpcKind.RPC_BUILTIN, param, remoteId,
          new AtomicBoolean(false));
      return result;
    }
  }

  private byte[] generateRandomBytes(int length) {
    Random random = new Random();
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) ('a' + random.nextInt(26));
    }
    return bytes;
  }
}
