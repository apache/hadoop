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

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcHandoffProto;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestProtoBufRpcServerHandoff {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestProtoBufRpcServerHandoff.class);

  @Test(timeout = 20000)
  public void test() throws Exception {
    Configuration conf = new Configuration();

    TestProtoBufRpcServerHandoffServer serverImpl =
        new TestProtoBufRpcServerHandoffServer();
    BlockingService blockingService =
        TestProtobufRpcHandoffProto.newReflectiveBlockingService(serverImpl);

    RPC.setProtocolEngine(conf, TestProtoBufRpcServerHandoffProtocol.class,
        ProtobufRpcEngine2.class);
    RPC.Server server = new RPC.Builder(conf)
        .setProtocol(TestProtoBufRpcServerHandoffProtocol.class)
        .setInstance(blockingService)
        .setVerbose(true)
        .setNumHandlers(1) // Num Handlers explicitly set to 1 for test.
        .build();
    server.start();

    InetSocketAddress address = server.getListenerAddress();
    long serverStartTime = System.currentTimeMillis();
    LOG.info("Server started at: " + address + " at time: " + serverStartTime);

    final TestProtoBufRpcServerHandoffProtocol client = RPC.getProxy(
        TestProtoBufRpcServerHandoffProtocol.class, 1, address, conf);

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CompletionService<ClientInvocationCallable> completionService =
        new ExecutorCompletionService<ClientInvocationCallable>(
            executorService);

    completionService.submit(new ClientInvocationCallable(client, 5000l));
    completionService.submit(new ClientInvocationCallable(client, 5000l));

    long submitTime = System.currentTimeMillis();
    Future<ClientInvocationCallable> future1 = completionService.take();
    Future<ClientInvocationCallable> future2 = completionService.take();

    ClientInvocationCallable callable1 = future1.get();
    ClientInvocationCallable callable2 = future2.get();

    LOG.info(callable1.toString());
    LOG.info(callable2.toString());

    // Ensure the 5 second sleep responses are within a reasonable time of each
    // other.
    Assert.assertTrue(Math.abs(callable1.endTime - callable2.endTime) < 2000l);
    Assert.assertTrue(System.currentTimeMillis() - submitTime < 7000l);

  }

  private static class ClientInvocationCallable
      implements Callable<ClientInvocationCallable> {
    final TestProtoBufRpcServerHandoffProtocol client;
    final long sleepTime;
    TestProtos.SleepResponseProto2 result;
    long startTime;
    long endTime;


    private ClientInvocationCallable(
        TestProtoBufRpcServerHandoffProtocol client, long sleepTime) {
      this.client = client;
      this.sleepTime = sleepTime;
    }

    @Override
    public ClientInvocationCallable call() throws Exception {
      startTime = System.currentTimeMillis();
      result = client.sleep(null,
          TestProtos.SleepRequestProto2.newBuilder().setSleepTime(sleepTime)
              .build());
      endTime = System.currentTimeMillis();
      return this;
    }

    @Override
    public String toString() {
      return "startTime=" + startTime + ", endTime=" + endTime +
          (result != null ?
              ", result.receiveTime=" + result.getReceiveTime() +
                  ", result.responseTime=" +
                  result.getResponseTime() : "");
    }
  }

  @ProtocolInfo(
      protocolName = "org.apache.hadoop.ipc.TestProtoBufRpcServerHandoff$TestProtoBufRpcServerHandoffProtocol",
      protocolVersion = 1)
  public interface TestProtoBufRpcServerHandoffProtocol
      extends TestProtobufRpcHandoffProto.BlockingInterface {
  }

  public static class TestProtoBufRpcServerHandoffServer
      implements TestProtoBufRpcServerHandoffProtocol {

    @Override
    public TestProtos.SleepResponseProto2 sleep
        (RpcController controller,
         TestProtos.SleepRequestProto2 request) throws
        ServiceException {
      final long startTime = System.currentTimeMillis();
      final ProtobufRpcEngineCallback2 callback =
          ProtobufRpcEngine2.Server.registerForDeferredResponse2();
      final long sleepTime = request.getSleepTime();
      new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          callback.setResponse(
              TestProtos.SleepResponseProto2.newBuilder()
                  .setReceiveTime(startTime)
                  .setResponseTime(System.currentTimeMillis()).build());
        }
      }.start();
      return null;
    }
  }
}
