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

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.junit.Assert;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Test facilities for unit tests for RPC. */
public class TestRpcBase {

  protected final static String SERVER_PRINCIPAL_KEY =
      "test.ipc.server.principal";
  protected final static String ADDRESS = "0.0.0.0";
  protected final static int PORT = 0;
  protected static InetSocketAddress addr;
  protected static Configuration conf;

  protected void setupConf() {
    conf = new Configuration();
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);
    UserGroupInformation.setConfiguration(conf);
  }

  protected static RPC.Builder newServerBuilder(
      Configuration serverConf) throws IOException {
    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // Get RPC server for server side implementation
    RPC.Builder builder = new RPC.Builder(serverConf)
        .setProtocol(TestRpcService.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(PORT);

    return builder;
  }

  protected static RPC.Server setupTestServer(Configuration serverConf,
                                       int numHandlers) throws IOException {
    return setupTestServer(serverConf, numHandlers, null);
  }

  protected static RPC.Server setupTestServer(Configuration serverConf,
                                       int numHandlers,
                                       SecretManager<?> serverSm)
      throws IOException {
    RPC.Builder builder = newServerBuilder(serverConf);

    if (numHandlers > 0) {
      builder.setNumHandlers(numHandlers);
    }

    if (serverSm != null) {
      builder.setSecretManager(serverSm);
    }

    return setupTestServer(builder);
  }

  protected static RPC.Server setupTestServer(RPC.Builder builder) throws IOException {
    RPC.Server server = builder.build();

    server.start();

    addr = NetUtils.getConnectAddress(server);

    return server;
  }

  protected static TestRpcService getClient(InetSocketAddress serverAddr,
                                     Configuration clientConf)
      throws ServiceException {
    try {
      return RPC.getProxy(TestRpcService.class, 0, serverAddr, clientConf);
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  protected static void stop(Server server, TestRpcService proxy) {
    if (proxy != null) {
      try {
        RPC.stopProxy(proxy);
      } catch (Exception ignored) {}
    }

    if (server != null) {
      try {
        server.stop();
      } catch (Exception ignored) {}
    }
  }

  /**
   * Count the number of threads that have a stack frame containing
   * the given string
   */
  protected static int countThreads(String search) {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains(search)) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  @ProtocolInfo(protocolName = "org.apache.hadoop.ipc.TestRpcBase$TestRpcService",
      protocolVersion = 1)
  public interface TestRpcService
      extends TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface {
  }

  public static class PBServerImpl implements TestRpcService {
    CountDownLatch fastPingCounter = new CountDownLatch(2);
    private List<Server.Call> postponedCalls = new ArrayList<>();

    @Override
    public TestProtos.EmptyResponseProto ping(RpcController unused,
                TestProtos.EmptyRequestProto request) throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(clientId);
      Assert.assertEquals(ClientId.BYTE_LENGTH, clientId.length);
      return TestProtos.EmptyResponseProto.newBuilder().build();
    }

    @Override
    public TestProtos.EchoResponseProto echo(
        RpcController unused, TestProtos.EchoRequestProto request)
        throws ServiceException {
      return TestProtos.EchoResponseProto.newBuilder().setMessage(
          request.getMessage())
          .build();
    }

    @Override
    public TestProtos.EmptyResponseProto error(
        RpcController unused, TestProtos.EmptyRequestProto request)
        throws ServiceException {
      throw new ServiceException("error", new RpcServerException("error"));
    }

    @Override
    public TestProtos.EmptyResponseProto error2(
        RpcController unused, TestProtos.EmptyRequestProto request)
        throws ServiceException {
      throw new ServiceException("error", new URISyntaxException("",
          "testException"));
    }

    @Override
    public TestProtos.EmptyResponseProto slowPing(
        RpcController unused, TestProtos.SlowPingRequestProto request)
        throws ServiceException {
      boolean shouldSlow = request.getShouldSlow();
      if (shouldSlow) {
        try {
          fastPingCounter.await(); //slow response until two fast pings happened
        } catch (InterruptedException ignored) {}
      } else {
        fastPingCounter.countDown();
      }

      return TestProtos.EmptyResponseProto.newBuilder().build();
    }

    @Override
    public TestProtos.EchoResponseProto2 echo2(
        RpcController controller, TestProtos.EchoRequestProto2 request)
        throws ServiceException {
      return TestProtos.EchoResponseProto2.newBuilder().addAllMessage(
          request.getMessageList()).build();
    }

    @Override
    public TestProtos.AddResponseProto add(
        RpcController controller, TestProtos.AddRequestProto request)
        throws ServiceException {
      return TestProtos.AddResponseProto.newBuilder().setResult(
          request.getParam1() + request.getParam2()).build();
    }

    @Override
    public TestProtos.AddResponseProto add2(
        RpcController controller, TestProtos.AddRequestProto2 request)
        throws ServiceException {
      int sum = 0;
      for (Integer num : request.getParamsList()) {
        sum += num;
      }
      return TestProtos.AddResponseProto.newBuilder().setResult(sum).build();
    }

    @Override
    public TestProtos.EmptyResponseProto testServerGet(
        RpcController controller, TestProtos.EmptyRequestProto request)
        throws ServiceException {
      if (!(Server.get() instanceof RPC.Server)) {
        throw new ServiceException("Server.get() failed");
      }
      return TestProtos.EmptyResponseProto.newBuilder().build();
    }

    @Override
    public TestProtos.ExchangeResponseProto exchange(
        RpcController controller, TestProtos.ExchangeRequestProto request)
        throws ServiceException {
      Integer[] values = new Integer[request.getValuesCount()];
      for (int i = 0; i < values.length; i++) {
        values[i] = i;
      }
      return TestProtos.ExchangeResponseProto.newBuilder()
          .addAllValues(Arrays.asList(values)).build();
    }

    @Override
    public TestProtos.EmptyResponseProto sleep(
        RpcController controller, TestProtos.SleepRequestProto request)
        throws ServiceException {
      try {
        Thread.sleep(request.getMilliSeconds());
      } catch (InterruptedException ignore) {}
      return  TestProtos.EmptyResponseProto.newBuilder().build();
    }
  }

  protected static TestProtos.EmptyRequestProto newEmptyRequest() {
    return TestProtos.EmptyRequestProto.newBuilder().build();
  }

  protected static TestProtos.EchoRequestProto newEchoRequest(String msg) {
    return TestProtos.EchoRequestProto.newBuilder().setMessage(msg).build();
  }

  protected static String convert(TestProtos.EchoResponseProto response) {
    return response.getMessage();
  }

  protected static TestProtos.SlowPingRequestProto newSlowPingRequest(
      boolean shouldSlow) throws ServiceException {
    return TestProtos.SlowPingRequestProto.newBuilder().
        setShouldSlow(shouldSlow).build();
  }

  protected static TestProtos.SleepRequestProto newSleepRequest(
      int milliSeconds) {
    return TestProtos.SleepRequestProto.newBuilder()
            .setMilliSeconds(milliSeconds).build();
  }
}
