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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtosLegacy;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpc2Proto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtosLegacy;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in src/test/proto
 * and protobuf service definition from src/test/test_rpc_service.proto
 */
@RunWith(Parameterized.class)
public class TestProtoBufRpc extends TestRpcBase {
  private static RPC.Server server;
  private final static int SLEEP_DURATION = 1000;

  /**
   * Test with legacy protobuf implementation in same server.
   */
  private boolean testWithLegacy;
  /**
   * Test with legacy protobuf implementation loaded first while creating the
   * RPC server.
   */
  private boolean testWithLegacyFirst;

  public TestProtoBufRpc(Boolean testWithLegacy, Boolean testWithLegacyFirst) {
    this.testWithLegacy = testWithLegacy;
    this.testWithLegacyFirst = testWithLegacyFirst;
  }

  @ProtocolInfo(protocolName = "testProto2", protocolVersion = 1)
  public interface TestRpcService2 extends
      TestProtobufRpc2Proto.BlockingInterface {
  }

  @ProtocolInfo(protocolName="testProtoLegacy", protocolVersion = 1)
  public interface TestRpcService2Legacy
      extends TestRpcServiceProtosLegacy.
      TestProtobufRpc2Proto.BlockingInterface {
  }

  public static class PBServer2Impl implements TestRpcService2 {

    @Override
    public EmptyResponseProto ping2(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo2(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public TestProtos.SleepResponseProto sleep(RpcController controller,
      TestProtos.SleepRequestProto request) throws ServiceException {
      try{
        Thread.sleep(request.getMilliSeconds());
      } catch (InterruptedException ex){
      }
      return  TestProtos.SleepResponseProto.newBuilder().build();
    }
  }

  public static class PBServer2ImplLegacy implements TestRpcService2Legacy {

    @Override
    public TestProtosLegacy.EmptyResponseProto ping2(
        com.google.protobuf.RpcController unused,
        TestProtosLegacy.EmptyRequestProto request)
        throws com.google.protobuf.ServiceException {
      return TestProtosLegacy.EmptyResponseProto.newBuilder().build();
    }

    @Override
    public TestProtosLegacy.EchoResponseProto echo2(
        com.google.protobuf.RpcController unused,
        TestProtosLegacy.EchoRequestProto request)
        throws com.google.protobuf.ServiceException {
      return TestProtosLegacy.EchoResponseProto.newBuilder()
          .setMessage(request.getMessage()).build();
    }

    @Override
    public TestProtosLegacy.SleepResponseProto sleep(
        com.google.protobuf.RpcController controller,
        TestProtosLegacy.SleepRequestProto request)
        throws com.google.protobuf.ServiceException {
      try {
        Thread.sleep(request.getMilliSeconds());
      } catch (InterruptedException ex) {
      }
      return TestProtosLegacy.SleepResponseProto.newBuilder().build();
    }
  }

  @Parameters
  public static Collection<Object[]> params() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[] {Boolean.TRUE, Boolean.TRUE });
    params.add(new Object[] {Boolean.TRUE, Boolean.FALSE });
    params.add(new Object[] {Boolean.FALSE, Boolean.FALSE });
    return params;
  }

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() throws IOException { // Setup server for both protocols
    conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 1024);
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_LOG_SLOW_RPC, true);
    // Set RPC engine to protobuf RPC engine
    if (testWithLegacy) {
      RPC.setProtocolEngine(conf, TestRpcService2Legacy.class,
          ProtobufRpcEngine.class);
    }
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine2.class);
    RPC.setProtocolEngine(conf, TestRpcService2.class,
        ProtobufRpcEngine2.class);

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service = TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    if (testWithLegacy && testWithLegacyFirst) {
      PBServer2ImplLegacy server2ImplLegacy = new PBServer2ImplLegacy();
      com.google.protobuf.BlockingService legacyService =
          TestRpcServiceProtosLegacy.TestProtobufRpc2Proto
              .newReflectiveBlockingService(server2ImplLegacy);
      server = new RPC.Builder(conf).setProtocol(TestRpcService2Legacy.class)
          .setInstance(legacyService).setBindAddress(ADDRESS).setPort(PORT)
          .build();
      server.addProtocol(RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService.class,
          service);
    } else {
      // Get RPC server for server side implementation
      server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
          .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
    }
    addr = NetUtils.getConnectAddress(server);

    // now the second protocol
    PBServer2Impl server2Impl = new PBServer2Impl();
    BlockingService service2 = TestProtobufRpc2Proto
        .newReflectiveBlockingService(server2Impl);

    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService2.class,
        service2);

    if (testWithLegacy && !testWithLegacyFirst) {
      PBServer2ImplLegacy server2ImplLegacy = new PBServer2ImplLegacy();
      com.google.protobuf.BlockingService legacyService =
          TestRpcServiceProtosLegacy.TestProtobufRpc2Proto
              .newReflectiveBlockingService(server2ImplLegacy);
      server
          .addProtocol(RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService2Legacy.class,
              legacyService);
    }
    server.start();
  }
  
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  private TestRpcService2 getClient2() throws IOException {
    return RPC.getProxy(TestRpcService2.class, 0, addr, conf);
  }

  private TestRpcService2Legacy getClientLegacy() throws IOException {
    return RPC.getProxy(TestRpcService2Legacy.class, 0, addr, conf);
  }

  @Test (timeout=5000)
  public void testProtoBufRpc() throws Exception {
    TestRpcService client = getClient(addr, conf);
    testProtoBufRpc(client);
  }
  
  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {  
    // Test ping method
    client.ping(null, newEmptyRequest());
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    assertThat(echoResponse.getMessage()).isEqualTo("hello");
    
    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, newEmptyRequest());
      fail("Expected exception is not thrown");
    } catch (ServiceException e) {
      RemoteException re = (RemoteException)e.getCause();
      RpcServerException rse = (RpcServerException) re
          .unwrapRemoteException(RpcServerException.class);
      assertThat(rse).isNotNull();
      assertThat(re.getErrorCode())
          .isEqualTo(RpcErrorCodeProto.ERROR_RPC_SERVER);
    }
  }
  
  @Test (timeout=5000)
  public void testProtoBufRpc2() throws Exception {
    TestRpcService2 client = getClient2();
    
    // Test ping method
    client.ping2(null, newEmptyRequest());
    
    // Test echo method
    EchoResponseProto echoResponse = client.echo2(null,
        newEchoRequest("hello"));
    assertThat(echoResponse.getMessage()).isEqualTo("hello");
    
    // Ensure RPC metrics are updated
    MetricsRecordBuilder rpcMetrics = getMetrics(server.getRpcMetrics().name());
    assertCounterGt("RpcQueueTimeNumOps", 0L, rpcMetrics);
    assertCounterGt("RpcProcessingTimeNumOps", 0L, rpcMetrics);
    
    MetricsRecordBuilder rpcDetailedMetrics = 
        getMetrics(server.getRpcDetailedMetrics().name());
    assertCounterGt("Echo2NumOps", 0L, rpcDetailedMetrics);

    if (testWithLegacy) {
      testProtobufLegacy();
    }
  }

  private void testProtobufLegacy()
      throws IOException, com.google.protobuf.ServiceException {
    TestRpcService2Legacy client = getClientLegacy();

    // Test ping method
    client.ping2(null, TestProtosLegacy.EmptyRequestProto.newBuilder().build());

    // Test echo method
    TestProtosLegacy.EchoResponseProto echoResponse = client.echo2(null,
        TestProtosLegacy.EchoRequestProto.newBuilder().setMessage("hello")
            .build());
    assertThat(echoResponse.getMessage()).isEqualTo("hello");

    // Ensure RPC metrics are updated
    MetricsRecordBuilder rpcMetrics = getMetrics(server.getRpcMetrics().name());
    assertCounterGt("RpcQueueTimeNumOps", 0L, rpcMetrics);
    assertCounterGt("RpcProcessingTimeNumOps", 0L, rpcMetrics);

    MetricsRecordBuilder rpcDetailedMetrics =
        getMetrics(server.getRpcDetailedMetrics().name());
    assertCounterGt("Echo2NumOps", 0L, rpcDetailedMetrics);
  }

  @Test (timeout=5000)
  public void testProtoBufRandomException() throws Exception {
    //No test with legacy
    assumeFalse(testWithLegacy);
    TestRpcService client = getClient(addr, conf);

    try {
      client.error2(null, newEmptyRequest());
    } catch (ServiceException se) {
      assertThat(se.getCause()).isInstanceOf(RemoteException.class);
      RemoteException re = (RemoteException) se.getCause();
      assertThat(re.getClassName())
          .isEqualTo(URISyntaxException.class.getName());
      assertThat(re.getMessage()).contains("testException");
      assertThat(re.getErrorCode())
          .isEqualTo(RpcErrorCodeProto.ERROR_APPLICATION);
    }
  }
  
  @Test(timeout=6000)
  public void testExtraLongRpc() throws Exception {
    //No test with legacy
    assumeFalse(testWithLegacy);
    TestRpcService2 client = getClient2();
    final String shortString = StringUtils.repeat("X", 4);
    // short message goes through
    EchoResponseProto echoResponse = client.echo2(null,
        newEchoRequest(shortString));
    assertThat(echoResponse.getMessage()).isEqualTo(shortString);
    
    final String longString = StringUtils.repeat("X", 4096);
    try {
      client.echo2(null, newEchoRequest(longString));
      fail("expected extra-long RPC to fail");
    } catch (ServiceException se) {
      // expected
    }
  }

  @Test(timeout = 12000)
  public void testLogSlowRPC() throws IOException, ServiceException,
      TimeoutException, InterruptedException {
    //No test with legacy
    assumeFalse(testWithLegacy);
    TestRpcService2 client = getClient2();
    // make 10 K fast calls
    for (int x = 0; x < 10000; x++) {
      try {
        client.ping2(null, newEmptyRequest());
      } catch (Exception ex) {
        throw ex;
      }
    }

    // Ensure RPC metrics are updated
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    assertThat(rpcMetrics.getProcessingSampleCount()).isGreaterThan(999L);
    long before = rpcMetrics.getRpcSlowCalls();

    // make a really slow call. Sleep sleeps for 1000ms
    client.sleep(null, newSleepRequest(SLEEP_DURATION * 3));

    // Ensure slow call is logged.
    GenericTestUtils.waitFor(()
        -> rpcMetrics.getRpcSlowCalls() == before + 1L, 10, 1000);
  }

  @Test(timeout = 12000)
  public void testEnsureNoLogIfDisabled() throws IOException, ServiceException {
    //No test with legacy
    assumeFalse(testWithLegacy);
    // disable slow RPC  logging
    server.setLogSlowRPC(false);
    TestRpcService2 client = getClient2();

    // make 10 K fast calls
    for (int x = 0; x < 10000; x++) {
      client.ping2(null, newEmptyRequest());
    }

    // Ensure RPC metrics are updated
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    assertThat(rpcMetrics.getProcessingSampleCount()).isGreaterThan(999L);
    long before = rpcMetrics.getRpcSlowCalls();

    // make a really slow call. Sleep sleeps for 1000ms
    client.sleep(null, newSleepRequest(SLEEP_DURATION));

    long after = rpcMetrics.getRpcSlowCalls();

    // make sure we never called into Log slow RPC routine.
    assertThat(before).isEqualTo(after);
  }
}
