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

import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpc2Proto;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in src/test/proto
 * and protobuf service definition from src/test/test_rpc_service.proto
 */
public class TestProtoBufRpc {
  public final static String ADDRESS = "0.0.0.0";
  public final static int PORT = 0;
  private static InetSocketAddress addr;
  private static Configuration conf;
  private static RPC.Server server;
  
  @ProtocolInfo(protocolName = "testProto", protocolVersion = 1)
  public interface TestRpcService
      extends TestProtobufRpcProto.BlockingInterface {
  }

  @ProtocolInfo(protocolName = "testProto2", protocolVersion = 1)
  public interface TestRpcService2 extends
      TestProtobufRpc2Proto.BlockingInterface {
  }

  public static class PBServerImpl implements TestRpcService {

    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public EmptyResponseProto error(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new RpcServerException("error"));
    }
    
    @Override
    public EmptyResponseProto error2(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new URISyntaxException("",
          "testException"));
    }
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
  }

  @Before
  public  void setUp() throws IOException { // Setup server for both protocols
    conf = new Configuration();
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service = TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // Get RPC server for server side implementation
    server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
    addr = NetUtils.getConnectAddress(server);
    
    // now the second protocol
    PBServer2Impl server2Impl = new PBServer2Impl();
    BlockingService service2 = TestProtobufRpc2Proto
        .newReflectiveBlockingService(server2Impl);
    
    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService2.class,
        service2);
    server.start();
  }
  
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  private static TestRpcService getClient() throws IOException {
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class,
        ProtobufRpcEngine.class);
        return RPC.getProxy(TestRpcService.class, 0, addr,
        conf);
  }
  
  private static TestRpcService2 getClient2() throws IOException {
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService2.class,
        ProtobufRpcEngine.class);
        return RPC.getProxy(TestRpcService2.class, 0, addr,
        conf);
  }

  @Test (timeout=5000)
  public void testProtoBufRpc() throws Exception {
    TestRpcService client = getClient();
    testProtoBufRpc(client);
  }
  
  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {  
    // Test ping method
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    client.ping(null, emptyRequest);
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");
    
    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, emptyRequest);
      Assert.fail("Expected exception is not thrown");
    } catch (ServiceException e) {
      RemoteException re = (RemoteException)e.getCause();
      RpcServerException rse = (RpcServerException) re
          .unwrapRemoteException(RpcServerException.class);
    }
  }
  
  @Test (timeout=5000)
  public void testProtoBufRpc2() throws Exception {
    TestRpcService2 client = getClient2();
    
    // Test ping method
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    client.ping2(null, emptyRequest);
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo2(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");
    
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
    TestRpcService client = getClient();
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();

    try {
      client.error2(null, emptyRequest);
    } catch (ServiceException se) {
      Assert.assertTrue(se.getCause() instanceof RemoteException);
      RemoteException re = (RemoteException) se.getCause();
      Assert.assertTrue(re.getClassName().equals(
          URISyntaxException.class.getName()));
      Assert.assertTrue(re.getMessage().contains("testException"));
    }
  }
}