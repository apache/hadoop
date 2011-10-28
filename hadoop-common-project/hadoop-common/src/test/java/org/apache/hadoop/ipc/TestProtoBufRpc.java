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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.junit.Assert;
import org.junit.Test;

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

  public static class ServerImpl implements BlockingInterface {

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
  }

  private static RPC.Server startRPCServer(Configuration conf)
      throws IOException {
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, BlockingService.class, ProtobufRpcEngine.class);

    // Create server side implementation
    ServerImpl serverImpl = new ServerImpl();
    BlockingService service = TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // Get RPC server for serer side implementation
    RPC.Server server = RPC.getServer(BlockingService.class, service, ADDRESS,
        PORT, conf);
    server.start();
    return server;
  }

  private static BlockingInterface getClient(Configuration conf,
      InetSocketAddress addr) throws IOException {
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, BlockingInterface.class,
        ProtobufRpcEngine.class);
    BlockingInterface client = RPC.getProxy(BlockingInterface.class, 0, addr,
        conf);
    return client;
  }

  @Test
  public void testProtoBufRpc() throws Exception {
    Configuration conf = new Configuration();
    RPC.Server server = startRPCServer(conf);
    BlockingInterface client = getClient(conf, server.getListenerAddress());
    
    // Test ping method
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    client.ping(null, emptyRequest);
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");
    
    // Test error method - it should be thrown as RemoteException
    try {
      client.error(null, emptyRequest);
      Assert.fail("Expected exception is not thrown");
    } catch (ServiceException e) {
      RemoteException re = (RemoteException)e.getCause();
      re.printStackTrace();
      RpcServerException rse = (RpcServerException) re
          .unwrapRemoteException(RpcServerException.class);
      rse.printStackTrace();
    }
  }
}