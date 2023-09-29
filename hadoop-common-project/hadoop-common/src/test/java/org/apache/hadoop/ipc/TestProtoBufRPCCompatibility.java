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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.OptRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.OptResponseProto;

import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.OldProtobufRpcProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.NewProtobufRpcProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.NewerProtobufRpcProto;
import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;

public class TestProtoBufRPCCompatibility {

  private static final String ADDRESS = "0.0.0.0";
  public final static int PORT = 0;
  private static InetSocketAddress addr;
  private static RPC.Server server;
  private static Configuration conf;

  @ProtocolInfo(protocolName = "testProto", protocolVersion = 1)
  public interface OldRpcService extends
      OldProtobufRpcProto.BlockingInterface {
  }

  @ProtocolInfo(protocolName = "testProto", protocolVersion = 2)
  public interface NewRpcService extends
      NewProtobufRpcProto.BlockingInterface {
  }

  @ProtocolInfo(protocolName = "testProto", protocolVersion = 2)
  public interface NewerRpcService extends
      NewerProtobufRpcProto.BlockingInterface {
  }

  public static class OldServerImpl implements OldRpcService {

    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(Server.getClientId());
      Assert.assertEquals(16, clientId.length);
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EmptyResponseProto echo(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(Server.getClientId());
      Assert.assertEquals(16, clientId.length);
      return EmptyResponseProto.newBuilder().build();
    }
  }

  public static class NewServerImpl implements NewRpcService {

    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(Server.getClientId());
      Assert.assertEquals(16, clientId.length);
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public OptResponseProto echo(RpcController unused, OptRequestProto request)
        throws ServiceException {
      return OptResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }
  }

  @ProtocolInfo(protocolName = "testProto", protocolVersion = 2)
  public static class NewerServerImpl implements NewerRpcService {

    @Override
    public EmptyResponseProto ping(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(Server.getClientId());
      Assert.assertEquals(16, clientId.length);
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EmptyResponseProto echo(RpcController unused, EmptyRequestProto request)
        throws ServiceException {
      // Ensure clientId is received
      byte[] clientId = Server.getClientId();
      Assert.assertNotNull(Server.getClientId());
      Assert.assertEquals(16, clientId.length);
      return EmptyResponseProto.newBuilder().build();
    }
  }

  @Test
  public void testProtocolVersionMismatch() throws IOException, ServiceException {
    conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 1024);
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, NewRpcService.class, ProtobufRpcEngine2.class);

    // Create server side implementation
    NewServerImpl serverImpl = new NewServerImpl();
    BlockingService service = NewProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);
    // Get RPC server for server side implementation
    server = new RPC.Builder(conf).setProtocol(NewRpcService.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
    addr = NetUtils.getConnectAddress(server);

    server.start();

    RPC.setProtocolEngine(conf, OldRpcService.class, ProtobufRpcEngine2.class);

    OldRpcService proxy = RPC.getProxy(OldRpcService.class, 0, addr, conf);
    // Verify that exception is thrown if protocolVersion is mismatch between
    // client and server.
    EmptyRequestProto emptyRequest = EmptyRequestProto.newBuilder().build();
    try {
      proxy.ping(null, emptyRequest);
      fail("Expected an exception to occur as version mismatch.");
    } catch (Exception e) {
      if (! (e.getMessage().contains("version mismatch"))){
        // Exception type is not what we expected, re-throw it.
        throw new IOException(e);
      }
    }

    // Verify that missing of optional field is still compatible in RPC call.
    RPC.setProtocolEngine(conf, NewerRpcService.class,
        ProtobufRpcEngine2.class);
    NewerRpcService newProxy = RPC.getProxy(NewerRpcService.class, 0, addr,
        conf);
    newProxy.echo(null, emptyRequest);

  }

}
