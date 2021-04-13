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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos2.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.thirdparty.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LinkedIn internal RPC Engine for protobuf based RPCs.
 * This is based on ProtobufRpcEngine and includes additional
 * features for internal use.
 */
@InterfaceStability.Evolving
public class LiProtobufRpcEngine2 extends ProtobufRpcEngine2 {
  public static final Logger LOG =
      LoggerFactory.getLogger(LiProtobufRpcEngine2.class);

  static {
    replaceProtocolEngine();
  }

  static void replaceProtocolEngine() {
    LiServer.LiProtoBufRpcInvoker invoker = new LiServer.LiProtoBufRpcInvoker();
    org.apache.hadoop.ipc.Server.rpcKindMap.put(
        RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        new org.apache.hadoop.ipc.Server.RpcKindMapValue(
            ProtobufRpcEngine2.RpcProtobufRequest.class, invoker));

    LOG.info("rpcKind=" + RPC.RpcKind.RPC_PROTOCOL_BUFFER
        + ", rpcRequestWrapperClass=" + ProtobufRpcEngine2.RpcProtobufRequest.class
        + ", rpcInvoker=" + invoker);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {

    final LiInvoker invoker = new LiInvoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth,
        alignmentContext);
    return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker), false);
  }

  @Override
  public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException {
    Class<ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
    return new ProtocolProxy<ProtocolMetaInfoPB>(protocol,
        (ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
            new Class[] { protocol }, new LiInvoker(protocol, connId, conf,
                factory)), false);
  }

  protected static class LiInvoker extends ProtobufRpcEngine2.Invoker {
    protected LiInvoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
        throws IOException {
      super(protocol, addr, ticket, conf, factory, rpcTimeout,
          connectionRetryPolicy, fallbackToSimpleAuth, alignmentContext);
    }

    protected LiInvoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory) {
      super(protocol, connId, conf, factory);
    }

    private RequestHeaderProto constructRpcRequestHeader(Method method,
        Message request) {
      RequestHeaderProto.Builder builder = RequestHeaderProto
          .newBuilder();
      builder.setMethodName(method.getName());


      // For protobuf, {@code protocol} used when creating client side proxy is
      // the interface extending BlockingInterface, which has the annotations
      // such as ProtocolName etc.
      //
      // Using Method.getDeclaringClass(), as in WritableEngine to get at
      // the protocol interface will return BlockingInterface, from where
      // the annotation ProtocolName and Version cannot be
      // obtained.
      //
      // Hence we simply use the protocol class used to create the proxy.
      // For PB this may limit the use of mixins on client side.
      builder.setDeclaringClassProtocolName(this.protocolName);
      builder.setClientProtocolVersion(this.clientProtocolVersion);
      return builder.build();
    }

    @Override
    protected Writable constructRpcRequest(Method method, Message theRequest) {
      RequestHeaderProto rpcRequestHeader =
          constructRpcRequestHeader(method, theRequest);
      return new RpcProtobufRequest(rpcRequestHeader, theRequest);
    }

    @Override
    public void close() throws IOException {
      super.close();
    }
  }

  public static class LiServer extends ProtobufRpcEngine2.Server {
    public LiServer(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig, AlignmentContext alignmentContext)
        throws IOException {
      super(protocolClass, protocolImpl, conf, bindAddress, port, numHandlers,
          numReaders, queueSizePerHandler, verbose, secretManager,
          portRangeConfig, alignmentContext);
    }

    /**
     * Protobuf invoker for {@link RpcInvoker}
     */
    static class LiProtoBufRpcInvoker
        extends ProtobufRpcEngine2.Server.ProtoBufRpcInvoker {
      private ExtensionRegistry extensionRegistry;

      LiProtoBufRpcInvoker() {
      }

      @Override
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are returned in the response.
       * See {@link HadoopRpcResponseProto}
       *
       * In this method there three types of exceptions possible and they are
       * returned in response as follows.
       * <ol>
       * <li> Exceptions encountered in this method that are returned
       * as {@link RpcServerException} </li>
       * <li> Exceptions thrown by the service is wrapped in ServiceException.
       * In that this method returns in response the exception thrown by the
       * service.</li>
       * <li> Other exceptions thrown by the service. They are returned as
       * it is.</li>
       * </ol>
       */
      public Writable call(RPC.Server server, String protocol,
          Writable writableRequest, long receiveTime) throws Exception {
        RpcProtobufRequest request = (RpcProtobufRequest) writableRequest;
        RequestHeaderProto rpcRequest = request.getRequestHeader();
        rpcRequest = rpcRequest.getParserForType()
            .parseFrom(rpcRequest.toByteArray(), this.extensionRegistry);

        String methodName = rpcRequest.getMethodName();
        String protoName = rpcRequest.getDeclaringClassProtocolName();
        long clientVersion = rpcRequest.getClientProtocolVersion();

        return call(server, protocol, request, receiveTime, methodName,
            protoName, clientVersion);
      }
    }
  }
}
