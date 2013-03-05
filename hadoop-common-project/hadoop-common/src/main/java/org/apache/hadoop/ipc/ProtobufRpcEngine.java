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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * RPC Engine for for protobuf based RPCs.
 */
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements RpcEngine {
  public static final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);
  
  static { // Register the rpcRequest deserializer for WritableRpcEngine 
    org.apache.hadoop.ipc.Server.registerProtocolEngine(
        RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcRequestWrapper.class,
        new Server.ProtoBufRpcInvoker());
  }

  private static final ClientCache CLIENTS = new ClientCache();

  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
        rpcTimeout, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
      ) throws IOException {

    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout, connectionRetryPolicy);
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
            new Class[] { protocol }, new Invoker(protocol, connId, conf,
                factory)), false);
  }

  private static class Invoker implements RpcInvocationHandler {
    private final Map<String, Message> returnTypes = 
        new ConcurrentHashMap<String, Message>();
    private boolean isClosed = false;
    private final Client.ConnectionId remoteId;
    private final Client client;
    private final long clientProtocolVersion;
    private final String protocolName;

    private Invoker(Class<?> protocol, InetSocketAddress addr,
        UserGroupInformation ticket, Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy) throws IOException {
      this(protocol, Client.ConnectionId.getConnectionId(
          addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
          conf, factory);
    }
    
    /**
     * This constructor takes a connectionId, instead of creating a new one.
     */
    public Invoker(Class<?> protocol, Client.ConnectionId connId,
        Configuration conf, SocketFactory factory) {
      this.remoteId = connId;
      this.client = CLIENTS.getClient(conf, factory, RpcResponseWrapper.class);
      this.protocolName = RPC.getProtocolName(protocol);
      this.clientProtocolVersion = RPC
          .getProtocolVersion(protocol);
    }

    private RequestHeaderProto constructRpcRequestHeader(Method method) {
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
      builder.setDeclaringClassProtocolName(protocolName);
      builder.setClientProtocolVersion(clientProtocolVersion);
      return builder.build();
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     * 
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are 
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     * 
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = Time.now();
      }
      
      if (args.length != 2) { // RpcController + Message
        throw new ServiceException("Too many parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + args.length);
      }
      if (args[1] == null) {
        throw new ServiceException("null param while calling Method: ["
            + method.getName() + "]");
      }

      RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);
      RpcResponseWrapper val = null;
      
      if (LOG.isTraceEnabled()) {
        LOG.trace(Thread.currentThread().getId() + ": Call -> " +
            remoteId + ": " + method.getName() +
            " {" + TextFormat.shortDebugString((Message) args[1]) + "}");
      }


      Message theRequest = (Message) args[1];
      try {
        val = (RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
            new RpcRequestWrapper(rpcRequestHeader, theRequest), remoteId);

      } catch (Throwable e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
              remoteId + ": " + method.getName() +
                " {" + e + "}");
        }

        throw new ServiceException(e);
      }

      if (LOG.isDebugEnabled()) {
        long callTime = Time.now() - startTime;
        LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
      }
      
      Message prototype = null;
      try {
        prototype = getReturnProtoType(method);
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      Message returnMessage;
      try {
        returnMessage = prototype.newBuilderForType()
            .mergeFrom(val.responseMessage).build();

        if (LOG.isTraceEnabled()) {
          LOG.trace(Thread.currentThread().getId() + ": Response <- " +
              remoteId + ": " + method.getName() +
                " {" + TextFormat.shortDebugString(returnMessage) + "}");
        }

      } catch (Throwable e) {
        throw new ServiceException(e);
      }
      return returnMessage;
    }

    @Override
    public void close() throws IOException {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }
      
      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message prototype = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), prototype);
      return prototype;
    }

    @Override //RpcInvocationHandler
    public ConnectionId getConnectionId() {
      return remoteId;
    }
  }

  /**
   * Wrapper for Protocol Buffer Requests
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  private static class RpcRequestWrapper implements Writable {
    RequestHeaderProto requestHeader;
    Message theRequest; // for clientSide, the request is here
    byte[] theRequestRead; // for server side, the request is here

    @SuppressWarnings("unused")
    public RpcRequestWrapper() {
    }

    RpcRequestWrapper(RequestHeaderProto requestHeader, Message theRequest) {
      this.requestHeader = requestHeader;
      this.theRequest = theRequest;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      OutputStream os = DataOutputOutputStream.constructOutputStream(out);
      
      ((Message)requestHeader).writeDelimitedTo(os);
      theRequest.writeDelimitedTo(os);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = ProtoUtil.readRawVarint32(in);
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      requestHeader = RequestHeaderProto.parseFrom(bytes);
      length = ProtoUtil.readRawVarint32(in);
      theRequestRead = new byte[length];
      in.readFully(theRequestRead);
    }
    
    @Override
    public String toString() {
      return requestHeader.getDeclaringClassProtocolName() + "." +
          requestHeader.getMethodName();
    }
  }

  /**
   *  Wrapper for Protocol Buffer Responses
   * 
   * Note while this wrapper is writable, the request on the wire is in
   * Protobuf. Several methods on {@link org.apache.hadoop.ipc.Server and RPC} 
   * use type Writable as a wrapper to work across multiple RpcEngine kinds.
   */
  private static class RpcResponseWrapper implements Writable {
    byte[] responseMessage;

    @SuppressWarnings("unused")
    public RpcResponseWrapper() {
    }

    public RpcResponseWrapper(Message message) {
      this.responseMessage = message.toByteArray();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(responseMessage.length);
      out.write(responseMessage);     
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      responseMessage = bytes;
    }
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  static Client getClient(Configuration conf) {
    return CLIENTS.getClient(conf, SocketFactory.getDefault(),
        RpcResponseWrapper.class);
  }
  
 

  @Override
  public RPC.Server getServer(Class<?> protocol, Object protocolImpl,
      String bindAddress, int port, int numHandlers, int numReaders,
      int queueSizePerHandler, boolean verbose, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig)
      throws IOException {
    return new Server(protocol, protocolImpl, conf, bindAddress, port,
        numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
        portRangeConfig);
  }
  
  public static class Server extends RPC.Server {
    /**
     * Construct an RPC server.
     * 
     * @param protocolClass the class of protocol
     * @param protocolImpl the protocolImpl whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @param portRangeConfig A config parameter that can be used to restrict
     * the range of ports used when port is 0 (an ephemeral port)
     */
    public Server(Class<?> protocolClass, Object protocolImpl,
        Configuration conf, String bindAddress, int port, int numHandlers,
        int numReaders, int queueSizePerHandler, boolean verbose,
        SecretManager<? extends TokenIdentifier> secretManager, 
        String portRangeConfig)
        throws IOException {
      super(bindAddress, port, null, numHandlers,
          numReaders, queueSizePerHandler, conf, classNameBase(protocolImpl
              .getClass().getName()), secretManager, portRangeConfig);
      this.verbose = verbose;  
      registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
          protocolImpl);
    }
    
    /**
     * Protobuf invoker for {@link RpcInvoker}
     */
    static class ProtoBufRpcInvoker implements RpcInvoker {
      private static ProtoClassProtoImpl getProtocolImpl(RPC.Server server,
          String protoName, long version) throws IOException {
        ProtoNameVer pv = new ProtoNameVer(protoName, version);
        ProtoClassProtoImpl impl = 
            server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
        if (impl == null) { // no match for Protocol AND Version
          VerProtocolImpl highest = 
              server.getHighestSupportedProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, 
                  protoName);
          if (highest == null) {
            throw new IOException("Unknown protocol: " + protoName);
          }
          // protocol supported but not the version that client wants
          throw new RPC.VersionMismatch(protoName, version,
              highest.version);
        }
        return impl;
      }

      @Override 
      /**
       * This is a server side method, which is invoked over RPC. On success
       * the return response has protobuf response payload. On failure, the
       * exception name and the stack trace are return in the resposne.
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
      public Writable call(RPC.Server server, String connectionProtocolName,
          Writable writableRequest, long receiveTime) throws Exception {
        RpcRequestWrapper request = (RpcRequestWrapper) writableRequest;
        RequestHeaderProto rpcRequest = request.requestHeader;
        String methodName = rpcRequest.getMethodName();
        
        
        /** 
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is 
         * sent form client to server at connection time. 
         * 
         * Each Rpc call also sends a protocol name 
         * (called declaringClassprotocolName). This name is usually the same
         * as the connection protocol name except in some cases. 
         * For example metaProtocols such ProtocolInfoProto which get info
         * about the protocol reuse the connection but need to indicate that
         * the actual protocol is different (i.e. the protocol is
         * ProtocolInfoProto) since they reuse the connection; in this case
         * the declaringClassProtocolName field is set to the ProtocolInfoProto.
         */

        String declaringClassProtoName = 
            rpcRequest.getDeclaringClassProtocolName();
        long clientVersion = rpcRequest.getClientProtocolVersion();
        if (server.verbose)
          LOG.info("Call: connectionProtocolName=" + connectionProtocolName + 
              ", method=" + methodName);
        
        ProtoClassProtoImpl protocolImpl = getProtocolImpl(server, 
                              declaringClassProtoName, clientVersion);
        BlockingService service = (BlockingService) protocolImpl.protocolImpl;
        MethodDescriptor methodDescriptor = service.getDescriptorForType()
            .findMethodByName(methodName);
        if (methodDescriptor == null) {
          String msg = "Unknown method " + methodName + " called on " 
                                + connectionProtocolName + " protocol.";
          LOG.warn(msg);
          throw new RpcServerException(msg);
        }
        Message prototype = service.getRequestPrototype(methodDescriptor);
        Message param = prototype.newBuilderForType()
            .mergeFrom(request.theRequestRead).build();
        
        Message result;
        try {
          long startTime = Time.now();
          server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
          result = service.callBlockingMethod(methodDescriptor, null, param);
          int processingTime = (int) (Time.now() - startTime);
          int qTime = (int) (startTime - receiveTime);
          if (LOG.isDebugEnabled()) {
            LOG.info("Served: " + methodName + " queueTime= " + qTime +
                      " procesingTime= " + processingTime);
          }
          server.rpcMetrics.addRpcQueueTime(qTime);
          server.rpcMetrics.addRpcProcessingTime(processingTime);
          server.rpcDetailedMetrics.addProcessingTime(methodName,
              processingTime);
        } catch (ServiceException e) {
          throw (Exception) e.getCause();
        } catch (Exception e) {
          throw e;
        }
        return new RpcResponseWrapper(result);
      }
    }
  }
}
